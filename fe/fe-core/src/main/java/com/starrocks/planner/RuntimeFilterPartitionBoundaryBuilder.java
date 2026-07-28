// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.planner;

import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.thrift.TPartitionBoundary;
import com.starrocks.thrift.TPartitionBoundaryType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

final class RuntimeFilterPartitionBoundaryBuilder {
    private RuntimeFilterPartitionBoundaryBuilder() {
    }

    static List<TPartitionBoundary> build(
            OlapTable table, List<Long> selectedPartitionIds, Set<SlotRef> probeSlots, int listValueLimit) {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (probeSlots.isEmpty()) {
            return List.of();
        }
        List<Column> partitionColumns = partitionInfo.getPartitionColumns(table.getIdToColumn());
        if (partitionColumns.isEmpty()) {
            return List.of();
        }
        if (partitionInfo instanceof RangePartitionInfo rangeInfo) {
            return buildRangeBoundaries(
                    table, selectedPartitionIds, probeSlots, partitionColumns.get(0),
                    partitionColumns.size() > 1, rangeInfo);
        }
        if (partitionInfo instanceof ListPartitionInfo listInfo) {
            return buildListBoundaries(
                    table, selectedPartitionIds, probeSlots, partitionColumns, listInfo, listValueLimit);
        }
        return List.of();
    }

    private static List<TPartitionBoundary> buildRangeBoundaries(
            OlapTable table, List<Long> selectedPartitionIds, Set<SlotRef> probeSlots,
            Column partitionColumn, boolean multiColumn, RangePartitionInfo rangeInfo) {
        List<TPartitionBoundary> boundaries = new ArrayList<>();
        for (SlotRef probeSlot : probeSlots) {
            int slotId = probeSlot.getSlotId().asInt();
            Column column = probeSlot.getColumn();
            if (!column.getColumnId().equals(partitionColumn.getColumnId())) {
                continue;
            }
            for (long partitionId : selectedPartitionIds) {
                Partition partition = table.getPartition(partitionId);
                Range<PartitionKey> range = rangeInfo.getRange(partitionId);
                if (partition == null || range == null || range.isEmpty()) {
                    continue;
                }
                TPartitionBoundary boundary = new TPartitionBoundary();
                boundary.setSlot_id(slotId);
                boundary.setBoundary_type(TPartitionBoundaryType.RANGE);
                if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
                    boundary.setRange_lower(ExprToThrift.treeToThrift(range.lowerEndpoint().getKeys().get(0)));
                } else {
                    boundary.setContains_null(true);
                }
                if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
                    boundary.setRange_upper(ExprToThrift.treeToThrift(range.upperEndpoint().getKeys().get(0)));
                    boundary.setRange_upper_inclusive(multiColumn);
                }
                addPhysicalBoundaries(partition, boundary, boundaries);
            }
        }
        return boundaries;
    }

    private static List<TPartitionBoundary> buildListBoundaries(
            OlapTable table, List<Long> selectedPartitionIds, Set<SlotRef> probeSlots,
            List<Column> partitionColumns, ListPartitionInfo listInfo, int listValueLimit) {
        List<TPartitionBoundary> boundaries = new ArrayList<>();
        for (SlotRef probeSlot : probeSlots) {
            int slotId = probeSlot.getSlotId().asInt();
            Column column = probeSlot.getColumn();
            int columnIndex = partitionColumnIndex(partitionColumns, column);
            for (long partitionId : selectedPartitionIds) {
                Partition partition = table.getPartition(partitionId);
                List<LiteralExpr> values = listValues(listInfo, partitionId, columnIndex);
                if (partition == null || values == null) {
                    continue;
                }
                Set<LiteralExpr> uniqueValues = new LinkedHashSet<>(values);
                if (uniqueValues.size() > listValueLimit) {
                    continue;
                }
                boolean containsNull = uniqueValues.removeIf(LiteralExpr::isConstantNull);
                TPartitionBoundary boundary = new TPartitionBoundary();
                boundary.setSlot_id(slotId);
                boundary.setBoundary_type(TPartitionBoundaryType.LIST);
                boundary.setContains_null(containsNull);
                if (!uniqueValues.isEmpty()) {
                    boundary.setList_values(ExprToThrift.treesToThrift(new ArrayList<>(uniqueValues)));
                }
                addPhysicalBoundaries(partition, boundary, boundaries);
            }
        }
        return boundaries;
    }

    private static int partitionColumnIndex(List<Column> partitionColumns, Column column) {
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (partitionColumns.get(i).getColumnId().equals(column.getColumnId())) {
                return i;
            }
        }
        return -1;
    }

    private static List<LiteralExpr> listValues(
            ListPartitionInfo listInfo, long partitionId, int columnIndex) {
        List<LiteralExpr> singleColumnValues = listInfo.getLiteralExprValues().get(partitionId);
        if (singleColumnValues != null) {
            return columnIndex == 0 ? singleColumnValues : null;
        }
        List<List<LiteralExpr>> tuples = listInfo.getMultiLiteralExprValues().get(partitionId);
        if (tuples == null) {
            return null;
        }
        List<LiteralExpr> values = new ArrayList<>(tuples.size());
        for (List<LiteralExpr> tuple : tuples) {
            if (columnIndex >= tuple.size()) {
                return null;
            }
            values.add(tuple.get(columnIndex));
        }
        return values;
    }

    private static void addPhysicalBoundaries(
            Partition partition, TPartitionBoundary boundary,
            List<TPartitionBoundary> boundaries) {
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            TPartitionBoundary physicalBoundary = new TPartitionBoundary(boundary);
            physicalBoundary.setPartition_id(physicalPartition.getId());
            boundaries.add(physicalBoundary);
        }
    }
}
