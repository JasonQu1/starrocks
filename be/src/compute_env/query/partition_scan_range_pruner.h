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

#pragma once

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/status.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "types/type_descriptor.h"

namespace starrocks {

class RuntimeFilter;
class RuntimeFilterProbeCollector;
class RuntimeState;

struct RuntimeFilterPartitionBoundary {
    int64_t partition_id = 0;
    SlotId slot_id = 0;
    TypeDescriptor column_type;
    TPartitionBoundaryType::type boundary_type = TPartitionBoundaryType::LIST;
    ColumnPtr values;
    ColumnPtr lower_bound;
    ColumnPtr upper_bound;
    bool contains_null = false;
    bool upper_bound_inclusive = false;
};

using RuntimeFilterPartitionBoundaries = std::vector<RuntimeFilterPartitionBoundary>;
using RuntimeFilterPartitionBoundaryMap = std::unordered_map<SlotId, RuntimeFilterPartitionBoundaries>;

struct RuntimeFilterPartitionPruneResult {
    int64_t evaluations = 0;
};

StatusOr<RuntimeFilterPartitionBoundaryMap> parse_runtime_filter_partition_boundaries(
        const TupleDescriptor* tuple_desc, const std::vector<TPartitionBoundary>& thrift_boundaries);

bool runtime_filter_prunes_partition(const RuntimeFilterPartitionBoundary& boundary, const RuntimeFilter& filter);
bool runtime_in_filter_prunes_partition(const RuntimeFilterPartitionBoundary& boundary, ExprContext& filter);

class RuntimeFilterPartitionPruner {
public:
    RuntimeFilterPartitionPruneResult update(const RuntimeFilterPartitionBoundaryMap& boundaries,
                                             const std::vector<ExprContext*>& runtime_in_filters,
                                             RuntimeFilterProbeCollector* runtime_filters, int32_t driver_sequence);
    bool is_partition_pruned(int64_t partition_id) const;

private:
    std::mutex _update_mutex;
    std::unordered_set<int32_t> _evaluated_filter_ids;
    bool _evaluated_in_filters = false;

    mutable std::shared_mutex _pruned_partition_ids_mutex;
    std::unordered_set<int64_t> _pruned_partition_ids;
};

// Materialize partition column candidate values described by `column_range` into a Column.
// Supports either a list of literal `list_values` or an inclusive integer/date `[begin_key, end_key]` range.
// Used by the backend-side dynamic partition pruning path in OlapScanNode.
StatusOr<ColumnPtr> build_partition_col_values(const SlotDescriptor* slot_desc, const TKeyRange& column_range);

// Drop scan ranges whose partition values cannot satisfy any of the single-column
// `partition_conjunct_ctxs`. The conjunct contexts must have been prepared and opened by the
// caller. The tuple descriptor is used to resolve partition column names to slots. On success
// `pruned_scan_ranges` is populated with the retained scan ranges.
Status prune_scan_ranges_by_partition_conjuncts(RuntimeState*, const TupleDescriptor* tuple_desc,
                                                const std::vector<ExprContext*>& partition_conjunct_ctxs,
                                                const std::vector<TScanRangeParams>& scan_ranges,
                                                std::vector<TScanRangeParams>* pruned_scan_ranges);

} // namespace starrocks
