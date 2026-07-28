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

#include "compute_env/query/partition_scan_range_pruner.h"

#include <algorithm>
#include <optional>

#include "column/column_helper.h"
#include "column/runtime_type_traits.h"
#include "common/object_pool.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "exprs/expr.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/literal.h"
#include "runtime/runtime_filter.h"
#include "types/date_value.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

StatusOr<ColumnPtr> build_partition_literal_values(const SlotDescriptor* slot_desc, const std::vector<TExpr>& literals,
                                                   bool contains_null) {
    auto column = ColumnHelper::create_column(slot_desc->type(), true);
    column->reserve(literals.size() + static_cast<size_t>(contains_null));
    for (const auto& literal : literals) {
        if (literal.nodes.size() != 1) {
            return Status::InvalidArgument("partition boundary must be a literal");
        }
        VectorizedLiteral literal_expr(literal.nodes[0]);
        auto value = literal_expr.value();
        if (literal_expr.type() != slot_desc->type() || value == nullptr) {
            return Status::InvalidArgument("partition boundary literal type mismatch");
        }
        if (value->only_null()) {
            column->append_nulls(1);
        } else {
            auto unwrapped = ColumnHelper::unpack_and_duplicate_const_column(1, value);
            column->append(*unwrapped, 0, 1);
        }
    }
    if (contains_null) {
        column->append_nulls(1);
    }
    return column;
}

bool range_intersects_values(const RuntimeFilterPartitionBoundary& boundary, const Column& values) {
    for (size_t i = 0; i < values.size(); ++i) {
        if (values.is_null(i)) {
            if (boundary.contains_null) {
                return true;
            }
            continue;
        }
        if (boundary.lower_bound != nullptr && values.compare_at(i, 0, *boundary.lower_bound, -1) < 0) {
            continue;
        }
        if (boundary.upper_bound == nullptr) {
            return true;
        }
        const int upper_comparison = values.compare_at(i, 0, *boundary.upper_bound, -1);
        if (upper_comparison < 0 || (upper_comparison == 0 && boundary.upper_bound_inclusive)) {
            return true;
        }
    }
    return false;
}

template <LogicalType Type>
bool range_pruned_by_min_max(const RuntimeFilterPartitionBoundary& boundary, const RuntimeFilter& filter) {
    constexpr LogicalType mapping_type = Type == TYPE_CHAR ? TYPE_VARCHAR : Type;
    const auto* min_max = dynamic_cast<const MinMaxRuntimeFilter<mapping_type>*>(filter.get_min_max_filter());
    if (min_max == nullptr) {
        return false;
    }
    if (boundary.contains_null && filter.has_null()) {
        return false;
    }
    if (min_max->is_empty_range()) {
        return true;
    }

    ObjectPool pool;
    auto min_column = ColumnHelper::create_column(boundary.column_type, true);
    min_column->append_datum(Datum(min_max->min_value(&pool)));
    if (boundary.upper_bound != nullptr) {
        const int comparison = min_column->compare_at(0, 0, *boundary.upper_bound, -1);
        if (comparison > 0 || (comparison == 0 && !boundary.upper_bound_inclusive)) {
            return true;
        }
    }

    auto max_column = ColumnHelper::create_column(boundary.column_type, true);
    max_column->append_datum(Datum(min_max->max_value(&pool)));
    if (boundary.lower_bound == nullptr) {
        return false;
    }
    const int comparison = max_column->compare_at(0, 0, *boundary.lower_bound, -1);
    return comparison < 0 || (comparison == 0 && !min_max->right_close_interval());
}

struct JoinRuntimeInFilter {
    SlotId slot_id;
    ColumnPtr values;
};

std::optional<JoinRuntimeInFilter> parse_join_runtime_in_filter(ExprContext& filter) {
    const Expr* root = filter.root();
    if (root->get_num_children() == 0 || !root->get_child(0)->is_slotref()) {
        return std::nullopt;
    }
    std::vector<SlotId> slot_ids;
    if (root->get_child(0)->get_slot_ids(&slot_ids) != 1) {
        return std::nullopt;
    }

#define M(TYPE)                                                                                      \
    case TYPE: {                                                                                     \
        constexpr LogicalType mapping_type = TYPE == TYPE_CHAR ? TYPE_VARCHAR : TYPE;                \
        const auto* predicate = dynamic_cast<const VectorizedInConstPredicate<mapping_type>*>(root); \
        if (predicate == nullptr || !predicate->is_join_runtime_filter()) {                          \
            return std::nullopt;                                                                     \
        }                                                                                            \
        return JoinRuntimeInFilter{.slot_id = slot_ids[0], .values = predicate->get_all_values()};   \
    }
    switch (root->get_child(0)->type().type) {
        APPLY_FOR_ALL_SCALAR_TYPE(M)
    default:
        return std::nullopt;
    }
#undef M
}

bool runtime_in_filter_values_prune_partition(const RuntimeFilterPartitionBoundary& boundary, const Column& values,
                                              ExprContext& filter) {
    if (boundary.boundary_type == TPartitionBoundaryType::RANGE) {
        return !range_intersects_values(boundary, values);
    }

    Chunk chunk;
    chunk.append_column(boundary.values, boundary.slot_id);
    auto result = filter.evaluate(&chunk);
    return result.ok() && ColumnHelper::count_true_with_notnull(result.value()) == 0;
}

} // namespace

StatusOr<RuntimeFilterPartitionBoundaryMap> parse_runtime_filter_partition_boundaries(
        const TupleDescriptor* tuple_desc, const std::vector<TPartitionBoundary>& thrift_boundaries) {
    if (tuple_desc == nullptr) {
        return Status::InvalidArgument("partition boundaries require a tuple descriptor");
    }

    RuntimeFilterPartitionBoundaryMap boundaries;
    for (const auto& thrift_boundary : thrift_boundaries) {
        auto* slot = tuple_desc->get_slot_by_id(thrift_boundary.slot_id);
        if (slot == nullptr) {
            return Status::InvalidArgument("partition boundary slot not found");
        }

        RuntimeFilterPartitionBoundary boundary{
                .partition_id = thrift_boundary.partition_id,
                .slot_id = thrift_boundary.slot_id,
                .column_type = slot->type(),
                .boundary_type = thrift_boundary.boundary_type,
                .contains_null = thrift_boundary.__isset.contains_null && thrift_boundary.contains_null,
                .upper_bound_inclusive =
                        thrift_boundary.__isset.range_upper_inclusive && thrift_boundary.range_upper_inclusive};
        switch (thrift_boundary.boundary_type) {
        case TPartitionBoundaryType::LIST: {
            if ((!thrift_boundary.__isset.list_values || thrift_boundary.list_values.empty()) && !boundary.contains_null) {
                return Status::InvalidArgument("empty LIST partition boundary");
            }
            ASSIGN_OR_RETURN(boundary.values,
                             build_partition_literal_values(slot, thrift_boundary.list_values, boundary.contains_null));
            break;
        }
        case TPartitionBoundaryType::RANGE: {
            if (thrift_boundary.__isset.range_lower) {
                ASSIGN_OR_RETURN(boundary.lower_bound,
                                 build_partition_literal_values(slot, {thrift_boundary.range_lower}, false));
                if (boundary.lower_bound->only_null()) {
                    return Status::InvalidArgument("RANGE partition boundary has NULL lower bound");
                }
            }
            if (thrift_boundary.__isset.range_upper) {
                ASSIGN_OR_RETURN(boundary.upper_bound,
                                 build_partition_literal_values(slot, {thrift_boundary.range_upper}, false));
                if (boundary.upper_bound->only_null()) {
                    return Status::InvalidArgument("RANGE partition boundary has NULL upper bound");
                }
            }
            break;
        }
        default:
            return Status::InvalidArgument("unknown partition boundary type");
        }
        boundaries[boundary.slot_id].emplace_back(std::move(boundary));
    }
    for (auto& [_, slot_boundaries] : boundaries) {
        std::sort(slot_boundaries.begin(), slot_boundaries.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.partition_id < rhs.partition_id; });
    }
    return boundaries;
}

bool runtime_filter_prunes_partition(const RuntimeFilterPartitionBoundary& boundary, const RuntimeFilter& filter) {
    if (filter.always_true() || filter.is_group_colocate_filter() || filter.num_hash_partitions() > 0) {
        return false;
    }
    if (boundary.boundary_type == TPartitionBoundaryType::LIST) {
        if (filter.type() != RuntimeFilterSerializeType::BLOOM_FILTER &&
            filter.type() != RuntimeFilterSerializeType::EMPTY_FILTER &&
            filter.type() != RuntimeFilterSerializeType::BITSET_FILTER) {
            return false;
        }
        RuntimeFilter::RunningContext context;
        context.use_merged_selection = false;
        context.selection.assign(boundary.values->size(), 1);
        filter.evaluate(boundary.values.get(), &context);
        return std::none_of(context.selection.begin(), context.selection.end(),
                            [](uint8_t selected) { return selected != 0; });
    }
    if (filter.type() != RuntimeFilterSerializeType::BLOOM_FILTER &&
        filter.type() != RuntimeFilterSerializeType::EMPTY_FILTER) {
        return false;
    }

#define M(TYPE) \
    case TYPE:  \
        return range_pruned_by_min_max<TYPE>(boundary, filter);
    switch (boundary.column_type.type) {
        APPLY_FOR_ALL_SCALAR_TYPE(M)
    default:
        return false;
    }
#undef M
}

bool runtime_in_filter_prunes_partition(const RuntimeFilterPartitionBoundary& boundary, ExprContext& filter) {
    auto runtime_in_filter = parse_join_runtime_in_filter(filter);
    if (!runtime_in_filter.has_value() || runtime_in_filter->slot_id != boundary.slot_id) {
        return false;
    }
    return runtime_in_filter_values_prune_partition(boundary, *runtime_in_filter->values, filter);
}

RuntimeFilterPartitionPruneResult RuntimeFilterPartitionPruner::update(
        const RuntimeFilterPartitionBoundaryMap& boundaries, const std::vector<ExprContext*>& runtime_in_filters,
        RuntimeFilterProbeCollector* runtime_filters, int32_t driver_sequence) {
    std::lock_guard update_lock(_update_mutex);

    RuntimeFilterPartitionPruneResult result;
    if (_evaluated_in_filters &&
        (runtime_filters == nullptr || _evaluated_filter_ids.size() == runtime_filters->descriptors().size())) {
        return result;
    }
    std::unordered_set<int64_t> newly_pruned;

    if (!_evaluated_in_filters) {
        for (auto* filter : runtime_in_filters) {
            if (filter == nullptr || filter->root() == nullptr) {
                continue;
            }
            auto runtime_in_filter = parse_join_runtime_in_filter(*filter);
            if (!runtime_in_filter.has_value()) {
                continue;
            }
            auto slot_boundaries = boundaries.find(runtime_in_filter->slot_id);
            if (slot_boundaries == boundaries.end()) {
                continue;
            }
            for (const auto& boundary : slot_boundaries->second) {
                if (newly_pruned.contains(boundary.partition_id)) {
                    continue;
                }
                ++result.evaluations;
                if (runtime_in_filter_values_prune_partition(boundary, *runtime_in_filter->values, *filter)) {
                    newly_pruned.emplace(boundary.partition_id);
                }
            }
        }
        _evaluated_in_filters = true;
    }

    if (runtime_filters != nullptr) {
        for (const auto& [filter_id, descriptor] : runtime_filters->descriptors()) {
            if (_evaluated_filter_ids.contains(filter_id)) {
                continue;
            }
            SlotId slot_id;
            if (descriptor == nullptr || descriptor->probe_expr_ctx() == nullptr ||
                descriptor->is_stream_build_filter() || !descriptor->can_push_down_runtime_filter() ||
                !descriptor->is_probe_slot_ref(&slot_id)) {
                _evaluated_filter_ids.emplace(filter_id);
                continue;
            }
            auto slot_boundaries = boundaries.find(slot_id);
            if (slot_boundaries == boundaries.end() || slot_boundaries->second.empty() ||
                descriptor->probe_expr_type() != slot_boundaries->second.front().column_type.type) {
                _evaluated_filter_ids.emplace(filter_id);
                continue;
            }
            const RuntimeFilter* filter = descriptor->runtime_filter(driver_sequence);
            if (filter == nullptr) {
                continue;
            }
            _evaluated_filter_ids.emplace(filter_id);
            if (filter->is_group_colocate_filter()) {
                continue;
            }
            for (const auto& boundary : slot_boundaries->second) {
                if (newly_pruned.contains(boundary.partition_id)) {
                    continue;
                }
                ++result.evaluations;
                if (runtime_filter_prunes_partition(boundary, *filter)) {
                    newly_pruned.emplace(boundary.partition_id);
                }
            }
        }
    }

    if (!newly_pruned.empty()) {
        std::unique_lock prune_lock(_pruned_partition_ids_mutex);
        _pruned_partition_ids.insert(newly_pruned.begin(), newly_pruned.end());
    }
    return result;
}

bool RuntimeFilterPartitionPruner::is_partition_pruned(int64_t partition_id) const {
    std::shared_lock lock(_pruned_partition_ids_mutex);
    return _pruned_partition_ids.contains(partition_id);
}

StatusOr<ColumnPtr> build_partition_col_values(const SlotDescriptor* slot_desc, const TKeyRange& column_range) {
    if (column_range.__isset.list_values && !column_range.list_values.empty()) {
        return build_partition_literal_values(slot_desc, column_range.list_values, false);
    } else if (column_range.__isset.begin_key && column_range.__isset.end_key) {
        if (slot_desc->type().is_date_type()) {
            auto lower_julian = date::from_date_literal(column_range.begin_key);
            auto upper_julian = date::from_date_literal(column_range.end_key);

            auto col = ColumnHelper::create_column(slot_desc->type(), true);
            col->reserve(upper_julian - lower_julian + 1);
            for (JulianDate date = lower_julian; date <= upper_julian; date++) {
                col->append_datum(Datum(DateValue{date}));
            }
            if (column_range.__isset.has_null && column_range.has_null) {
                col->append_nulls(1);
            }
            return col;
        } else if (slot_desc->type().is_integer_type()) {
            size_t size = column_range.end_key - column_range.begin_key + 1;
            auto col = ColumnHelper::create_column(slot_desc->type(), true);
            col->reserve(size);
#define M(TYPE)                                                                    \
    if (slot_desc->type().type == TYPE) {                                          \
        for (int64_t v = column_range.begin_key; v <= column_range.end_key; v++) { \
            col->append_datum(Datum((RunTimeTypeTraits<TYPE>::CppType)v));         \
        }                                                                          \
    }
            APPLY_FOR_ALL_INT_TYPE(M)
#undef M
            if (column_range.__isset.has_null && column_range.has_null) {
                col->append_nulls(1);
            }
            return col;
        } else {
            DCHECK(false) << "Unsupported partition column range, column name: " << column_range.column_name;
            return Status::InternalError("Unsupported partition column range");
        }
    } else {
        DCHECK(false) << "Unsupported partition column range, column name: " << column_range.column_name;
        return Status::InternalError("Unsupported partition column range");
    }
}

Status prune_scan_ranges_by_partition_conjuncts(RuntimeState*, const TupleDescriptor* tuple_desc,
                                                const std::vector<ExprContext*>& partition_conjunct_ctxs,
                                                const std::vector<TScanRangeParams>& scan_ranges,
                                                std::vector<TScanRangeParams>* pruned_scan_ranges) {
    if (partition_conjunct_ctxs.empty() || tuple_desc == nullptr) {
        *pruned_scan_ranges = scan_ranges;
        return Status::OK();
    }

    phmap::flat_hash_map<std::string, SlotDescriptor*> column_name_to_slot;
    for (auto* slot : tuple_desc->slots()) {
        column_name_to_slot[slot->col_name()] = slot;
    }

    std::vector<TScanRangeParams> temp;
    temp.reserve(scan_ranges.size());
    for (const auto& scan_range : scan_ranges) {
        const auto& internal_range = scan_range.scan_range.internal_scan_range;
        if (!internal_range.__isset.partition_column_ranges || internal_range.partition_column_ranges.empty()) {
            temp.emplace_back(scan_range);
            continue;
        }

        bool is_pruned = false;
        for (const auto& partition_column_range : internal_range.partition_column_ranges) {
            auto it = column_name_to_slot.find(partition_column_range.column_name);
            if (it == column_name_to_slot.end()) {
                continue;
            }
            auto* slot = it->second;
            ASSIGN_OR_RETURN(auto col, build_partition_col_values(slot, partition_column_range));

            Chunk partition_cols_chunk;
            Filter filter(col->size(), 1);
            partition_cols_chunk.append_column(std::move(col), slot->id());

            std::vector<SlotId> slot_ids;
            for (auto* ctx : partition_conjunct_ctxs) {
                slot_ids.clear();
                if (ctx->root()->get_slot_ids(&slot_ids) != 1 || slot_ids[0] != slot->id()) {
                    continue;
                }
                ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate(&partition_cols_chunk, filter.data()));
                size_t true_count = ColumnHelper::count_true_with_notnull(column);
                if (true_count == column->size()) {
                    continue;
                } else if (0 == true_count) {
                    is_pruned = true;
                    break;
                } else {
                    bool all_zero = false;
                    ColumnHelper::merge_two_filters(column, &filter, &all_zero);
                    if (all_zero) {
                        is_pruned = true;
                        break;
                    }
                }
            }
            if (is_pruned) {
                break;
            }
        }

        if (!is_pruned) {
            temp.emplace_back(scan_range);
        }
    }
    pruned_scan_ranges->swap(temp);
    return Status::OK();
}

} // namespace starrocks
