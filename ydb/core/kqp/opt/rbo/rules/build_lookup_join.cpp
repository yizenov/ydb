#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

// Only synchronous, ready, whole-table indexes can serve a lookup probe (same filter as the CBO's
// applicability check and the single-table index selection rule).
bool IsLookupIndexEligible(const TIndexDescription& index) {
    switch (index.Type) {
        case TIndexDescription::EType::GlobalAsync:
        case TIndexDescription::EType::GlobalJson:
        case TIndexDescription::EType::GlobalJsonCompact:
        case TIndexDescription::EType::LocalMinMax:
        case TIndexDescription::EType::LocalBloomFilter:
        case TIndexDescription::EType::LocalBloomNgramFilter:
            return false;
        default:
            break;
    }
    return index.State == TIndexDescription::EIndexState::Ready;
}

} // anonymous namespace

bool TBuildLookupJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Join &&
        input->Props.JoinAlgo.has_value() &&
        *input->Props.JoinAlgo == EJoinAlgoType::LookupJoin;
}

TIntrusivePtr<IOperator> TBuildLookupJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    Y_UNUSED(props);
    auto& ctx = rboCtx.ExprCtx;

    const auto join = CastOperator<TOpJoin>(input);

    // Phase 4 supports a simple inner lookup join whose right side is a single-consumer base-table
    // read, with equi-keys and no residual join filters.
    if (join->JoinKind != "Inner" || !join->JoinFilters.empty()) {
        return input;
    }
    const auto rightInput = join->GetRightInput();
    if (rightInput->Kind != EOperator::Source || !rightInput->IsSingleConsumer()) {
        return input;
    }
    const auto rightRead = CastOperator<TOpRead>(rightInput);
    if (rightRead->GetRanges() || rightRead->GetTableStorageType() != NYql::EStorageType::RowStorage) {
        return input;
    }

    // Map each right-side join-key IU to its physical column name in the right read.
    const auto& rightOutputIUs = rightRead->GetOutputIUs();
    TVector<TInfoUnit> leftKeys;
    TVector<TString> rightKeyColumns;
    for (const auto& [leftKey, rightKey] : join->JoinKeys) {
        const auto it = std::find(rightOutputIUs.begin(), rightOutputIUs.end(), rightKey);
        if (it == rightOutputIUs.end()) {
            return input;
        }
        leftKeys.push_back(leftKey);
        rightKeyColumns.push_back(rightRead->Columns[it - rightOutputIUs.begin()]);
    }

    // Choose what to probe. If the right table's own primary key starts with a join key, probe the
    // base table. Otherwise look for a covering secondary index whose leading key columns are join
    // keys (Rule 2 - mirrors the legacy ChooseIndexForLookupJoin) and probe its impl table instead.
    const auto tablePath = TExprBase(rightRead->TableCallable).Cast<TKqpTable>().Path().StringValue();
    const auto tableDesc = rboCtx.KqpCtx.Tables->EnsureTableExists(rboCtx.KqpCtx.Cluster, tablePath, rightRead->Pos, ctx);
    if (!tableDesc) {
        return input;
    }

    THashSet<TString> joinKeySet(rightKeyColumns.begin(), rightKeyColumns.end());
    const auto& pkColumns = tableDesc->Metadata->KeyColumnNames;
    if (!pkColumns.empty() && joinKeySet.contains(pkColumns[0])) {
        return MakeIntrusive<TOpLookupJoin>(join->GetLeftInput(), join->Pos, join->JoinKind, rightRead->TableCallable,
                                            rightRead->Columns, rightOutputIUs, leftKeys, rightKeyColumns);
    }

    THashMap<TString, TInfoUnit> rightColumnToLeftKey;
    for (size_t i = 0; i < rightKeyColumns.size(); i++) {
        rightColumnToLeftKey[rightKeyColumns[i]] = leftKeys[i];
    }

    size_t bestPrefix = 0;
    TIntrusivePtr<TKikimrTableMetadata> bestImpl;
    const TIndexDescription* bestIndex = nullptr;
    for (size_t i = 0; i < tableDesc->Metadata->Indexes.size(); i++) {
        const auto& index = tableDesc->Metadata->Indexes[i];
        if (!IsLookupIndexEligible(index)) {
            continue;
        }
        const auto& implMeta = tableDesc->Metadata->ImplTables[i];
        if (!implMeta) {
            continue;
        }
        // Phase 4 supports covering indexes only: the probe must be answerable from the impl table.
        bool covers = true;
        for (const auto& column : rightRead->Columns) {
            if (!implMeta->Columns.contains(column)) {
                covers = false;
                break;
            }
        }
        if (!covers) {
            continue;
        }
        size_t prefix = 0;
        while (prefix < index.KeyColumns.size() && joinKeySet.contains(index.KeyColumns[prefix])) {
            ++prefix;
        }
        if (prefix > bestPrefix) {
            bestPrefix = prefix;
            bestImpl = implMeta;
            bestIndex = &index;
        }
    }

    if (!bestIndex) {
        return input;
    }

    // Probe by the index key prefix, in index key order (the impl table is keyed that way).
    TVector<TInfoUnit> indexLeftKeys;
    TVector<TString> indexKeyColumns;
    for (size_t i = 0; i < bestPrefix; i++) {
        indexKeyColumns.push_back(bestIndex->KeyColumns[i]);
        indexLeftKeys.push_back(rightColumnToLeftKey.at(bestIndex->KeyColumns[i]));
    }

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO][lookup join] table=" << tablePath << " probing index=" << bestIndex->Name
                                 << " keyPrefix=" << bestPrefix;

    // clang-format off
    auto implTableMeta = Build<TKqpTable>(ctx, rightRead->Pos)
        .Path().Build(bestImpl->Name)
        .PathId().Build(bestImpl->PathId.ToString())
        .SysView().Build(bestImpl->SysView)
        .Version().Build(bestImpl->SchemaVersion)
    .Done();
    // clang-format on

    return MakeIntrusive<TOpLookupJoin>(join->GetLeftInput(), join->Pos, join->JoinKind, implTableMeta.Ptr(),
                                        rightRead->Columns, rightOutputIUs, indexLeftKeys, indexKeyColumns);
}

} // namespace NKikimr::NKqp
