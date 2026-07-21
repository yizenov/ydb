#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

bool TBuildLookupJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Join &&
        input->Props.JoinAlgo.has_value() &&
        *input->Props.JoinAlgo == EJoinAlgoType::LookupJoin;
}

TIntrusivePtr<IOperator> TBuildLookupJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    Y_UNUSED(props);
    Y_UNUSED(rboCtx);

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

    return MakeIntrusive<TOpLookupJoin>(join->GetLeftInput(), join->Pos, join->JoinKind, rightRead->TableCallable,
                                        rightRead->Columns, rightOutputIUs, leftKeys, rightKeyColumns);
}

} // namespace NKikimr::NKqp
