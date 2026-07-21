#include "kqp_operator.h"
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>

namespace NKikimr::NKqp::NOpt {

struct TRBORelOptimizerNode : public TRelOptimizerNode {

    TRBORelOptimizerNode(TVector<TString> labels, TOptimizerStatistics stats, TIntrusivePtr<IOperator> op) :
        TRelOptimizerNode(labels[0], std::move(stats)),
        _Labels(labels),
        Op(op)
        {}

    TVector<TString> Labels() override {
        return _Labels;
    }

    void Print(std::stringstream& stream, int ntabs) override {
        for (int i = 0; i < ntabs; i++) {
            stream << "    ";
        }
        stream << "Rels: ";

        for (auto r : _Labels ) {
            stream << r << ", ";
        }
        stream << "\n";

        for (int i = 0; i < ntabs; i++) {
            stream << "    ";
        }
        stream << Stats << "\n";
    }

    TVector<TString> _Labels;
    TIntrusivePtr<IOperator> Op;
};

struct TRBOProviderContext : public TKqpProviderContext {
    TRBOProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel, bool useBlockHashJoin) : TKqpProviderContext(kqpCtx, optLevel, useBlockHashJoin) {}

    // A lookup join probes the right relation by its primary key: it is applicable when the right
    // side's leading key column is one of the right-side join keys. This mirrors the primary-key
    // branch of the legacy IsLookupJoinApplicableDetailed (ydb/core/kqp/opt/logical/kqp_opt_cbo.cpp),
    // but reads the key columns from the CBO node's statistics rather than a legacy read expr, since
    // the new RBO carries operators (not TKqlReadTable nodes) at CBO time.
    static bool IsLookupJoinApplicable(const std::shared_ptr<IBaseOptimizerNode>& right, const TVector<TJoinColumn>& rightJoinKeys) {
        const auto& stats = right->Stats;
        if (!stats.KeyColumns || stats.KeyColumns->Data.empty()) {
            return false;
        }
        const auto& firstKeyColumn = stats.KeyColumns->Data[0];
        for (const auto& joinKey : rightJoinKeys) {
            if (joinKey.AttributeName == firstKeyColumn) {
                return true;
            }
        }
        return false;
    }

    virtual bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        NKqp::EJoinAlgoType joinAlgo,
        EJoinKind joinKind
    ) override {
        if (joinAlgo == NKqp::EJoinAlgoType::LookupJoin) {
            // Realized by TBuildLookupJoinRule + the LookupJoinRows stream lookup in physical conversion.
            return IsLookupJoinApplicable(right, rightJoinKeys);
        }
        if (joinAlgo != NKqp::EJoinAlgoType::MapJoin && joinAlgo != NKqp::EJoinAlgoType::GraceJoin) {
            return false;
        }
        return TKqpProviderContext::IsJoinApplicable(left, right, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind);
    }
};
}
