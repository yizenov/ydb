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
    bool IsLookupJoinApplicable(const std::shared_ptr<IBaseOptimizerNode>& right, const TVector<TJoinColumn>& rightJoinKeys) const {
        THashSet<TString> joinKeys;
        for (const auto& joinKey : rightJoinKeys) {
            joinKeys.insert(joinKey.AttributeName);
        }

        const auto& stats = right->Stats;
        if (stats.KeyColumns && !stats.KeyColumns->Data.empty() && joinKeys.contains(stats.KeyColumns->Data[0])) {
            return true;
        }

        // Secondary-index probe: applicable when some ready, synchronous index has its leading key
        // column among the join keys (mirrors the index branch of the legacy check).
        //
        // TODO(lookup-join-index): the rewrite (TBuildLookupJoinRule) selects the index and builds a
        // plan, but probing the index impl table by only the index key is a PARTIAL key prefix (the
        // impl table's key is index key columns + base PK), and execution fails packing the lookup
        // rows ("CopyToChunked(): Unexpected end of buffer"). The PK probe, which supplies a full
        // key, works. Keep disabled until the prefix probe is handled (legacy sets
        // TKqpStreamLookupSettings::AllowNullKeysPrefixSize for this case).
        constexpr bool IndexProbeReady = false;
        if (!IndexProbeReady) {
            return false;
        }

        auto rel = std::dynamic_pointer_cast<TRBORelOptimizerNode>(right);
        if (!rel || !rel->Op || rel->Op->Kind != EOperator::Source) {
            return false;
        }
        const auto read = CastOperator<TOpRead>(rel->Op);
        const auto path = NYql::NNodes::TKqpTable(read->TableCallable).Path().StringValue();
        const auto& tables = KqpCtx.Tables->GetTables();
        const auto it = tables.find(std::make_pair(TString(KqpCtx.Cluster), path));
        if (it == tables.end() || !it->second.Metadata) {
            return false;
        }
        for (const auto& index : it->second.Metadata->Indexes) {
            if (!IsLookupIndexEligible(index) || index.KeyColumns.empty()) {
                continue;
            }
            if (joinKeys.contains(index.KeyColumns[0])) {
                return true;
            }
        }
        return false;
    }

    static bool IsLookupIndexEligible(const NYql::TIndexDescription& index) {
        switch (index.Type) {
            case NYql::TIndexDescription::EType::GlobalAsync:
            case NYql::TIndexDescription::EType::GlobalJson:
            case NYql::TIndexDescription::EType::GlobalJsonCompact:
            case NYql::TIndexDescription::EType::LocalMinMax:
            case NYql::TIndexDescription::EType::LocalBloomFilter:
            case NYql::TIndexDescription::EType::LocalBloomNgramFilter:
                return false;
            default:
                break;
        }
        return index.State == NYql::TIndexDescription::EIndexState::Ready;
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
