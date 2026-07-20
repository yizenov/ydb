#include "kqp_rbo_index_selection.h"

#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

// Candidate eligibility, mirroring the legacy optimizer: only synchronous, ready, whole-table
// indexes with a real impl table can serve a read (async/json/local/bloom variants are skipped).
bool IsIndexEligible(const TIndexDescription& index) {
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

// Phase 1 supports covering indexes only: every physical column the read produces must exist in the
// index impl table, so the read can be served entirely from the index without a lookup to the base
// table.
bool IndexCoversRead(const TKikimrTableMetadata& implMeta, const TOpRead& read) {
    for (const auto& column : read.Columns) {
        if (!implMeta.Columns.contains(column)) {
            return false;
        }
    }
    return true;
}

// Selection score without sort order (deliberately simpler than the legacy tuple): a longer point
// prefix wins first, then a longer range-usable prefix. std::pair compares lexicographically, so
// larger is better.
using TIndexScore = std::pair<size_t, size_t>; // (PointPrefixLen, UsedPrefixLen)

} // anonymous namespace

bool TSelectIndexRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->Children.front()->Kind == EOperator::Source;
}

TIntrusivePtr<IOperator> TSelectIndexRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    Y_UNUSED(props);
    auto& kqpCtx = rboCtx.KqpCtx;
    auto& ctx = rboCtx.ExprCtx;
    auto& typeCtx = rboCtx.TypeCtx;

    const auto filter = CastOperator<TOpFilter>(input);
    const auto maybeRead = filter->GetInput();
    if (maybeRead->Kind != EOperator::Source) {
        return input;
    }
    const auto read = CastOperator<TOpRead>(maybeRead);

    // Row tables only. Column storage uses range pushdown (push_ranges) and never lowers to an index
    // read; a read that already carries ranges has been rewritten and must not be retargeted.
    if (read->GetTableStorageType() != NYql::EStorageType::RowStorage || read->GetRanges()) {
        return input;
    }

    const auto type = filter->FilterExpr.Node->GetTypeAnn();
    if (!type || type->GetKind() == ETypeAnnotationKind::Pg) {
        return input;
    }

    const auto tablePath = TExprBase(read->GetTable()).Cast<TKqpTable>().Path().StringValue();
    const auto tableDesc = kqpCtx.Tables->EnsureTableExists(kqpCtx.Cluster, tablePath, read->Pos, ctx);
    if (!tableDesc || tableDesc->Metadata->Indexes.empty()) {
        return input;
    }

    // Prepare the predicate range extractor against the read's scheme, exactly as push_ranges does.
    auto lambda = TCoLambda(GetLambdaForRangeExtractor(filter->FilterExpr.Node, read->Type, rboCtx));
    auto arg = lambda.Args().Arg(0).Ptr();
    arg->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());

    THashSet<TString> possibleKeys;
    auto settings = PrepareExtractorSettings(kqpCtx);
    auto extractor = MakePredicateRangeExtractor(settings);
    auto schemeType = PrepareSchemeType(*read, tableDesc->SchemeNode, ctx);
    if (!extractor->Prepare(lambda.Ptr(), *schemeType, possibleKeys, ctx, typeCtx)) {
        return input;
    }

    // Key columns must be named as the scheme type exposes them so BuildComputeNode lines up with the
    // names the extractor resolved.
    const auto physicalToExposed = BuildPhysicalToExposedName(*read);
    const bool exposesQualified = std::any_of(read->OutputIUs.begin(), read->OutputIUs.end(),
                                              [](const TInfoUnit& iu) { return !iu.GetAlias().empty(); });
    auto scoreKeyColumns = [&](const TVector<TString>& physicalKeys) -> TIndexScore {
        TVector<TString> exposedKeys;
        exposedKeys.reserve(physicalKeys.size());
        for (const auto& key : physicalKeys) {
            exposedKeys.emplace_back(ResolveExposedName(key, *read, physicalToExposed, exposesQualified));
        }
        const auto buildResult = extractor->BuildComputeNode(exposedKeys, ctx, typeCtx);
        return {buildResult.PointPrefixLen, buildResult.UsedPrefixLen};
    };

    // The primary-key columns as the read exposes them. These are the lookup keys for a non-covering
    // index and the eligibility gate: the non-covering rewrite reads exactly the PK from the index,
    // so the filter must be evaluable on the PK alone (we do not yet project extra index columns
    // before the lookup).
    TVector<TInfoUnit> pkIUs;
    for (const auto& pk : tableDesc->Metadata->KeyColumnNames) {
        pkIUs.emplace_back(read->Alias, pk);
    }
    bool filterOnPkOnly = true;
    for (const auto& iu : filter->GetFilterIUs(props)) {
        if (std::find(pkIUs.begin(), pkIUs.end(), iu) == pkIUs.end()) {
            filterOnPkOnly = false;
            break;
        }
    }

    // The primary key is the baseline; an index must strictly beat it to be worth choosing.
    TIndexScore bestScore = scoreKeyColumns(tableDesc->Metadata->KeyColumnNames);
    TMaybe<TString> bestIndex;
    TIntrusivePtr<TKikimrTableMetadata> bestImpl;
    bool bestCovering = true;
    for (size_t i = 0; i < tableDesc->Metadata->Indexes.size(); ++i) {
        const auto& index = tableDesc->Metadata->Indexes[i];
        if (!IsIndexEligible(index)) {
            continue;
        }
        const auto& implMeta = tableDesc->Metadata->ImplTables[i];
        if (!implMeta) {
            continue;
        }
        const bool covers = IndexCoversRead(*implMeta, *read);
        if (!covers && !filterOnPkOnly) {
            continue; // Phase 2 limitation: non-covering rewrite only when the filter is PK-only.
        }
        const TIndexScore score = scoreKeyColumns(index.KeyColumns);
        if (score > bestScore) {
            bestScore = score;
            bestIndex = index.Name;
            bestImpl = implMeta;
            bestCovering = covers;
        }
    }

    if (!bestIndex) {
        return input;
    }

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO][index] table=" << tablePath << " selected index=" << *bestIndex
                                 << " covering=" << bestCovering << " pointPrefix=" << bestScore.first
                                 << " usedPrefix=" << bestScore.second;

    // Inlined BuildTableMeta for the index impl table (rbo cannot PEERDIR its parent kqp/opt).
    // clang-format off
    auto implTableMeta = Build<TKqpTable>(ctx, read->Pos)
        .Path().Build(bestImpl->Name)
        .PathId().Build(bestImpl->PathId.ToString())
        .SysView().Build(bestImpl->SysView)
        .Version().Build(bestImpl->SchemaVersion)
    .Done();
    // clang-format on

    if (bestCovering) {
        // Covering: redirect the read to the impl table; the filter stays above it unchanged.
        auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->GetOutputIUs(), read->StorageType,
                                              implTableMeta.Ptr(), read->OlapFilterLambda, read->Limit, std::nullopt,
                                              read->OriginalPredicate, read->SortDir, read->Props, read->Pos);
        return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, filter->FilterExpr);
    }

    // Non-covering: read exactly the primary key from the index impl, apply the filter on the index,
    // then stream-lookup the base table by that key to fetch the originally requested columns.
    //
    // The stream lookup matches its input struct fields to the table's key columns by their plain,
    // unqualified names, so the index read exposes the PK under plain names (empty alias) and the
    // filter's PK references are renamed to match. The lookup's output is renamed back to the read's
    // qualified output IUs during physical conversion.
    TVector<TString> pkColumns(tableDesc->Metadata->KeyColumnNames.begin(), tableDesc->Metadata->KeyColumnNames.end());
    TVector<TInfoUnit> pkIUsPlain;
    for (const auto& pk : pkColumns) {
        pkIUsPlain.emplace_back(TString(), pk);
    }

    auto indexRead = MakeIntrusive<TOpRead>(read->Alias, pkColumns, pkIUsPlain, read->StorageType,
                                            implTableMeta.Ptr(), nullptr, nullptr, std::nullopt, std::nullopt,
                                            ESortDir::None, read->Props, read->Pos);
    auto indexFilter = MakeIntrusive<TOpFilter>(indexRead, filter->Pos, filter->Props, filter->FilterExpr);

    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap;
    for (const auto& pk : pkColumns) {
        renameMap[TInfoUnit(read->Alias, pk)] = TInfoUnit(TString(), pk);
    }
    indexFilter->RenameUsedIUs(renameMap, ctx);

    return MakeIntrusive<TOpTableLookup>(indexFilter, read->Pos, read->TableCallable, read->Columns,
                                         read->GetOutputIUs(), pkIUsPlain);
}

} // namespace NKikimr::NKqp
