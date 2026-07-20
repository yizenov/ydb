#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>

namespace NKikimr::NKqp {

// Predicate-range-extractor wiring shared between range pushdown (push_ranges.cpp, where these are
// defined) and automatic index selection (select_index.cpp). Both need to run the extractor against
// a table read's scheme and resolve key-column names to the read's exposed output names.
NYql::TExprNode::TPtr GetLambdaForRangeExtractor(NYql::TExprNode::TPtr node, const NYql::TTypeAnnotationNode* inputType,
                                                 TRBOContext& rboCtx);
NYql::TPredicateExtractorSettings PrepareExtractorSettings(TKqpOptimizeContext& kqpCtx);
THashMap<TString, TString> BuildPhysicalToExposedName(const TOpRead& read);
TString ResolveExposedName(const TString& physicalName, const TOpRead& read,
                           const THashMap<TString, TString>& physicalToExposed, bool exposesQualified);
const NYql::TStructExprType* PrepareSchemeType(const TOpRead& read, const NYql::TStructExprType* schemeType,
                                               NYql::TExprContext& ctx);

} // namespace NKikimr::NKqp
