/*
 * outfuncs.h
 *
 *  Created on: 20 juin 2014
 *      Author: alex
 */

#ifndef OUTFUNCS_H_
#define OUTFUNCS_H_

#include "lib/stringinfo.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
typedef enum ArgType
{	S_NULL = 0,
	S_AND,
	S_OR,
	S_NOT



} ArgType;
extern void _outSimOpExpr(StringInfo str, const OpExpr *node);

extern void _outSimVar(StringInfo str, const Var *node);
extern void _outSimConst(StringInfo str, const Const *node);
extern void _outSimParam(StringInfo str, const Param *node);
extern void _outSimSubPlan(StringInfo str, const SubPlan *node);
extern void _outSimNode(ArgType type, StringInfo str, const void *obj);
extern void _outSimRelabelType(StringInfo str, const RelabelType *node) ;
extern void _outNode(StringInfo str, const void *obj);
extern void _outSimRestrictInfo(StringInfo str, const RestrictInfo *node);
#endif /* OUTFUNCS_H_ */
