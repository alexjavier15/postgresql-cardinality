/*
 * memocache.h
 *
 *  Created on: 15 sept. 2014
 *      Author: alex
 */

#ifndef MEMOCACHE_H_
#define MEMOCACHE_H_


#include "lib/ilist.h"
#include "nodes/relation.h"


#define cacheTag(cacheptr)		(((const CacheM*)(cacheptr))->type)
#define IsACache(cacheptr,_type_)		(cacheTag(cacheptr) == M_##_type_)
extern void InitCachesForMemo(void);
extern void printMemoCache(void);
typedef struct MemoClause {
	NodeTag type;
	Oid opno;
	List *args;

} MemoClause;

typedef struct MemoInfoData1 {
	int found;
	double rows;
	int nbmatches;
	List *unmatches;
	List *matches;
	double loops;
	double removed_rows;

} MemoInfoData1;


typedef enum CacheTag {
	M_Invalid = 0,

	M_MemoQuery, M_JoinCache

} CacheTag;
typedef enum ListType {
	M_NAME = 0,

	M_CLAUSE

} ListType;
typedef struct CacheM {
	CacheTag type;
	int length;
	dlist_head *content;
	int size;
} CacheM;

extern void get_join_memo_size1(MemoInfoData1 *result, RelOptInfo *joinrel, int level, char *quals,
		bool isParameterized);
extern void get_baserel_memo_size1(MemoInfoData1 *result, MemoRelation * memorelation, int lclauses, char *quals,
		bool isIndex);
extern void store_join(List *lrelName, int level, List *clauses);
extern void export_join(FILE *file);
extern MemoRelation * set_base_rel_tuples(List *lrelName, int level, double tuples);
extern void set_memo_join_sizes(void);
extern int  get_memo_loop_count(List *relation_name, List *clauses, int level);
extern int get_memo_removed(List *relation_name, List *clauses, int level);
extern MemoRelation *  get_Memorelation(List *lrelName, int level, List *clauses, bool isIndex);
extern void set_plain_rel_sizes_from_memo(PlannerInfo *root, RelOptInfo *rel, Path *path,double *loop_count, bool isIndex);
extern void set_join_sizes_from_memo(PlannerInfo *root, RelOptInfo *rel, JoinPath *pathnode);

#endif /* MEMOCACHE_H_ */

