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
#define print_list(str1,list) \
		str1 = nodeToString(list);	\
		printf("%s\n",debackslash(str1, strlen(str1)));\
		pfree(str1);\
		fflush(stdout);

#define cacheTag(cacheptr)		(((const CacheM*)(cacheptr))->type)
#define IsACache(cacheptr,_type_)		(cacheTag(cacheptr) == M_##_type_)

#define UNMATCHED -1
#define FULL_MATCHED 0
#define MATCHED_RIGHT 1
#define MATCHED_LEFT 2
extern void InitCachesForMemo(void);
extern void printMemoCache(void);

typedef struct MemoClause {
	NodeTag type;
	Oid opno;
	List *args;
	RestrictInfo * parent; /* optional pointer to an existing RestrictInfo*/
	double selectivity;

} MemoClause;

typedef struct MemoInfoData1 {
	int found;
	double rows;
	List *unmatches;
	List *matches;
	double loops;
	double removed_rows;
	char *debug;
	char *debug1;
	char *debug2;

} MemoInfoData1;

typedef enum CacheTag {
	M_Invalid = 0,

	M_MemoQuery, M_JoinCache, M_SelCache

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

extern void get_relation_size(MemoInfoData1 *result, PlannerInfo *root, RelOptInfo *rel, List *quals, bool isParam,
		SpecialJoinInfo * sjinfo);
extern void store_join(List *lrelName, int level, List *clauses, double rows, bool isParam);
extern void export_join(FILE *file);
extern MemoRelation * set_base_rel_tuples(List *lrelName, int level, double tuples);
extern void set_memo_join_sizes(void);
extern int get_memo_loop_count(List *relation_name, List *clauses, int level);
extern int get_memo_removed(List *relation_name, List *clauses, int level);
extern MemoRelation * get_Memorelation(MemoInfoData1 *resultList, List *lrelName, int level, List *clauses,
		int isIndex);
extern void set_plain_rel_sizes_from_memo(PlannerInfo *root, RelOptInfo *rel, Path *path, double *loop_count,
		bool isIndex);
extern void set_join_sizes_from_memo(PlannerInfo *root, RelOptInfo *rel, JoinPath *pathnode);
extern void set_agg_sizes_from_memo(PlannerInfo *root, Path *path);
extern void free_memo_cache(void);
#endif /* MEMOCACHE_H_ */

