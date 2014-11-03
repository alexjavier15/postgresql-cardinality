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
#define BITMAPSET_TREE_SIZE 256

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
	Selectivity last;

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

typedef struct RteReferences {

	Value **rte_table;
	int size;
} RteReferences;

extern void recost_rel_path(PlannerInfo *root, RelOptInfo *baserel);
extern void recost_plain_rel_path(PlannerInfo *root, RelOptInfo *baserel);
extern void recost_paths(PlannerInfo *root, RelOptInfo *joinrel);
extern bool lcontains(RelOptInfo *rel, List *clauses);
extern List * restictInfoToMemoClauses(List *clauses);
extern void push_reference(Index index, Value * name);
extern void * fetch_unique_rte_reference(void);
extern void * get_cur_rte_reference(void);
extern void add_recosted_paths(RelOptInfo *joinrel);
extern void update_and_recost(PlannerInfo *root, RelOptInfo *joinrel);
extern void invalide_removed_path(RelOptInfo *rel, Path* path);

extern void attach_child_joinpath(Path *parent_path, Path *child_path);
extern void add_relation(MemoRelation * relation, int rellen);
extern MemoRelation* create_memo_realation(int level, bool isParam, List *relname, double rows, int loops,
		List *clauses);
extern void update_cached_joins(List *joinname, int level, double rows);

extern List * restictInfoToMemoClauses(List *clauses);
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
extern void check_NoMemo_queries(void);

#endif /* MEMOCACHE_H_ */

