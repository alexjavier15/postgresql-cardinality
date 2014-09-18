/*
 * memocache.h
 *
 *  Created on: 15 sept. 2014
 *      Author: alex
 */

#ifndef MEMOCACHE_H_
#define MEMOCACHE_H_

#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "lib/ilist.h"

#define cacheTag(cacheptr)		(((const CacheM*)(cacheptr))->type)
#define IsACache(cacheptr,_type_)		(cacheTag(cacheptr) == M_##_type_)
extern void InitCachesForMemo(void);
extern void printMemoCache(void);

typedef struct MemoInfoData1 {
	int found;
	double rows;
	int nbmatches;
	List *unmatches;
	List *matches;

} MemoInfoData1;

typedef enum CacheTag {
	M_Invalid = 0,

	M_MemoQuery, M_JoinCache

} CacheTag;

typedef struct CacheM {
	CacheTag type;
	int length;
	dlist_head *content;
	int size;
} CacheM;

extern void get_join_memo_size1(MemoInfoData1 *result, RelOptInfo *joinrel, int level, char *quals,
		bool isParameterized);
extern void get_baserel_memo_size1(MemoInfoData1 *result, const List *lrelName, int level, int lclauses, char *quals);
extern void store_join(List *lrelName, int level);
extern void export_join(FILE *file);

#endif /* MEMOCACHE_H_ */

