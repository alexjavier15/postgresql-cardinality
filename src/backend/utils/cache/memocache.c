/*
 * mamocache.c
 *
 *  Created on: 15 sept. 2014
 *      Author: alex
 */

#include "postgres.h"

#include "utils/memocache.h"
#include "storage/fd.h"
#include "optimizer/pathnode.h"
#include "lib/stringinfo.h"
#include "nodes/print.h"
#include "nodes/readfuncs.h"
#include "optimizer/cost.h"

#define print_relation(str1, str2, memorelation)	 \
	str1 = nodeToString((memorelation)->relationname);	\
			str2 = nodeToString((memorelation)->clauses);	\
			printf("%d %s %lf %d %s\n", (memorelation)->level,\
					debackslash(str1, strlen(str1)), (memorelation)->rows,	\
					(memorelation)->clauseslength,	\
					debackslash(str2, strlen(str2)));	\
			fflush(stdout);

#define DEFAULT_MAX_JOIN_SIZE 	5

#define isFullMatched(result) \
		(result->found == 2 ? 1 : 0)
#define isMatched(result) \
		(result->found > 0 ? 1 : 0)

typedef struct MemoQuery {
	CacheTag type;
	int length;
	dlist_head *content;
	int size;
	char *id;
	dlist_node list_node;
} MemoQuery;
typedef struct MemoRelation {
	dlist_node list_node;
	int level;
	List* relationname;
	double rows;
	int clauseslength;
	List* clauses;
} MemoRelation;

typedef struct CachedJoins {
	CacheTag type;
	int length;
	dlist_head *content;
	int size;

} CachedJoins;

typedef struct MemoCache {

	dlist_head content;

} MemoCache;

FILE *file;
FILE *joincache;
static struct MemoCache memo_cache;
static struct CachedJoins join_cache;

CachedJoins *join_cache_ptr = &join_cache;
MemoCache *memo_cache_ptr = &memo_cache;

static int read_line(StringInfo str, FILE *file);
static void InitJoinCache(void);
static void InitMemoCache(void);

static List * build_string_list(char * stringList);
static void buildSimpleStringList(StringInfo str, List *list);
//static void fill_memo_cache(struct MemoCache *static void compt_lists(MemoInfoData *result, List *str1, const List *str2);

static MemoRelation *newMemoRelation(void);
static void cmp_lists(MemoInfoData1 *result, List *str1, const List *str2);
static MemoQuery * find_seeder_relations(MemoRelation **relation1, MemoRelation **relation2,
		MemoRelation **commonrelation, List * targetrel, int level, int rellen1);
static void set_estimated_join_rows(MemoRelation *relation1, MemoRelation *relation2, MemoRelation * commonrelation,
		MemoRelation *target);
static void check_NoMemo_queries(void);
static void add_relation(const void * memo, MemoRelation * relation, int rellen);
static void readAndFillFromFile(FILE *file, const void *sink);
static void printContentRelations(CacheM *cache);
static void discard_existing_joins(void);
static MemoRelation * contains(CacheM* cache, List * relname, int level);
void InitCachesForMemo(void) {

	dlist_init(&memo_cache_ptr->content);

	file = AllocateFile("memoTxt.txt", "rb");
	joincache = AllocateFile("joins.txt", "rb");
	if (enable_memo)
		InitMemoCache();
	InitJoinCache();

	if (file != NULL)
		fclose(file);

	if (joincache != NULL)
		fclose(joincache);
}

void InitMemoCache(void) {

	MemoQuery *memo;
	memo = (MemoQuery *) palloc0(sizeof(MemoQuery));
	memo->type = M_MemoQuery;
	memo->length = DEFAULT_MAX_JOIN_SIZE;
	memo->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
	//printf("Initializing Memo cache\n------------------------\n");
	if (file != NULL)
		readAndFillFromFile(file, memo);
	dlist_push_tail(&memo_cache_ptr->content, &memo->list_node);
	//printMemoCache();

	//printf("Memo cache ending\n-----------------------\n");

}
void InitJoinCache(void) {

	//printf("Initializing join cache\n------------------------\n");
	join_cache_ptr->type = M_JoinCache;
	join_cache_ptr->length = DEFAULT_MAX_JOIN_SIZE;
	join_cache_ptr->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
	if (joincache != NULL)
		readAndFillFromFile(joincache, join_cache_ptr);
	//printContentRelations((CacheM *) join_cache_ptr);

	//printf("Join cache ending\n-----------------------\n");

	if (enable_memo) {
		discard_existing_joins();
		check_NoMemo_queries();
	}
	//printf("New memo cache state:\n-----------------------\n");

	//printMemoCache();
	//printf("End\n-----------------------\n");
	//printf("New join cache state:\n-----------------------\n");
	//printContentRelations((CacheM *) join_cache_ptr);
	//printf("End\n-----------------------\n");

}
void discard_existing_joins(void) {

	dlist_mutable_iter iter;
	dlist_iter iter1;
	dlist_iter iter2;

	dlist_foreach(iter1, &memo_cache_ptr->content) {

		MemoQuery *query = dlist_container(MemoQuery,list_node, iter1.cur);

		int i;

		for (i = 0; i < join_cache_ptr->length; ++i) {

			dlist_foreach_modify (iter, &join_cache_ptr->content[i]) {
				MemoRelation *cachedjoin = dlist_container(MemoRelation,list_node, iter.cur);
				dlist_foreach (iter2, &query->content[i]) {
					List *result;
					MemoRelation *existingjoin = dlist_container(MemoRelation,list_node, iter2.cur);
					result = list_difference(cachedjoin->relationname, existingjoin->relationname);
					if (result == NIL)
						dlist_delete(&(cachedjoin->list_node));

				}
			}
		}
	}
}
void readAndFillFromFile(FILE *file, const void *sink) {

	int ARG_NUM = 4;
	char srelname[DEFAULT_SIZE];
	char *cquals = NULL;
	StringInfoData str;

	int rellen;
	MemoRelation * tmprelation;

	cquals = (char *) palloc(DEFAULT_SIZE * sizeof(char));

	//printf("Initializing cache... \n");
	initStringInfo(&str);

	memset(cquals, '\0', DEFAULT_SIZE);
	if (cquals == NULL) {
		//printf("error allocating memory \n");
		//fflush(stdout);

		return;

	}

	fflush(stdout);

	while (read_line(&str, file) != EOF) {
		tmprelation = newMemoRelation();
		//Make sure that we have enough space allocatd to read the quals
		if (sscanf(str.data, "%d\t%s\t%*d\t%lf\t%d", &(tmprelation->level), srelname, &(tmprelation->rows),
				&(tmprelation->clauseslength)) == ARG_NUM) {

			if (tmprelation->clauseslength >= DEFAULT_SIZE) {

				cquals = (char *) repalloc(cquals, (tmprelation->clauseslength + 1) * sizeof(char));
				if (cquals == NULL) {
					//printf("error allocating memory \n");
					//fflush(stdout);

					return;

				}
				memset(cquals, '0', tmprelation->clauseslength + 1);
				cquals[tmprelation->clauseslength] = '\0';

			}

			if (sscanf(str.data, "%*d\t%*s\t%*d\t%*d\t%*d\t%[^\n]", cquals) == 1) {

				tmprelation->relationname = build_string_list(srelname);
				tmprelation->clauses = build_string_list(cquals);
			}
			rellen = list_length(tmprelation->relationname);

			add_relation(sink, tmprelation, rellen);
		}

		memset(srelname, '\0', strlen(srelname) + 1);
		memset(cquals, '\0', DEFAULT_SIZE);
		resetStringInfo(&str);

	}
	pfree(cquals);
}
static int read_line(StringInfo str, FILE *file) {
	int ch;

	while (((ch = fgetc(file)) != '\n') && (ch != EOF)) {

		appendStringInfoChar(str, ch);

	}
	return ch;
}

static MemoRelation * newMemoRelation(void) {

	MemoRelation * relation;
	relation = (MemoRelation *) palloc(sizeof(MemoRelation));
	return relation;
}
void add_relation(const void * sink, MemoRelation * relation, int rellen) {
	if (IsACache(sink,Invalid)) {
		return;

	} else {
		CacheM * cache = (CacheM *) sink;

		if (rellen > cache->length) {

			cache->content = (dlist_head *) repalloc(cache->content, (rellen) * sizeof(dlist_head));
			cache->length = rellen;

		}

		dlist_push_tail(&cache->content[--rellen], &relation->list_node);

		cache->size = cache->size + 1;
	}

}
void get_baserel_memo_size1(MemoInfoData1 *result, const List *lrelName, int level, int clauses_length, char *quals) {

	List *lquals2 = NIL;

	MemoInfoData1 resultName;
	MemoInfoData1 *resultName_ptr;
	/*char *str1;
	 char *str2;*/
	dlist_iter iter;
	dlist_iter iter1;

	resultName_ptr = &resultName;

	result->found = 0;
	result->rows = -1;

	lquals2 = build_string_list(quals);

	dlist_foreach(iter, &memo_cache_ptr->content) {

		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);

		dlist_foreach (iter1, &query->content[0]) {
			MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter1.cur);
			if (memorelation->level == level) {
				cmp_lists(resultName_ptr, memorelation->relationname, lrelName);

				if (isFullMatched(resultName_ptr)) {

					cmp_lists(result, memorelation->clauses, lquals2);

					if (isMatched(result)) {
						//	printf(" Matched base relation! :\n");
						//	print_relation(str1, str2, memorelation);

						result->rows = memorelation->rows;
						return;
					}
				}

			}

		}

	}
}

void get_join_memo_size1(MemoInfoData1 *result, RelOptInfo *joinrel, int level, char *quals, bool isParameterized) {
	/*char *str1;
	 char *str2;
	 */
	dlist_iter iter;
	dlist_iter iter1;

	result->found = 0;
	result->rows = -1;

	dlist_foreach(iter, &memo_cache_ptr->content) {
		int rellen = list_length(joinrel->rel_name);
		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);

		dlist_foreach (iter1, &query->content[rellen - 1]) {
			List * ldiff;
			MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter1.cur);
			if (memorelation->level == level) {

				ldiff = list_difference(joinrel->rel_name, memorelation->relationname);
				if (ldiff == NIL) {
					//printf(" Matched join relation! :\n");
					//	print_relation(str1, str2, memorelation);
					result->rows = memorelation->rows;
					result->found = 2;
					return;

				}

			}
		}

	}

	//printf(" Not Matched join relation! :\n");

}
static List * build_string_list(char * stringList) {
	char *save;
	char *s = ",";
	char *tmp = NULL;
	char *newstring;
	Value *value;
	List *result = NIL;

	tmp = strtok_r(stringList, s, &save);
	while (tmp != NULL) {
		newstring = (char *) palloc((strlen(tmp) + 1) * sizeof(char));
		strcpy(newstring, tmp);
		newstring[strlen(tmp)] = '\0';

		value = makeString(newstring);
		result = lappend(result, value);
		tmp = strtok_r(NULL, s, &save);

	}
	Assert(result != NIL);
	return result;
}

void cmp_lists(MemoInfoData1 * result, List *lleft, const List *lright) {
	List * ldiff = NIL;

	if (list_length(lleft) >= list_length(lright)) {
		ldiff = list_difference(lleft, lright);
		result->found = (ldiff == NIL) + ((ldiff == NIL) && (list_length(lleft) == list_length(lright)))
				+ ((ldiff != NIL) && (list_length(lleft) > list_length(lright)));

		result->nbmatches = ldiff != NIL ? list_length(lleft) - list_length(ldiff) : list_length(lleft);
		result->unmatches = ldiff;
		result->matches = list_intersection(lleft, lright);
	}
}

static void check_NoMemo_queries(void) {
	MemoRelation *relation1 = NULL;
	MemoRelation *relation2 = NULL;
	MemoRelation *commonrelation = NULL;

	MemoRelation *tmprelation = NULL;
	MemoQuery *query = NULL;
	bool found_new = true;
	/*	char *str1;
	 char *str2;*/

	int rellen;
	dlist_mutable_iter iter;

	while (found_new) {
		int i;
		found_new = false;

		for (i = 0; i < join_cache_ptr->length; ++i) {

			dlist_foreach_modify (iter, &join_cache_ptr->content[i]) {
				MemoRelation *target = dlist_container(MemoRelation,list_node, iter.cur);
				rellen = list_length(target->relationname);
				//print_relation(str1, str2, target);
				query = find_seeder_relations(&relation1, &relation2, &commonrelation, target->relationname,
						target->level, rellen);

				//check for candidates matches existence
				if (relation1 != NULL && relation2 != NULL && query != NULL) {
					// delete the current NoMemoRelation from the un_cache

					tmprelation = newMemoRelation();

					//Call the calculation function to get estimated rows and merk the RelOptInfo
					//struct

					set_estimated_join_rows(relation1, relation2, commonrelation, target);
					tmprelation->relationname = target->relationname;
					tmprelation->rows = target->rows;
					tmprelation->level = target->level;
					add_relation(query, tmprelation, rellen);
					found_new = true;
					dlist_delete(&(target->list_node));

				}
				relation1 = NULL;
				relation2 = NULL;
				commonrelation = NULL;
				//printf("\n");
			}
		}
	}

}
static MemoQuery * find_seeder_relations(MemoRelation **relation1, MemoRelation **relation2,
		MemoRelation **commonrelation, List * targetrel, int level, int joinlen) {

	dlist_iter iter;
	dlist_iter iter1;
	MemoCache *cache;
	MemoInfoData1 resultName;
	MemoInfoData1 *resultName_ptr;
	List *tmp = NIL;
	/*	char *str1 = NULL;
	 char *str2 = NULL;*/

	cache = &memo_cache;
	resultName_ptr = &resultName;

	if (list_length(targetrel) == 1) {
		printf("got 1");
		return NULL;

	} else {

		dlist_foreach(iter, &cache->content) {

			MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);

			// we fetch the joins with the expected rellen number of relations
			dlist_foreach (iter1, &query->content[joinlen-1]) {
				MemoRelation * commonrel = NULL;
				MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter1.cur);
				if (memorelation->level == level) {

					cmp_lists(resultName_ptr, memorelation->relationname, targetrel);
					//printf("matches %d \n", resultName_ptr->nbmatches);
					// Check if we had the expected number of matches in join

					//If *relation is null we are in out first loop

					if (*relation1 == NULL) {
						//printf("Entering in 1 loop with %d matches \n", resultName_ptr->nbmatches);
						// In the first pass we need to find a relation who differs from the target
						// relation just from one base relation

						commonrel = contains((CacheM *) query, resultName_ptr->matches, level);

						if (resultName_ptr->nbmatches == (joinlen - 1) && commonrel != NULL) {
							//Mark the first relation and build the resulted expected target join
							//relation for matching
							*relation1 = memorelation;
							*commonrelation = commonrel;
							//printf("Common realation:\n");
							//print_relation(str1, str2, *commonrelation);
							tmp = list_copy(targetrel);
							tmp = list_concat(tmp, resultName_ptr->unmatches);
							find_seeder_relations(relation1, relation2, commonrelation, tmp, level, list_length(tmp));
							if (*relation2 != NULL) {
								return query;
							}
						}

					} else {
						//printf("chcking second loop");
						Assert(*relation2 == NULL);
						//In the second pass we have  to match a full join  then
						//the number of base realtion matches are equal to joinlen

						if (resultName_ptr->nbmatches == joinlen) {
							*relation2 = memorelation;

							/*printf("Found candidates relations : \n");
							 print_relation(str1, str2, *relation1);
							 print_relation(str1, str2, *relation2);
							 printf("-------------------------------\n");
							 */
							return NULL;
						}

					}

				}

			}

		}
		return NULL;
	}

}

MemoRelation * contains(CacheM* cache, List * relname, int level) {
	dlist_iter iter;
	int rellen = list_length(relname);

	if (relname == NIL)

		return NULL;

	dlist_foreach(iter, &cache->content[rellen-1]) {

		MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter.cur);
		if (list_difference(memorelation->relationname, relname) == NIL && memorelation->level == level)
			return memorelation;

	}
	return NULL;
}
static void set_estimated_join_rows(MemoRelation *relation1, MemoRelation *relation2, MemoRelation * commonrelation,
		MemoRelation *target) {
	double rows;

	rows = (relation2->rows / relation1->rows) * commonrelation->rows;

	target->rows = rows <= 0 ? 1 : rows;

	/*printf("Injected new estimated size  %lf for : \n", rows);
	 print(target->relationname);
	 printf("\n");*/
	fflush(stdout);

}
void store_join(List *lrelName, int level) {
	StringInfoData str;
	MemoRelation *relation;
	initStringInfo(&str);
	relation = (MemoRelation*) palloc(sizeof(MemoRelation));

	//appendStringInfoString(&str, " ");

	//appendStringInfoString(&str, " ");
	relation->level = level;
	relation->relationname = lrelName;
	add_relation(join_cache_ptr, relation, list_length(lrelName));
	/*printf("Staring printing join cache\n -------------------------------- \n");
	 printContentRelations((CacheM *) join_cache_ptr);
	 printf("Ending printing join cache\n -------------------------------- \n");*/

}
void export_join(FILE *file) {
	StringInfoData str;

	dlist_iter iter;

	initStringInfo(&str);

	if (file != NULL) {
		int i;
		for (i = 0; i < join_cache_ptr->length; ++i) {

			dlist_foreach (iter, &join_cache_ptr->content[i]) {
				MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter.cur);
				buildSimpleStringList(&str, memorelation->relationname);

				fprintf(file, "%d\t%s\t0\t0\t0\t<>\n", memorelation->level, str.data);
				resetStringInfo(&str);
			}

		}
		fclose(file);

	}
}
void printMemoCache(void) {
	dlist_iter iter;

	dlist_foreach(iter, &memo_cache_ptr->content) {

		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);

		printContentRelations((CacheM*) query);
		printf("Cache size : %d realtions.\n", query->size);
		fflush(stdout);
	}
}
void printContentRelations(CacheM *cache) {

	char * str1;
	char * str2;
	dlist_iter iter;

	int i;
	for (i = 0; i < cache->length; ++i) {

		dlist_foreach (iter, &cache->content[i]) {
			MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter.cur);
			print_relation(str1, str2, memorelation);

		}

	}

}
void buildSimpleStringList(StringInfo str, List *list) {

	ListCell *lc;

	foreach (lc, list) {

		appendStringInfoString(str, ((Value *) lfirst(lc))->val.str);
		if (lnext(lc)) {

			appendStringInfoString(str, ",");

		}
	}
}
