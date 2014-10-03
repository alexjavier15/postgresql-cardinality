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
#include "nodes/relation.h"

#include <openssl/md5.h>

#define IsIndex(relation)	((relation)->nodeType == T_IndexOnlyScan  \
		|| (relation)->nodeType == T_IndexScan || (relation)->nodeType == T_BitmapHeapScan)
#define IsUntaged(relation) ((relation)->nodeType == T_Invalid )
#define DEFAULT_MAX_JOIN_SIZE 	10

#define isFullMatched(result) \
		((result)->found == 2 ? 1 : 0)
#define isMatched(result) \
		((result)->found > 0 ? 1 : 0)

typedef struct MemoQuery {
	CacheTag type;
	int length;
	dlist_head *content;
	int size;
	char *id;
	dlist_node list_node;
} MemoQuery;

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
MemoQuery *memo_query_ptr;
CachedJoins *join_cache_ptr = &join_cache;
MemoCache *memo_cache_ptr = &memo_cache;
bool initialized = false;

static int read_line(StringInfo str, FILE *file);
static void InitJoinCache(void);
static void InitMemoCache(void);

static List * build_string_list(char * stringList, ListType type);
static void buildSimpleStringList(StringInfo str, List *list);
//static void fill_memo_cache(struct MemoCache *static void compt_lists(MemoInfoData *result, List *str1, const List *str2);

static MemoRelation *newMemoRelation(void);
static void cmp_lists(MemoInfoData1 *result, List *str1, const List *str2);
static MemoQuery * find_seeder_relations(MemoRelation **relation1, MemoRelation **relation2,
		MemoRelation **commonrelation, List * targetName, List * targetClauses, int level, int rellen1);
static void set_estimated_join_rows(MemoRelation *relation1, MemoRelation *relation2, MemoRelation * commonrelation,
		MemoRelation *target);
static void check_NoMemo_queries(void);
static void add_relation(void * memo, MemoRelation * relation, int rellen);
static void readAndFillFromFile(FILE *file, void *sink);
static void printContentRelations(CacheM *cache);
static void discard_existing_joins(void);
static void contains(MemoRelation ** relation, CacheM* cache, List * relname, int level);
static int equals(MemoRelation *rel1, MemoRelation *rel2);
MemoClause * parse_clause(char * clause);
static char * parse_args(StringInfo buff, char * arg);
static void set_path_sizes(PlannerInfo *root, RelOptInfo *rel, Path *path, double *loop_count, bool isIndex);

static void print_relation(MemoRelation *memorelation) {
	char *str1 = nodeSimToString_((memorelation)->relationname);
	char *str2 = nodeSimToString_((memorelation)->clauses);
	printf("%d %s %lf %lf %d %s\n", (memorelation)->level, str1, (memorelation)->rows, (memorelation)->tuples,
			(memorelation)->clauseslength, str2);
	pfree(str1);
	pfree(str2);
	fflush(stdout);

}

static List * restictInfoToMemoClauses(List *clauses) {
	char *dc = NULL;
	List *lquals = NIL;
	int rest;

	StringInfoData str1;

	initStringInfo(&str1);
	str1.reduced = true;
	build_selec_string(&str1, clauses, &rest);
	if (rest) {

		dc = debackslash(str1.data, str1.len);
		lquals = build_string_list(dc, M_CLAUSE);
	}
	return lquals;

}

//int md5(FILE *inFile);
void InitCachesForMemo(void) {

	joincache = AllocateFile("joins.txt", "rb");
	if (enable_memo && !initialized) {
		dlist_init(&memo_cache_ptr->content);

		file = AllocateFile("memoTxt.txt", "rb");
		InitMemoCache();
		initialized = true;

	}
	if (join_cache_ptr->type != M_JoinCache)
		InitJoinCache();
}

void InitMemoCache(void) {

	MemoQuery *memo;
	memo = (MemoQuery *) palloc0(sizeof(MemoQuery));
	memo->type = M_MemoQuery;
	memo->length = DEFAULT_MAX_JOIN_SIZE;
	memo->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
	printf("Initializing Memo cache\n------------------------\n");
	if (file != NULL)
		readAndFillFromFile(file, memo);
	memo_query_ptr = memo;
	dlist_push_tail(&memo_cache_ptr->content, &memo->list_node);
	printMemoCache();
	printf("Memo cache ending\n-----------------------\n");

}
void InitJoinCache(void) {

	//printf("Initializing join cache\n------------------------\n");
	join_cache_ptr->type = M_JoinCache;
	join_cache_ptr->length = DEFAULT_MAX_JOIN_SIZE;
	join_cache_ptr->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
	if (joincache != NULL && enable_memo)
		readAndFillFromFile(joincache, join_cache_ptr);
	/*
	 printContentRelations((CacheM *) join_cache_ptr);

	 printf("Join cache ending\n-----------------------\n");*/

	//printf("New join cache state:\n-----------------------\n");
	//printContentRelations((CacheM *) join_cache_ptr);
	//printf("End\n-----------------------\n");
	if (enable_memo) {
		set_memo_join_sizes();
	}
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
					MemoRelation *existingjoin = dlist_container(MemoRelation,list_node, iter2.cur);

					if (equals(cachedjoin, existingjoin))
						dlist_delete(&(cachedjoin->list_node));

				}
			}
		}
	}

}
static void printClause(List *clauses) {
	int rest;
	StringInfoData str1;
	char *dc = NULL;
	initStringInfo(&str1);
	build_selec_string(&str1, clauses, &rest);
	if (rest) {

		dc = debackslash(str1.data, str1.len);
		printf("clauses : %s\n", dc);
	}

}
void readAndFillFromFile(FILE *file, void *sink) {

	int ARG_NUM = 7;
	char srelname[DEFAULT_SIZE];
	char *cquals = NULL;
	StringInfoData str;

	int rellen;
	MemoRelation * tmprelation;

	cquals = (char *) palloc(DEFAULT_SIZE * sizeof(char));

	//printf("Initializing cache... \n");
	initStringInfo(&str);
	str.reduced = true;

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
		if (sscanf(str.data, "%d\t%d\t%s\t%*d\t%lf\t%d\t%lf\t%d", &(tmprelation->nodeType), &(tmprelation->level),
				srelname, &(tmprelation->rows), &(tmprelation->loops), &(tmprelation->removed_rows),
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

			if (sscanf(str.data, "%*d\t%*d\t%*s\t%*d\t%*d\t%*d\t%*d\t%*d\t%[^\n]", cquals) == 1) {

				tmprelation->relationname = build_string_list(srelname, M_NAME);
				if (strcmp(cquals, "<>") == 0)
					tmprelation->clauses = NIL;
				else
					tmprelation->clauses = build_string_list(cquals, M_CLAUSE);
				if (IsIndex(tmprelation))
					tmprelation->test = false;
				tmprelation->tuples = 0;

			}
			rellen = list_length(tmprelation->relationname);

			add_relation(sink, tmprelation, rellen);
		}

		memset(srelname, '\0', strlen(srelname) + 1);
		memset(cquals, '\0', DEFAULT_SIZE);
		resetStringInfo(&str);

	}
	pfree(str.data);
	pfree(cquals);
}
static int read_line(StringInfo str, FILE *file) {
	int ch;

	while (((ch = fgetc(file)) != '\n') && (ch != EOF)) {

		appendStringInfoChar(str, ch);

	}
	return ch;
}
static void delete_memorelation(MemoRelation *memorelation) {
	pfree(memorelation->relationname);
	if (memorelation->clauses != NIL)
		pfree(memorelation->clauses);
	pfree(memorelation);

}

static MemoRelation * newMemoRelation(void) {

	MemoRelation * relation;
	relation = (MemoRelation *) palloc(sizeof(MemoRelation));
	relation->nodeType = 0;
	relation->loops = 1;
	relation->removed_rows = 0;
	relation->test = true;
	relation->tuples = 0;
	relation->rows = 0;
	relation->level = 0;
	relation->clauses = NIL;
	relation->relationname = NIL;
	return relation;
}
void add_relation(void * sink, MemoRelation * relation, int rellen) {
	if (IsACache(sink,Invalid)) {
		return;

	} else {
		CacheM * cache = (CacheM *) sink;

		if (rellen > cache->length) {

			if (IsACache(sink,JoinCache)) {

				join_cache_ptr->content = (dlist_head *) repalloc(join_cache_ptr->content,
						(rellen + 1) * sizeof(dlist_head));
				cache->length = rellen;
			}

			if (IsACache(sink,MemoQuery)) {
				cache->content = (dlist_head *) repalloc(((MemoQuery *) sink)->content, (rellen) * sizeof(dlist_head));
				cache->length = rellen;
			}
			MemSetAligned(&cache->content[rellen - 1], 0, sizeof(dlist_head));
		}

		dlist_push_tail(&cache->content[rellen - 1], &relation->list_node);

		cache->size = cache->size + 1;
	}

}
void get_baserel_memo_size1(MemoInfoData1 *result, List * relname, int level, List *quals, bool isIndex) {
	MemoRelation * memo_rel = NULL;

	/*char *str1;
	 char *str2;*/
	result->loops = 0;
	result->found = 0;
	result->rows = -1;

	memo_rel = get_Memorelation(result, relname, level, quals, 2);

	if (memo_rel) {
		//	printf(" Matched base relation! :\n");
		//	print_relation(str1, str2, memorelation);
		result->loops = memo_rel->loops;
		result->rows = memo_rel->rows;
		return;
	}
}

void get_join_memo_size1(MemoInfoData1 *result, RelOptInfo *joinrel, int level, char *quals, bool isParameterized) {

	dlist_iter iter;

	result->found = 0;
	result->rows = -1;

	dlist_foreach(iter, &memo_cache_ptr->content) {
		MemoRelation *memorelation = NULL;
		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);

		contains(&memorelation, (CacheM *) query, joinrel->rel_name, level);
		if (memorelation != NULL && !IsIndex(memorelation)) {
			/*printf("Found join: \n");
			 print_relation( memorelation);*/
			result->rows = memorelation->rows;
			result->found = 2;

			return;

		}
	}

}

MemoRelation * get_Memorelation(MemoInfoData1 *result, List *lrelName, int level, List *clauses, int isIndex) {
	dlist_iter iter;
//	char *str3, *str4;
	MemoRelation *relation;
	/*char *str1;
	 char *str2;*/
	char *dc = NULL;
	List *lquals = NIL;
	int rest;

	StringInfoData str1;

	initStringInfo(&str1);
	str1.reduced = true;

	relation = NULL;
	build_selec_string(&str1, clauses, &rest);
	if (rest) {

		dc = debackslash(str1.data, str1.len);
	}

	dlist_foreach(iter, &memo_cache_ptr->content) {

		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);

		contains(&relation, (CacheM *) query, lrelName, level);
		if (relation != NULL) {
			if (rest)
				lquals = build_string_list(dc, M_CLAUSE);
			cmp_lists(result, relation->clauses, lquals);
			if (isFullMatched(result)) {
				pfree(str1.data);
				return relation;

			}
			if (isIndex == 2 && isMatched(result) && !IsIndex(relation))

			{
				pfree(str1.data);
				return relation;

			}
		}
	}

	return NULL;

}
int get_memo_removed(List *relation_name, List *clauses, int level) {

	dlist_iter iter;
	MemoRelation *relation = NULL;
	int loops = 0;

	dlist_foreach(iter, &memo_cache_ptr->content) {
		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);
		contains(&relation, (CacheM *) query, relation_name, level);
		if (relation != NULL && IsIndex(relation)) {
			loops = relation->removed_rows;
		}
	}

	return loops;
}
static void remove_par(char * stringList) {

	int newlen = strlen(stringList) - 1;
	stringList[newlen] = '\0';

}

static List * build_string_list(char * stringList1, ListType type) {
	char *save;
	char *s = ",";
	char *tmp = NULL;
	char *stringList = stringList1;
	char *newstring;
	void *value = NULL;
	List *result = NIL;
	if (type == M_CLAUSE) {
		remove_par(stringList);
		stringList = &stringList[1];
	}

	tmp = strtok_r(stringList, s, &save);
	while (tmp != NULL) {
		newstring = (char *) palloc((strlen(tmp) + 1) * sizeof(char));
		strcpy(newstring, tmp);
		newstring[strlen(tmp)] = '\0';
		switch (type) {

		case M_NAME:
			value = makeString(newstring);

			break;
		case M_CLAUSE:

			value = parse_clause(newstring);
			break;
		default:
			printf("Error building list");
			break;
		}
		tmp = strtok_r(NULL, s, &save);
		if (value != NULL)
			result = lappend(result, value);

	}
	return result;
}

MemoClause * parse_clause(char * clause) {
	MemoClause *clause_ptr = (MemoClause *) palloc(sizeof(MemoClause));
	StringInfoData buff;
	char * args = NULL;
	char * arg = NULL;
	Value* value = NULL;
	int arg_len = 7;
	initStringInfo(&buff);
	clause_ptr->type = T_MemoClause;
	clause_ptr->opno = 0;
	clause_ptr->args = NIL;

	if (sscanf(clause, "{  :opno %u", &clause_ptr->opno) == 1) {
		args = strstr(clause, ":args (");
		if (args != NULL) {

			args = &args[arg_len];
			while ((args = parse_args(&buff, args)) != NULL) {
				arg = (char *) palloc((buff.len + 1) * sizeof(char));
				arg = strcpy(arg, buff.data);
				arg[buff.len] = '\0';
				value = makeString(arg);
				clause_ptr->args = lappend(clause_ptr->args, value);
				resetStringInfo(&buff);
			}

			pfree(buff.data);
		}

	} else if (clause != NULL && strcmp(clause, "<>") == 0) {
		clause_ptr = NULL;

	} else {

		printf("syntax error when parsing clause: %s \n", clause);
		pfree(buff.data);
		pfree(clause_ptr);
		clause_ptr = NULL;
	}

	return clause_ptr;

}

char * parse_args(StringInfo buff, char * arg) {
	int i = 0;
	int balanced = 0;
	char * result = NULL;
	if (arg[i] == '{') {
		i++;
		balanced++;
		while (balanced != 0 && arg[i] != '\0') {

			if (arg[i] == '}')

				balanced--;
			else if (arg[i] == '{')
				balanced++;
			else if (balanced < 0) {
				printf("Error parsing arg");
				break;

			}
			appendStringInfoChar(buff, arg[i]);
			i++;
		}
		buff->len--;
		buff->data[buff->len] = '\0';

		if (balanced == 0)

			result = &arg[i];
	}
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
static bool equalSet(const List *a, const List *b) {
	const ListCell *item_a;
	if (a->type != a->type) {
		printf("not equal type\n");

		return false;
	}

	foreach(item_a, a) {
		if (!list_member_remove(b, lfirst(item_a))) {
			printf("member not found\n");

			return false;
		}

	}
	return true;

}

static void check_NoMemo_queries(void) {
	MemoRelation *relation1 = NULL;
	MemoRelation *relation2 = NULL;
	MemoRelation *commonrelation = NULL;
	MemoQuery *query = NULL;
	bool found_new = true;
	/*char *str1;
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
				/*printf("Checking :\n**********************\n");
				 print_relation(str1, str2, target);
				 printf("********************************\n");*/
				query = find_seeder_relations(&relation1, &relation2, &commonrelation, target->relationname,
						target->clauses, target->level, rellen);

				//check for candidates matches existence
				if (relation1 != NULL && relation2 != NULL && query != NULL) {
					// delete the current NoMemoRelation from the un_cache

					//Call the calculation function to get estimated rows and merk the RelOptInfo
					//struct
					found_new = true;
					set_estimated_join_rows(relation1, relation2, commonrelation, target);
					dlist_delete(&(target->list_node));

					if (target->rows >= 1) {

						add_relation(query, target, rellen);

					} else {
						pfree(target);
					}

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
		MemoRelation **commonrelation, List * targetName, List * targetClauses, int level, int joinlen) {

	dlist_iter iter;
	dlist_iter iter1;
	MemoInfoData1 resultName;
	MemoInfoData1 *resultName_ptr;

	List *tmp1 = NIL;
	//char *str2 = NULL;

	resultName_ptr = &resultName;

	if (list_length(targetName) == 1) {
		printf("got 1");
		return NULL;

	} else {

		dlist_foreach(iter, &memo_cache_ptr->content) {

			MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);

			// we fetch the joins with the expected rellen number of relations
			dlist_foreach (iter1, &query->content[joinlen-1]) {
				MemoRelation * commonrel = NULL;
				MemoRelation * diffrel = NULL;
				List *clauses = NIL;
				MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter1.cur);
				if (memorelation->level == level && memorelation->test) {

					cmp_lists(resultName_ptr, memorelation->relationname, targetName);

					//printf("matches %d \n", resultName_ptr->nbmatches);
					// Check if we had the expected number of matches in join

					//If *relation is null we are in out first loop

					if (*relation1 == NULL) {
						//printf("Entering in 1 loop with %d matches \n", resultName_ptr->nbmatches);
						// In the firstfind_seeder_relations pass we need to find a relation who differs from the target
						// relation just from one base relation

						contains(&commonrel, (CacheM *) query, resultName_ptr->matches, level);
						if (commonrel != NULL) {
							contains(&diffrel, (CacheM *) query, resultName_ptr->unmatches, level);
							clauses = list_union(memorelation->clauses, targetClauses);
							/*printf("Common relation is:\n");

							 print_relation(str1, str2, commonrel);*/
							if ((resultName_ptr->nbmatches == (joinlen - 1)) && diffrel != NULL && !IsIndex(commonrel)
									&& !IsIndex(diffrel)) {
								//Mark the first relation and build the resulted expected target join
								//relation for matching
								*relation1 = memorelation;
								*commonrelation = commonrel;

								tmp1 = list_union(targetName, memorelation->relationname);
								/*printf("Looking for relation:\n");
								 print_list(str1, tmp1);
								 printf("With clauses :\n");
								 print_list(str1, clauses);*/
								return find_seeder_relations(relation1, relation2, commonrelation, tmp1, clauses, level,
										list_length(tmp1));

							}
						}

					} else {
						List *b = list_copy(targetClauses);
						//printf("chcking second loop");
						Assert(*relation2 == NULL);
						//In the second pass we have  to match a full join  then
						//the number of base realtion matches are equal to joinlen

						if (resultName_ptr->nbmatches == joinlen && equalSet(memorelation->clauses, b)) {
							pfree(b);
							*relation2 = memorelation;
							/*

							 printf("Found candidates relations : \n");
							 print_relation(str1, str2, *relation1);
							 print_relation(str1, str2, *relation2);
							 printf("-------------------------------\n");
							 */

							return query;
						}

					}

				}
			}

		}

	}
	return NULL;
}

void contains(MemoRelation ** relation, CacheM* cache, List * relname, int level) {
	dlist_iter iter;
	//char *str1, *str2 = NULL;
	MemoInfoData1 result;
	int rellen = list_length(relname);
	MemoRelation *memorelation = NULL;
	if (relname == NIL)
		return;

	dlist_foreach(iter, &cache->content[rellen-1]) {

		memorelation = dlist_container(MemoRelation,list_node, iter.cur);
		cmp_lists(&result, memorelation->relationname, relname);

		if (isFullMatched(&result) && memorelation->level == level) {

			*relation = &(*memorelation);
			break;;
		}

	}

}
static void set_estimated_join_rows(MemoRelation *relation1, MemoRelation *relation2, MemoRelation * commonrelation,
		MemoRelation *target) {
	double rows;

	rows = (relation2->rows / relation1->rows) * commonrelation->rows;
	target->rows = rows <= 0 ? 1 : rows;

	/*printf("Injected new estimated size  %lf for : \n", rows);
	 print(target->relationname);
	 printf("\n");
	 fflush(stdout);*/

}
void store_join(List *lrelName, int level, List *clauses, double rows) {
	StringInfoData str;
	MemoRelation *relation;
	initStringInfo(&str);

	relation = newMemoRelation();
	Assert(lrelName!=NIL);

//appendStringInfoString(&str, " ");

//appendStringInfoString(&str, " ");
	relation->rows = rows;
	relation->level = level;
	relation->relationname = list_copy(lrelName);
	relation->clauses = list_copy(clauses);
	add_relation(join_cache_ptr, relation, list_length(lrelName));
	/*printf("Relation Stored\n -------------------------------- \n");
	 print_relation(str1, str2, relation);
	 printf("Ending printing join cache\n -------------------------------- \n");*/

}
void export_join(FILE *file) {
	StringInfoData str;

	int i;
	StringInfoData clauses;

	dlist_iter iter;

	if (join_cache_ptr->size != 0 && file != NULL) {
		initStringInfo(&str);
		initStringInfo(&clauses);
		clauses.reduced = true;

		for (i = 0; i < join_cache_ptr->length; ++i) {

			dlist_foreach (iter, &join_cache_ptr->content[i]) {
				MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter.cur);
				buildSimpleStringList(&str, memorelation->relationname);
				nodeSimToString(memorelation->clauses, &clauses);

				fprintf(file, "0\t%d\t%s\t%.0f\t0\t1\t0\t%d\t%s\n", memorelation->level, str.data, memorelation->rows,
						clauses.len, clauses.data);

				resetStringInfo(&str);
				resetStringInfo(&clauses);

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

	dlist_iter iter;

	int i;
	for (i = 0; i < cache->length; ++i) {

		dlist_foreach (iter, &cache->content[i]) {
			MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter.cur);
			print_relation(memorelation);

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

/*int  md5(FILE *inFile) {

 unsigned char c[MD5_DIGEST_LENGTH];
 int i;

 MD5_CTX mdContext;
 int bytes;
 unsigned char data[1024];

 if (inFile == NULL) {
 printf("file  can't be opened.\n");
 return 0;
 }

 MD5_Init(&mdContext);
 while ((bytes = fread(data, 1, 1024, inFile)) != 0)
 MD5_Update(&mdContext, data, bytes);
 MD5_Final(c, &mdContext);
 for (i = 0; i < MD5_DIGEST_LENGTH; i++)
 fwrite(c, sizeof(unsigned char), MD5_DIGEST_LENGTH, inFile);
 printf("%02x", c[i]);
 return 0;

 }*/
MemoRelation * set_base_rel_tuples(List *lrelName, int level, double tuples) {

	dlist_iter iter;
	//char *str1, *str2 = NULL;
	MemoRelation *relation = NULL;

	dlist_foreach(iter, &memo_cache_ptr->content) {
		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);
		contains(&relation, (CacheM *) query, lrelName, level);
		if (relation != NULL && tuples != 0) {
			if (relation->rows > tuples)
				relation->rows = tuples;
			relation->tuples = tuples;
			//	print_relation(str1, str2, relation)

		}
	}
	return relation;

}

void set_memo_join_sizes(void) {
	int i = 1;
	dlist_mutable_iter iter;
	if (enable_memo) {
		discard_existing_joins();
		check_NoMemo_queries();
	}

	/*	printf("New memo cache state :\n-----------------------\n");

	 printMemoCache();
	 printf("End\n-----------------------\n");*/
	for (i = 1; i < join_cache_ptr->length; ++i) {

		dlist_foreach_modify (iter, &join_cache_ptr->content[i]) {

			MemoRelation *target = dlist_container(MemoRelation,list_node, iter.cur);
			if (target != NULL) {
				dlist_delete(&(target->list_node));

				delete_memorelation(target);

			}
		}
	}
	pfree(join_cache_ptr->content);
	join_cache_ptr->content = NULL;
	join_cache_ptr->size = 0;

}
void free_memo_cache(void) {
	int i = 1;
	dlist_mutable_iter iter;
	if (memo_query_ptr != NULL) {
		for (i = 1; i < memo_query_ptr->length; ++i) {

			dlist_foreach_modify (iter, &memo_query_ptr->content[i]) {

				MemoRelation *target = dlist_container(MemoRelation,list_node, iter.cur);
				if (target != NULL) {
					dlist_delete(&(target->list_node));

					delete_memorelation(target);

				}
			}
		}
		pfree(memo_query_ptr->content);
		memo_query_ptr->content = NULL;
		memo_query_ptr->size = 0;
	}

}
static bool isAlreadyFetched(RelOptInfo *rel, int level, List *restrictList, bool isIndex) {
	List *b = list_copy(rel->restrictList);
	bool result = false;
	/*	printf("Verifying fetched for : ! \n");
	 printClause(restrictList);
	 fflush(stdout);*/
	if (rel->last_index_type != isIndex)
		return false;
	if (rel->last_level != level)
		return false;
	if (list_length(restrictList) != list_length(rel->last_restrictList)) {

		/*		printf("Not equals lengths\n");*/
		return false;
	}
	result = equalSet(restrictList, b);
	if (b != NULL)
		pfree(b);
	return result;
}
int equals(MemoRelation *rel1, MemoRelation *rel2) {
	MemoInfoData1 resultName;
	MemoInfoData1 resultClause;
	MemoInfoData1 *resultName_ptr;
	MemoInfoData1 *resultClause_ptr;
	resultName_ptr = &resultName;
	resultClause_ptr = &resultClause;
	cmp_lists(resultName_ptr, rel1->relationname, rel2->relationname);
	cmp_lists(resultClause_ptr, rel1->clauses, rel2->clauses);
	return isFullMatched(resultName_ptr) && isFullMatched(resultClause_ptr);

}

void set_plain_rel_sizes_from_memo(PlannerInfo *root, RelOptInfo *rel, Path *path, double *loop_count, bool isIndex) {

	path->restrictList = list_copy(rel->baserestrictinfo);
	if (path->param_info)
		path->restrictList = list_concat_unique(path->restrictList, list_copy(path->param_info->ppi_clauses));
	if (enable_memo) {
		set_path_sizes(root, rel, path, loop_count, isIndex);

	} else {
		if (path->param_info)
			path->rows = path->param_info->ppi_rows;
		else
			path->rows = path->parent->rows;
	}
}
void set_join_sizes_from_memo(PlannerInfo *root, RelOptInfo *rel, JoinPath *pathnode) {
	pathnode->path.restrictList = list_concat_unique(pathnode->path.restrictList,
			list_copy(pathnode->innerjoinpath->restrictList));
	pathnode->path.restrictList = list_concat_unique(pathnode->path.restrictList,
			list_copy(pathnode->outerjoinpath->restrictList));
	pathnode->path.restrictList = list_concat_unique(pathnode->path.restrictList,
			list_copy(pathnode->joinrestrictinfo));
	if (enable_memo) {
		/*	List *l = restictInfoToMemoClauses(pathnode->path.restrictList);
		 char * c = nodeSimToString_(l);
		 printf("Go to probe : \n");
		 printf("%s\n", c);
		 fflush(stdout);*/

		set_path_sizes(root, rel, &pathnode->path, NULL, false);

	} else {
		if (pathnode->path.param_info)
			pathnode->path.rows = pathnode->path.param_info->ppi_rows;
		else
			pathnode->path.rows = pathnode->path.parent->rows;
	}
}

void set_path_sizes(PlannerInfo *root, RelOptInfo *rel, Path *path, double *loop_count, bool isIndex) {
	MemoRelation * memo_rel = NULL;
	MemoInfoData1 result;
	/*	char *str1, *str2;*/
	bool isFetched = false;
	int level = rel->rtekind == RTE_JOIN ? root->query_level : root->query_level + rel->rtekind;
	List *b = list_copy(path->restrictList);
	isFetched = isAlreadyFetched(rel, level, b, isIndex);
	if (b != NULL)
		pfree(b);
	if (isFetched) {

		/*		printf("Relation already fetched ! \n");*/
		memo_rel = rel->last_memorel;

	} else {

		memo_rel = get_Memorelation(&result, rel->rel_name, level, path->restrictList, isIndex);
	}

	if (memo_rel != NULL) {
		/*	printf(" Setting path sizes for! :\n");

		 print_relation(str1, str2, memo_rel);*/
		if (loop_count != NULL)
			*loop_count = *loop_count < memo_rel->loops ? memo_rel->loops : *loop_count;

		path->rows = clamp_row_est(memo_rel->rows / memo_rel->loops);
		path->total_rows = memo_rel->rows;
		path->removed_rows = memo_rel->removed_rows;
		if (!isFetched) {
			rel->last_level = level;
			if (rel->last_restrictList) {
				pfree(rel->last_restrictList);

			}
			rel->last_restrictList = list_copy(path->restrictList);
			rel->last_index_type = isIndex;
			rel->last_memorel = memo_rel;
		}
		printf("injected\n");
		print_relation(memo_rel);
		double re = 0;
		if (path->param_info)
			re = path->param_info->ppi_rows;
		else
			re = path->parent->rows;
		printf("estimated : %lf \n", re);
	} else {
		if (path->param_info)
			path->rows = path->param_info->ppi_rows;
		else
			path->rows = path->parent->rows;
		char *str1;
		printf("not injected : \n ");
		print_list(str1,rel->rel_name );
		printClause(path->restrictList );
		printf("estimated : %lf \n", path->rows);
		fflush(stdout);

	}

}
void set_agg_sizes_from_memo(PlannerInfo *root, Path *path) {
	MemoRelation * memo_rel = NULL;
	MemoInfoData1 result;
	char *str1 = NULL;

	if (enable_memo) {
		memo_rel = get_Memorelation(&result, path->nodename, root->query_level, path->restrictList, false);

	}
	if (memo_rel != NULL) {
		print_list(str1, path->nodename);
		path->rows = clamp_row_est(memo_rel->rows / memo_rel->loops);

	} else {

		path->rows = 0;
	}
}
