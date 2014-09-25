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
#include <openssl/md5.h>

#define print_relation(str1, str2, memorelation)	 \
	str1 = nodeToString((memorelation)->relationname);	\
			str2 = nodeToString((memorelation)->clauses);	\
			printf("%d %s %lf %lf %d %s\n", (memorelation)->level,\
					debackslash(str1, strlen(str1)), (memorelation)->rows,	\
					(memorelation)->tuples,(memorelation)->clauseslength,	\
					debackslash(str2, strlen(str2)));	\
			fflush(stdout);
#define print_list(str1,list) \
		str1 = nodeToString(list);	\
		printf("%s\n",debackslash(str1, strlen(str1)))

#define IsIndex(relation)	((relation)->nodeType == T_IndexOnlyScan  \
		|| (relation)->nodeType == T_IndexScan || (relation)->nodeType == T_BitmapHeapScan)
#define IsUntaged(relation) ((relation)->nodeType == T_Invalid )
#define DEFAULT_MAX_JOIN_SIZE 	5

#define isFullMatched(result) \
		(result->found == 2 ? 1 : 0)
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
static MemoRelation * contains(CacheM* cache, List * relname, int level);
static int equals(MemoRelation *rel1, MemoRelation *rel2);
MemoClause * parse_clause(char * clause);
static char * parse_arg(StringInfo buff, char * arg);

//int md5(FILE *inFile);
void InitCachesForMemo(void) {

	dlist_init(&memo_cache_ptr->content);

	file = AllocateFile("memoTxt.txt", "rb");
	joincache = AllocateFile("joins.txt", "rb");
	if (enable_memo)
		InitMemoCache();
	InitJoinCache();
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
	memo_query_ptr = memo;
	dlist_push_tail(&memo_cache_ptr->content, &memo->list_node);
	/*printMemoCache();
	printf("Memo cache ending\n-----------------------\n");
*/
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
				srelname, &(tmprelation->rows), &(tmprelation->loops), &(tmprelation->removed),
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

static MemoRelation * newMemoRelation(void) {

	MemoRelation * relation;
	relation = (MemoRelation *) palloc(sizeof(MemoRelation));
	relation->nodeType = 0;
	relation->loops = 1;
	relation->removed = 0;

	relation->test = true;
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
void get_baserel_memo_size1(MemoInfoData1 *result, MemoRelation * memorelation, int clauses_length, char *quals,
		bool isIndex) {

	List *lquals2 = NIL;

	/*char *str1;
	 char *str2;*/
	result->loops = 0;
	result->found = 0;
	result->rows = -1;

	lquals2 = build_string_list(quals, M_CLAUSE);

	cmp_lists(result, memorelation->clauses, lquals2);

	if (isMatched(result) && IsIndex(memorelation) == isIndex) {
		//	printf(" Matched base relation! :\n");
		//	print_relation(str1, str2, memorelation);
		result->loops = memorelation->loops;
		result->rows = memorelation->rows;
		return;
	}
}

void get_join_memo_size1(MemoInfoData1 *result, RelOptInfo *joinrel, int level, char *quals, bool isParameterized) {
	/*char *str1;
	 char *str2;
	 */
	dlist_iter iter;

	result->found = 0;
	result->rows = -1;

	dlist_foreach(iter, &memo_cache_ptr->content) {

		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);

		MemoRelation *memorelation = contains((CacheM *) query, joinrel->rel_name, level);
		if (memorelation != NULL && !IsIndex(memorelation)) {
			/*printf("Found join: \n");
			 print_relation(str1, str2, memorelation);
			 result->rows = memorelation->rows;
			 result->found = 2;*/
			return;

		}
	}

}

MemoRelation * get_Memorelation(List *lrelName, int level, List *clauses, bool isIndex) {
	MemoInfoData1 result;
	StringInfoData str;
	int rest;
	dlist_iter iter;
	MemoRelation *relation = NULL;
	/*char *str1;
	 char *str2;*/

	initStringInfo(&str);
	build_selec_string(&str, clauses, &rest);
	//	printf("c : %s\n",str.data);

		dlist_foreach(iter, &memo_cache_ptr->content)
	{
		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);
		relation = contains((CacheM *) query, lrelName, level);
		if (relation != NULL) {

			cmp_lists(&result, relation->clauses, build_string_list(str.data, M_CLAUSE));

			if (isMatched(&result) && IsIndex(relation) == isIndex) {
				//	printf(" Matched base relation! :\n");
				//	print_relation(str1, str2, memorelation);
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
		relation = contains((CacheM *) query, relation_name, level);
		if (relation != NULL && IsIndex(relation)) {
			loops = relation->removed;
		}
	}

	return loops;
}

static List * build_string_list(char * stringList, ListType type) {
	char *save;
	char *s = ",";
	char *tmp = NULL;
	char *newstring;
	void *value = NULL;
	List *result = NIL;

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
	int arg_len = 6;
	initStringInfo(&buff);
	clause_ptr->type = T_MemoClause;
	clause_ptr->opno = 0;
	clause_ptr->args = NIL;

	if (clause[0] == '{') {
		if (sscanf(clause, "{  :opno %u", &clause_ptr->opno) == 1) {
			args = strstr(clause, ":args");
			if (args != NULL) {

				args = &args[arg_len];
				while ((args = parse_arg(&buff, args)) != NULL) {
					arg = (char *) palloc((buff.len + 1) * sizeof(char));
					arg = strcpy(arg, buff.data);
					arg[buff.len] = '\0';
					value = makeString(arg);
					clause_ptr->args = lappend(clause_ptr->args, value);
					resetStringInfo(&buff);
				}

				pfree(buff.data);
			}
		}

	} else if (clause != NULL && strcmp(clause, "<>") == 0) {
		clause_ptr = NULL;

	} else {

		printf("syntax error when parsing clause: %s", clause);
		pfree(&buff);
		pfree(clause_ptr);
		clause_ptr = NULL;
	}

	return clause_ptr;

}

char * parse_arg(StringInfo buff, char * arg) {
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
	MemoInfoData1 resultClause;
	MemoInfoData1 *resultName_ptr;
	MemoInfoData1 *resultClause_ptr;

	List *tmp1 = NIL;
	/*char *str1 = NULL;
	 char *str2 = NULL;*/

	resultName_ptr = &resultName;
	resultClause_ptr = &resultClause;

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
						// In the first pass we need to find a relation who differs from the target
						// relation just from one base relation

						commonrel = contains((CacheM *) query, resultName_ptr->matches, level);
						if (commonrel != NULL) {
							diffrel = contains((CacheM *) query, resultName_ptr->unmatches, level);
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
						//printf("chcking second loop");
						Assert(*relation2 == NULL);
						//In the second pass we have  to match a full join  then
						//the number of base realtion matches are equal to joinlen
						cmp_lists(resultClause_ptr, memorelation->clauses, targetClauses);

						if (resultName_ptr->nbmatches == joinlen && isFullMatched(resultClause_ptr)) {
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
	 printf("\n");
	 fflush(stdout);*/

}
void store_join(List *lrelName, int level, List *clauses) {
	StringInfoData str;
	MemoRelation *relation;
	initStringInfo(&str);

	relation = newMemoRelation();

//appendStringInfoString(&str, " ");

//appendStringInfoString(&str, " ");
	relation->level = level;
	relation->relationname = lrelName;
	relation->clauses = clauses;
	add_relation(join_cache_ptr, relation, list_length(lrelName));
	/*printf("Staring printing join cache\n -------------------------------- \n");
	 printContentRelations((CacheM *) join_cache_ptr);
	 printf("Ending printing join cache\n -------------------------------- \n");*/

}
void export_join(FILE *file) {
	StringInfoData str;
	StringInfoData clauses;

	dlist_iter iter;

	initStringInfo(&str);
	initStringInfo(&clauses);

	if (file != NULL) {
		int i;
		for (i = 0; i < join_cache_ptr->length; ++i) {

			dlist_foreach (iter, &join_cache_ptr->content[i]) {
				MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter.cur);
				buildSimpleStringList(&str, memorelation->relationname);
				nodeSimToString(memorelation->clauses, &clauses);

				fprintf(file, "0\t%d\t%s\t0\t0\t0\t0\t%d\t%s\n", memorelation->level, str.data, clauses.len,
						clauses.data);
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
		relation = contains((CacheM *) query, lrelName, level);
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

	//printf("New memo sizes for join cache state :\n-----------------------\n");

	/*for (i = 1; i < memo_query_ptr->length; ++i) {
	 ListCell *lc;
	 double tuples = 1;

	 dlist_foreach (iter, &memo_query_ptr->content[i]) {
	 MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter.cur);
	 foreach(lc,memorelation->relationname) {
	 List *baserel_name = NIL;
	 MemoRelation * baserel = NULL;
	 baserel_name = lappend(baserel_name, (Value *) lfirst(lc));
	 baserel = (MemoRelation *) contains((CacheM *) memo_query_ptr, baserel_name, memorelation->level);
	 tuples *= baserel->tuples;

	 }

	 memorelation->tuples = tuples;
	 print_relation(str1, str2, memorelation)
	 tuples = 1;
	 }

	 }*/
	if (enable_memo) {
		discard_existing_joins();
		check_NoMemo_queries();
	}
	/*printf("New memo cache state :\n-----------------------\n");

	 printMemoCache();
	 printf("End\n-----------------------\n");*/

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

