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
		|| (relation)->nodeType == T_IndexScan || (relation)->nodeType == T_BitmapIndexScan)
#define IsUntaged(relation) ((relation)->nodeType == T_Invalid )
#define DEFAULT_MAX_JOIN_SIZE 	10

#define isFullMatched(result) \
		((result)->found == FULL_MATCHED)
#define isMatched(result) \
		((result)->found >= FULL_MATCHED)

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
typedef struct SelectivityCache {
	CacheTag type;
	dlist_node list_node;
	int length;
	dlist_head *content;
	int size;

} SelectivityCache;
typedef struct MemoSelectivity {
	List *clauses;
	double selectivity;
} MemoSelectivity;

typedef struct MemoCache {

	dlist_head content;

} MemoCache;

FILE *file;
FILE *joincache;
FILE *selcache;
static struct MemoCache memo_cache;
static struct SelectivityCache sel_cache;
static struct CachedJoins join_cache;
MemoQuery *memo_query_ptr;
CachedJoins *join_cache_ptr = &join_cache;
MemoCache *memo_cache_ptr = &memo_cache;
SelectivityCache *sel_cache_ptr = &sel_cache;
bool initialized = false;

static int read_line(StringInfo str, FILE *file);
static void InitJoinCache(void);
static void InitMemoCache(void);
static void InitSelectivityCache(void);
static List * build_string_list(char * stringList, ListType type);
static void buildSimpleStringList(StringInfo str, List *list);
//static void fill_memo_cache(struct MemoCache *static void compt_lists(MemoInfoData *result, List *str1, const List *str2);

static MemoRelation *newMemoRelation(void);
static void cmp_lists(MemoInfoData1 *result, List *str1, List *str2);
static MemoQuery * find_seeder_relations(MemoRelation **relation1, MemoRelation **relation2,
		MemoRelation **commonrelation, List * targetName, List * targetClauses, int level, int rellen1);
static void set_estimated_join_rows(MemoRelation *relation1, MemoRelation *relation2, MemoRelation * commonrelation,
		MemoRelation *target);
static void check_NoMemo_queries(void);
static void add_node(void * memo, MemoRelation * node, int rellen);
static void readAndFillFromFile(FILE *file, void *sink);
static void printContentRelations(CacheM *cache);
static void discard_existing_joins(void);
static void contains(MemoInfoData1 *result, MemoRelation ** relation, CacheM* cache, List * relname, List *clauses,
		int level, int isParam);
static int equals(MemoRelation *rel1, MemoRelation *rel2);
MemoClause * parse_clause(char * clause);
static char * parse_args(StringInfo buff, char * arg);
static void set_path_sizes(PlannerInfo *root, RelOptInfo *rel, Path *path, double *loop_count, bool isParam);

static void print_relation(MemoRelation *memorelation) {
	char *str1 = nodeSimToString_((memorelation)->relationname);
	char *str2 = nodeSimToString_((memorelation)->clauses);
	printf("%d %s %lf %d %d %s\n", (memorelation)->level, str1, (memorelation)->rows, (memorelation)->loops,
			(memorelation)->clauseslength, str2);
	pfree(str1);
	pfree(str2);
	fflush(stdout);

}
static void remove_par(char * stringList) {

	int newlen = strlen(stringList) - 1;
	if (stringList[newlen] == ')') {
		stringList[newlen] = '\0';
	}

}
void add_relation(MemoRelation * relation, int rellen) {
	//print_relation(relation);
	add_node(memo_query_ptr, relation, rellen);

}

/*Build a List of MemoClause from a RestrictInfo List. Every MemoClause is linked to its RestrictInfo Parent*/
List * restictInfoToMemoClauses(List *clauses) {
	List *lquals = NIL;
	ListCell *lc;

	char * str1 = NULL;
	if (clauses == NIL)
		return NIL;
	foreach(lc,clauses) {
		char *sclause = NULL;
		switch (nodeTag(lfirst(lc))) {

			case T_RestrictInfo: {
				MemoClause *mclause;
				str1 = nodeSimToString_(lfirst(lc));
				sclause = str1;
				remove_par(sclause);
				if (sclause[0] == '(')
					sclause = sclause + 1;

				mclause = parse_clause(sclause);
				if (mclause != NULL) {

					mclause->parent = (RestrictInfo *) lfirst(lc);
					lquals = lappend(lquals, mclause);

				}
				break;
			}
			default: {
				printf("Unexpected Node when building MemoClauses : %d", nodeTag(lfirst(lc)));
				break;
			}

		}

	}
	/*printf("Converted quals : \n");
	 printMemo(lquals);*/
	if (str1) {
		pfree(str1);
	}
	return lquals;

}
MemoRelation* create_memo_realation(int level, bool isParam, List *relname, double rows, int loops, List *clauses) {

	MemoRelation *relation = newMemoRelation();
	relation->level = level;
	relation->isParameterized = isParam;
	relation->relationname = list_copy(relname);
	relation->rows = rows;
	relation->loops = loops;
	relation->clauseslength = 1;
	relation->clauses = restictInfoToMemoClauses(clauses);
	return relation;
}
//int md5(FILE *inFile);
void InitCachesForMemo(void) {

	joincache = AllocateFile("joins.txt", "rb");
	if (enable_memo && !initialized) {
		dlist_init(&memo_cache_ptr->content);

		file = AllocateFile("memoTxt.txt", "rb");
		//selcache = AllocateFile("c_memoTxt.txt", "rb");
		InitMemoCache();
		initialized = true;

	}
	printf("calling init cache");
	if (join_cache_ptr->type != M_JoinCache)
		InitJoinCache();
}
static void InitSelectivityCache(void) {
	char *cquals = NULL;
	StringInfoData str;
	int clauseLen;
	MemoSelectivity *msel = (MemoSelectivity *) palloc(sizeof(MemoSelectivity));
	int ARG_NUM = 2;

	if (selcache != NULL) {
		sel_cache_ptr->type = M_SelCache;
		sel_cache_ptr->length = DEFAULT_MAX_JOIN_SIZE;
		sel_cache_ptr->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
		while (read_line(&str, file) != EOF) {

			char *tab = strchr(str.data, '\t');
			int len = tab - str.data;
			cquals = (char *) palloc(sizeof(char) * (len + 1));
			if (sscanf(str.data, "%[^\t]\t%lf", cquals, &msel->selectivity) == ARG_NUM) {

				msel->clauses = build_string_list(cquals, M_CLAUSE);
				clauseLen = list_length(msel->clauses);

				add_node(sel_cache_ptr, msel, clauseLen);

			}
			pfree(cquals);
			resetStringInfo(&str);

		}

	}
}
void InitMemoCache(void) {

	MemoQuery *memo;
	int i = 0;
	memo = (MemoQuery *) palloc0(sizeof(MemoQuery));
	memo->type = M_MemoQuery;
	memo->length = DEFAULT_MAX_JOIN_SIZE;
	memo->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
	MemSetAligned(memo->content, 0, (DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
	for (i = 0; i < DEFAULT_MAX_JOIN_SIZE; i++) {
		dlist_init(&memo->content[i]);
	}

	printf("Initializing Memo cache\n------------------------\n");
	if (file != NULL)
		readAndFillFromFile(file, memo);
	memo_query_ptr = memo;
	dlist_push_tail(&memo_cache_ptr->content, &memo->list_node);
	printMemoCache();
	printf("Memo cache ending\n-----------------------\n");

}
void InitJoinCache(void) {
	int i = 0;
	printf("Initializing join cache\n------------------------\n");
	join_cache_ptr->type = M_JoinCache;
	join_cache_ptr->length = DEFAULT_MAX_JOIN_SIZE;
	join_cache_ptr->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
	MemSetAligned(join_cache_ptr->content, 0, (DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));

	for (i = 0; i < DEFAULT_MAX_JOIN_SIZE; i++) {
		dlist_init(&join_cache_ptr->content[i]);
	}
	if (joincache != NULL && enable_memo)
		readAndFillFromFile(joincache, join_cache_ptr);
	/*
	 printContentRelations((CacheM *) join_cache_ptr);

	 printf("Join cache ending\n-----------------------\n");*/

	//printf("New join cache state:\n-----------------------\n");
	//printContentRelations((CacheM *) join_cache_ptr);
	printf("End\n-----------------------\n");
	if (enable_memo && enable_memo_propagation) {
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

	int ARG_NUM = 8;
	char srelname[DEFAULT_SIZE];
	char *cquals = NULL;
	StringInfoData str;
	char *dequals = NULL;

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
		if (sscanf(str.data, "%d\t%d\t%d\t%s\t%*d\t%lf\t%d\t%lf\t%d", &(tmprelation->nodeType),
				&(tmprelation->isParameterized), &(tmprelation->level), srelname, &(tmprelation->rows),
				&(tmprelation->loops), &(tmprelation->removed_rows), &(tmprelation->clauseslength)) == ARG_NUM) {

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

			if (sscanf(str.data, "%*d\t%*d\t%*d\t%*s\t%*d\t%*d\t%*d\t%*d\t%*d\t%[^\n]", cquals) == 1) {

				tmprelation->relationname = build_string_list(srelname, M_NAME);
				dequals = debackslash(cquals, tmprelation->clauseslength);
				tmprelation->clauses = build_string_list(dequals, M_CLAUSE);
				tmprelation->tuples = 0;

			} else {
				printf("Error reading memo: Bad argument number");
				fflush(stdout);
			}
			rellen = list_length(tmprelation->relationname);

			add_node(sink, tmprelation, rellen);
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
	relation->isParameterized = false;
	return relation;
}
void add_node(void * sink, MemoRelation * node, int rellen) {
	if (IsACache(sink,Invalid)) {
		return;

	} else {
		CacheM * cache = (CacheM *) sink;

		if (rellen > cache->length) {
			int actLength;
			if (IsACache(sink,JoinCache)) {

				join_cache_ptr->content = (dlist_head *) repalloc(join_cache_ptr->content,
						(rellen + 1) * sizeof(dlist_head));
				for (actLength = cache->length; actLength < rellen; actLength++) {
					dlist_init(&join_cache_ptr->content[actLength]);
				}
				cache->length = rellen;
			}

			if (IsACache(sink,MemoQuery)) {
				cache->content = (dlist_head *) repalloc(((MemoQuery *) sink)->content, (rellen) * sizeof(dlist_head));
				cache->length = rellen;
				for (actLength = cache->length; actLength < rellen; actLength++) {
					dlist_init(&cache->content[actLength]);
				}
			}
			/*	if (IsACache(sink,SelCache)) {
			 cache->content = (dlist_head *) repalloc(((SelectivityCache *) sink)->content,
			 (rellen) * sizeof(dlist_head));
			 cache->length = rellen;
			 }*/
			MemSetAligned(&cache->content[rellen - 1], 0, sizeof(dlist_head));
		}

		dlist_push_tail(&cache->content[rellen - 1], &node->list_node);

		cache->size = cache->size + 1;
	}

}
void get_relation_size(MemoInfoData1 *result, PlannerInfo *root, RelOptInfo *rel, List *quals, bool isParam,
		SpecialJoinInfo * sjinfo) {
	MemoRelation * memo_rel = NULL;
	double mrows = 1;
	List *final_clauses = NIL;
	bool isNew = false;
	int level = rel->rtekind > RTE_SUBQUERY ? root->query_level : root->query_level + rel->rtekind;

	result->loops = 1;
	result->found = -1;
	result->rows = -1;

	memo_rel = get_Memorelation(result, list_copy(rel->rel_name), level, quals, isParam);
	//printf("Result %d\n", result->found);

	//print_relation(memo_rel);

	if (memo_rel) {
		result->loops = memo_rel->loops;
		result->rows = memo_rel->rows;

		mrows = result->rows / result->loops;
	}

	switch (result->found) {

		case FULL_MATCHED:
			//printf(" FULL Matched  relation! :\n");

			result->rows = mrows;
			final_clauses = NIL;
			break;

		case MATCHED_LEFT: {
			ListCell * lc;
			if (!sjinfo || sjinfo->jointype == JOIN_INNER) {
				//printf(" MATCHED_LEFT  relation! :\n");

				/*				printMemo(result.unmatches);*/
				result->rows = mrows;
				foreach(lc,result->unmatches) {

					final_clauses = lappend(final_clauses, ((MemoClause *) lfirst(lc))->parent);

				}
				isNew = true;
			}
			break;
		}
		case MATCHED_RIGHT: {
			if (!sjinfo) {
				//	printf(" MATCHED_RIGHT  relation! :\n");

				final_clauses = NIL;
				result->rows = rel->tuples * clauselist_selectivity(root, quals, 0, JOIN_INNER, NULL);
				result->rows = result->rows >= mrows ? result->rows : mrows;
				isNew = true;
			}
			break;
		}
		default: {
			//printf(" UNMATCHED  relation! :\n");
			if (!sjinfo) {
				final_clauses = quals;
				//printMemo(final_clauses);
				result->rows = rel->tuples;
				//printf("result->rows : %lf\n", result->rows);
				isNew = true;

			}
			break;
		}
	}
	if (!sjinfo || result->found == FULL_MATCHED
			|| (sjinfo && result->found == MATCHED_LEFT && sjinfo->jointype == JOIN_INNER)) {
		result->rows = result->rows * clauselist_selectivity(root, final_clauses, 0, JOIN_INNER, NULL);
		result->rows = clamp_row_est(result->rows);
		if (isNew) {
			MemoRelation *newRelation = create_memo_realation(level, isParam, rel->rel_name, result->rows,
					result->loops, final_clauses);
			add_node(memo_query_ptr, newRelation, list_length(rel->rel_name));
		}
	} else {
		result->rows = -1;
		result->loops = 1;

	}

}

MemoRelation * get_Memorelation(MemoInfoData1 *result, List *lrelName, int level, List *clauses, int isIndex) {
//	char *str3, *str4;
	MemoRelation *relation;
	/*char *str1;
	 char *str2;*/

	relation = NULL;

	contains(result, &relation, (CacheM *) memo_query_ptr, lrelName, clauses, level, isIndex);

	return relation;

}
int get_memo_removed(List *relation_name, List *clauses, int level) {

	dlist_iter iter;
	MemoRelation *relation = NULL;
	int loops = 0;

	dlist_foreach(iter, &memo_cache_ptr->content) {
		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);
		contains(NULL, &relation, (CacheM *) query, relation_name, NIL, level, 2);
		if (relation != NULL && IsIndex(relation)) {
			loops = relation->removed_rows;
		}
	}

	return loops;
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
		if (stringList[0] == '(')
			stringList = stringList + 1;
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
	clause_ptr->parent = NULL;

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

	} else if (strcmp(clause, "<>") == 0) {
		return NULL;

	} else {

		printf("syntax error when parsing clause: %s \n", clause);
		pfree(buff.data);
		pfree(clause_ptr);
		return NULL;
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
/*compare list a against list b. Fille the MemoInfoData1 with :
 * unmatched: all the items in b and not in a.
 * matched: all the items in b and not in a.
 * found : 2 if very element is matched. 1 if at least one element was matched and
 *  list_length(a) > list_length(b)
 * */
void cmp_lists(MemoInfoData1 * result, List *lleft, List *lright) {
	List *a = list_copy(lleft);
	List *b = list_copy(lright);

//	printMemo(lleft);
	result->unmatches = NIL;
	result->matches = NIL;
	const ListCell *item_b;
	if (lleft) {
		result->debug2 = nodeSimToString_(lleft);
	}
	foreach(item_b, b) {
		if (!list_member_remove(a, lfirst(item_b))) {

			//printf("member not found\n");
			//printMemo(lfirst(item_b));
			result->unmatches = lappend(result->unmatches, lfirst(item_b));
			result->debug = nodeSimToString_(result->unmatches);
			//	printMemo(result->unmatches);
		} else {
			result->matches = lappend(result->matches, lfirst(item_b));
			result->debug1 = nodeSimToString_(result->matches);

		}

	}
	result->found = UNMATCHED;

	if ((list_length(lright) == list_length(lleft)) && (list_length(lleft) == list_length(result->matches))
			&& (result->unmatches == NIL)) {
		result->found = FULL_MATCHED;
		return;

	}

	if (list_length(lleft) <= list_length(lright)) {

		if (result->unmatches != NIL)
			result->found = UNMATCHED;
		else
			result->found = MATCHED_LEFT;

	}
	if (list_length(lleft) >= list_length(lright)) {
		{
			if (list_length(result->matches) != list_length(lright))
				result->found = UNMATCHED;
			else if (result->unmatches != NIL)
				result->found = MATCHED_RIGHT;
		}

	}
}
bool equalSet(const List *a, const List *b) {
	ListCell *item_a;
	if (list_length(a) != list_length(b))
		return false;
	foreach(item_a, a) {
		if (!list_member_remove(b, lfirst(item_a))) {
			//	printf("member not found\n");

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
				/*		printf("Checking :\n**********************\n");
				 print_relation(target);
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
					//printf("Founds rows :\n%lf\n**********************\n", target->rows);

					if (target->rows >= 1) {

						add_node(query, target, rellen);

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
//	char *str1, *str2 = NULL;

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
				// ATTENTION: the candidate relation must be at the same query level!
				if (memorelation->level == level) {
					cmp_lists(resultName_ptr, memorelation->relationname, targetName);

					//printf("matches %d \n", resultName_ptr->nbmatches);
					// Check if we had the expected number of matches in join

					//If *relation is null we are in out first loop

					if (*relation1 == NULL) {
						//printf("Entering in 1 loop with %d matches \n", resultName_ptr->nbmatches);
						// In the firstfind_seeder_relations pass we need to find a relation who differs from the target
						// relation just from one base relation

						contains(NULL, &commonrel, (CacheM *) query, resultName_ptr->matches, NIL, level, 2);
						if (commonrel != NULL) {
							/*bool diff_realIndex = false;
							 bool com_realIndex = false;*/
							contains(NULL, &diffrel, (CacheM *) query, resultName_ptr->unmatches, NIL, level, 2);
							clauses = list_union(memorelation->clauses, targetClauses);
							/*

							 if (diffrel != NULL)
							 diff_realIndex = IsIndex(diffrel) && diffrel->loops > 1;
							 com_realIndex = IsIndex(commonrel) && commonrel->loops > 1;
							 */

							/*printf("Common relation is:\n");

							 print_relation(commonrel);*/
							if ((list_length(resultName_ptr->matches) == (joinlen - 1)) && diffrel != NULL) {
								//Mark the first relation and build the resulted expected target join
								//relation for matching
								*relation1 = memorelation;
								*commonrelation = commonrel;

								tmp1 = list_union(targetName, memorelation->relationname);
								/*		printf("Looking for relation:\n");
								 printMemo(tmp1);
								 printClause(clauses);*/
								return find_seeder_relations(relation1, relation2, commonrelation, tmp1, clauses, level,
										list_length(tmp1));

							}
						}

					} else {
						List *b = list_copy(targetClauses);
						/*						printf("chcking second loop");*/
						Assert(*relation2 == NULL);
						//In the second pass we have  to match a full join  then
						//the number of base realtion matches are equal to joinlen
						//print_relation(memorelation);
						if (list_length(resultName_ptr->matches) == joinlen && equalSet(memorelation->clauses, b)) {
							pfree(b);
							*relation2 = memorelation;

							/*		printf("Found candidates relations : \n");
							 print_relation(*relation1);
							 print_relation(*relation2);
							 printf("-------------------------------\n");*/

							return query;
						}

					}

				}

			}
		}

	}
	return NULL;
}

void contains(MemoInfoData1 *result, MemoRelation ** relation, CacheM* cache, List * relname, List *clauses, int level,
		int isParam) {
	dlist_iter iter;
	List *lquals = NIL;

	//char *str1, *str2 = NULL;
	int rellen = list_length(relname);
	MemoRelation *memorelation = NULL;
	MemoRelation *rightmatched = NULL;
	MemoRelation *leftmatched = NULL;

	bool equal = false;
	if (relname == NIL)
		return;

	dlist_foreach(iter, &cache->content[rellen-1]) {

		memorelation = dlist_container(MemoRelation,list_node, iter.cur);
		equal = equalSet(list_copy(memorelation->relationname), list_copy(relname));
		if (equal && memorelation->level == level) {

			if ((isParam < 2 && memorelation->isParameterized == isParam) || isParam == 2) {
				//	print_relation(memorelation);
				if (result != NULL) {

					lquals = restictInfoToMemoClauses(clauses);

					cmp_lists(result, memorelation->clauses, lquals);
					/*if (list_length(relname) == 4) {

					 printMemo(relname);

					 printMemo(relname);
					 }*/
					if (isFullMatched(result)) {
						//	printf("IS FULL MATCHED\n!");
						*relation = &(*memorelation);
						return;
					}
					if (result->found == MATCHED_RIGHT) {
						rightmatched = memorelation;

					}
					if (result->found == MATCHED_LEFT) {
						leftmatched = memorelation;

					}
				} else {
					*relation = &(*memorelation);
					return;
				}

			}

		}

	}
	*relation = leftmatched == NULL ? &(*rightmatched) : NULL;
}
static void set_estimated_join_rows(MemoRelation *relation1, MemoRelation *relation2, MemoRelation * commonrelation,
		MemoRelation *target) {
	double rows;

	rows = (clamp_row_est(relation2->rows / relation2->loops) / clamp_row_est(relation1->rows / relation1->loops))
			* clamp_row_est(commonrelation->rows / commonrelation->loops);
	target->rows = rows <= 0 ? 1 : rows;
	target->loops = 1;

	/*printf("Injected new estimated size  %lf for : \n", rows);
	 print(target->relationname);
	 printf("\n");
	 fflush(stdout);*/

}
void store_join(List *lrelName, int level, List *clauses, double rows, bool isParam) {
	StringInfoData str;
	MemoRelation *relation;
	initStringInfo(&str);

	relation = newMemoRelation();
	if (list_length(lrelName) > 0) {
		Assert(lrelName!=NIL);

//appendStringInfoString(&str, " ");

//appendStringInfoString(&str, " ");
		relation->rows = rows;
		relation->level = level;
		relation->relationname = list_copy(lrelName);
		relation->clauses = list_copy(clauses);
		relation->isParameterized = isParam;
		add_node(join_cache_ptr, relation, list_length(lrelName));
	}
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

				fprintf(file, "0\t%d\t%d\t%s\t%.0f\t0\t1\t0\t%d\t%s\n", memorelation->isParameterized,
						memorelation->level, str.data, memorelation->rows, clauses.len, clauses.data);

				resetStringInfo(&str);
				resetStringInfo(&clauses);

			}

		}

		fclose(file);

	}

}
void printMemoCache(void) {

	printContentRelations((CacheM*) memo_query_ptr);
	printf("Cache size : %d realtions.\n", memo_query_ptr->size);
	fflush(stdout);

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
		contains(NULL, &relation, (CacheM *) query, lrelName, NIL, level, 2);
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

	printf("New memo cache state :\n-----------------------\n");

	printMemoCache();
	printf("End\n-----------------------\n");
	if (enable_memo) {
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
		memo_query_ptr = NULL;
	}

	printf("Freeing join \n");
	fflush(stdout);

	join_cache_ptr->type = M_Invalid;

}
static bool isAlreadyFetched(RelOptInfo *rel, int level, List *restrictList, bool isIndex) {
	List *b = list_copy(rel->last_restrictList);
	bool result = false;
	/*	printf("Verifying fetched for : ! \n");
	 printClause(restrictList);
	 fflush(stdout);*/
	if (rel->last_index_type != isIndex)
		return false;
	if (rel->last_level != level)
		return false;
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

	if (path->param_info) {
		path->restrictList = list_concat_unique(path->restrictList, list_copy(path->param_info->ppi_clauses));
		path->rows = path->param_info->ppi_rows;
		if (enable_memo && path->param_info->paramloops) {

			*loop_count = path->param_info->paramloops;
		}

		path->isParameterized = true;
	} else {
		if (enable_memo) {
			set_path_sizes(root, rel, path, loop_count, false);
		} else {

			path->rows = path->parent->rows;
		}
		path->isParameterized = false;
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

		set_path_sizes(root, rel, &pathnode->path, NULL, pathnode->path.param_info != NULL);

	} else {
		if (pathnode->path.param_info) {
			pathnode->path.rows = pathnode->path.param_info->ppi_rows;

		}

		else {
			pathnode->path.rows = pathnode->path.parent->rows;

		}
		pathnode->path.isParameterized = pathnode->path.param_info != NULL;
	}
	if (pathnode->path.rows == 0) {

		printf("injected 0 rows for : \n");
		printMemo(rel->rel_name);
	}
}

void set_path_sizes(PlannerInfo *root, RelOptInfo *rel, Path *path, double *loop_count, bool isParam) {
	MemoRelation * memo_rel = NULL;
	MemoInfoData1 result;
//char *str1;
	/*	char *str1, *str2;*/
	bool isFetched = false;
	int level = rel->rtekind == RTE_JOIN ? root->query_level : root->query_level + rel->rtekind;
	List *b = list_copy(path->restrictList);
	isFetched = isAlreadyFetched(rel, level, b, isParam);
	if (b != NULL)
		pfree(b);
	if (isFetched) {

		printf("Relation already fetched ! \n");
		memo_rel = rel->last_memorel;

	} else {


		memo_rel = get_Memorelation(&result, rel->rel_name, level, path->restrictList, isParam);
	}

	if (memo_rel != NULL) {

		printMemo(rel->rel_name);
		if (loop_count != NULL && isParam) {

			*loop_count = *loop_count < memo_rel->loops ? memo_rel->loops : *loop_count;
		}

		path->total_rows = memo_rel->rows;
		path->removed_rows = memo_rel->removed_rows;
		if (!isFetched) {
			rel->last_level = level;
			if (rel->last_restrictList) {
				pfree(rel->last_restrictList);

			}

			rel->last_restrictList = list_copy(path->restrictList);
			rel->last_index_type = isParam;
			rel->last_memorel = memo_rel;
		}
		path->rows = clamp_row_est(memo_rel->rows / memo_rel->loops);

	} else {
		if (path->param_info) {
			path->rows = path->param_info->ppi_rows;
		} else {
			path->rows = path->parent->rows;

		}
	}
}

/*	printf("not injected : \n ");
 print_list(str1, rel->rel_name);
 printClause(path->restrictList);
 printf("estimated : %lf \n", path->rows);
 fflush(stdout);*/

void set_agg_sizes_from_memo(PlannerInfo *root, Path *path) {
	MemoRelation * memo_rel = NULL;
	MemoInfoData1 result;
//char *str1 = NULL;

	if (enable_memo) {
		memo_rel = get_Memorelation(&result, path->nodename, root->query_level, path->restrictList, false);

	}
	if (memo_rel != NULL) {
//print_list(str1, path->nodename);
		path->rows = clamp_row_est(memo_rel->rows / memo_rel->loops);

	} else {

		path->rows = 0;
	}
}
