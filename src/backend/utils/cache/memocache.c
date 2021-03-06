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
#include "nodes/print.h"
#include "nodes/readfuncs.h"
#include "optimizer/cost.h"
#include "nodes/relation.h"
#include <time.h>
#include <openssl/md5.h>
#include "miscadmin.h"

#define IsIndex(relation)	((relation)->nodeType == T_IndexOnlyScan  \
		|| (relation)->nodeType == T_IndexScan || (relation)->nodeType == T_BitmapIndexScan)
#define IsUntaged(relation) ((relation)->nodeType == T_Invalid )
#define IsBoolLoop(node)	((node)->opno == AND_EXPR \
		|| (node)->opno == OR_EXPR || (node)->opno == NOT_EXPR)
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
	int max_level;
	char *id;
	dlist_node list_node;
} MemoQuery;

typedef struct CachedJoins {
	CacheTag type;
	int length;
	dlist_head *content;
	int size;
	int max_level;

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
	Selectivity selectivity;
} MemoSelectivity;

typedef struct MemoCache {

	dlist_head content;

} MemoCache;
typedef struct NameDataIdx {

	Value *name;
	List* indexes;

} NameDataIdx;
typedef struct RteReferencesIndex {

	List *NameData;
} RteReferencesIndex;

FILE *file;
FILE *joincache;
FILE *selcache;
static struct RteReferences rte_ref;
static struct RteReferencesIndex rte_ref_idx;

RteReferences *rte_ref_ptr = &rte_ref;
RteReferencesIndex *rte_ref_idx_ptr = &rte_ref_idx;

static struct MemoCache memo_cache;
static struct SelectivityCache sel_cache;
static struct CachedJoins join_cache;
static struct CachedJoins join_cache_propagation;

MemoQuery *memo_query_ptr;
CachedJoins *join_cache_ptr = &join_cache;
CachedJoins *join_cache_prop_ptr = &join_cache_propagation;

MemoCache *memo_cache_ptr = &memo_cache;
SelectivityCache *sel_cache_ptr = &sel_cache;
bool initialized = false;

static int read_line(StringInfo str, FILE *file);

static void InitMemoCache(void);
static void InitSelectivityCache(void);
static List * build_string_list(char * stringList, ListType type);
//static void fill_memo_cache(struct MemoCache *static void compt_lists(MemoInfoData *result, List *str1, const List *str2);
static void recost_join_children(PlannerInfo *root, JoinPath * jpath);
static void recost_path_recurse(PlannerInfo *root, Path * path);
static MemoRelation *newMemoRelation(void);
static void cmp_lists(MemoInfoData1 *result, List *str1, List *str2);
static MemoQuery * find_seeder_relations(MemoRelation **relation1, MemoRelation **relation2,
		MemoRelation **commonrelation, List * targetName, List * targetClauses, int level, int rellen1);
static void set_estimated_join_rows(MemoRelation *relation1, MemoRelation *relation2, MemoRelation * commonrelation,
		MemoRelation *target);
static void add_node(void * memo, MemoRelation * node, int rellen);
static void readAndFillFromFile(FILE *file, void *sink);
static void printContentRelations(CacheM *cache);
static void discard_existing_joins(void);
static void contains(MemoInfoData1 *result, MemoRelation ** relation, CacheM* cache, List * relname, List *clauses,
		int level, int isParam);
static int equals(MemoRelation *rel1, MemoRelation *rel2);
MemoClause * parse_clause(char * clause);
static char * parse_args(StringInfo buff, char * arg);
static List * parse_clause_list(char * lclause, char **next);
static MemoClause * parse_clause1(char * clause, char **next);
static void * parse_item_list(char * element, char **next);
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
	char *next = "";
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

				mclause = parse_item_list(sclause, &next);
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
	relation->clauseslength = list_length(clauses);
	relation->clauses = restictInfoToMemoClauses(clauses);
	return relation;
}
void * get_cur_rte_reference(void) {

	return rte_ref_ptr;
}
void * fetch_unique_rte_reference(void) {
	int i = 0;
	RteReferences *newrte = (RteReferences *) palloc0(sizeof(RteReferences));
	newrte->rte_table = (Value **) palloc0((rte_ref_ptr->size) * sizeof(Value *));
	newrte->size = rte_ref_ptr->size;
	for (i = 0; i < newrte->size; i++) {
		if (rte_ref_ptr->rte_table[i] != NULL)
			newrte->rte_table[i] = (Value *) copyObject(rte_ref_ptr->rte_table[i]);
		else
			newrte->rte_table[i] = NULL;
	}
	return newrte;
}
void push_reference(Index index, Value * name) {
//	NameDataIdx namedata;
//	ListCell *lc;
//	bool found = false;
	if (index >= rte_ref_ptr->size) {

		Value ** newtable = (Value **) palloc0((index + 1) * sizeof(Value *));
		memcpy(newtable, rte_ref_ptr->rte_table, (rte_ref_ptr->size) * sizeof(Value*));
		rte_ref_ptr->rte_table = newtable;
		rte_ref_ptr->size = index + 1;
	}
	rte_ref_ptr->rte_table[index] = name;
	/*	foreach(lc, rte_ref_idx_ptr->NameData) {
	 NameDataIdx *nd = (NameDataIdx *) lfirst(lc);
	 if (equal(name, nd->name)) {
	 nd->indexes = lappend_int(nd->indexes, index);
	 found = true;
	 break;
	 }

	 }
	 if (!found) {
	 namedata.name = name;
	 namedata.indexes = lappend_int(namedata.indexes, index);
	 printMemo(namedata.name);
	 printMemo(namedata.indexes);
	 rte_ref_idx_ptr->NameData = lappend(rte_ref_idx_ptr->NameData, &namedata);

	 }

	 foreach(lc, rte_ref_idx_ptr->NameData) {
	 NameDataIdx *nd = (NameDataIdx *) lfirst(lc);
	 printMemo(nd->name);
	 printMemo(nd->indexes);
	 printf("..................");

	 }*/
	printf("%u | ", index);
	printMemo(rte_ref_ptr->rte_table[index]);

}
//int md5(FILE *inFile);
void InitCachesForMemo(void) {
	int i = 0;

	rte_ref_ptr->rte_table = (Value **) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(Value *));
	rte_ref_ptr->size = DEFAULT_MAX_JOIN_SIZE;
	joincache = AllocateFile("joins.txt", "rb");
	if (join_cache_ptr->type != M_JoinCache) {

		//printf("elapsed time:  %lf ms \n", (1000 * ((float) t) / CLOCKS_PER_SEC));
		printf("Initializing join cache\n------------------------\n");
		join_cache_ptr->type = M_JoinCache;
		join_cache_ptr->length = DEFAULT_MAX_JOIN_SIZE;
		join_cache_ptr->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
		join_cache_ptr->max_level = 0;
		MemSetAligned(join_cache_ptr->content, 0, (DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));

		for (i = 0; i < DEFAULT_MAX_JOIN_SIZE; i++) {
			dlist_init(&join_cache_ptr->content[i]);
		}
	}
	if (enable_memo && !initialized) {
		dlist_init(&memo_cache_ptr->content);

		file = AllocateFile("memoTxt.txt", "rb");
		//selcache = AllocateFile("c_memoTxt.txt", "rb");
		InitMemoCache();
		initialized = true;

	}

}

void InitMemoCache(void) {

	MemoQuery *memo;
	int i = 0;
	rte_ref_idx_ptr->NameData = NIL;
	memo = (MemoQuery *) palloc0(sizeof(MemoQuery));
	memo->type = M_MemoQuery;
	memo->length = DEFAULT_MAX_JOIN_SIZE;
	memo->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
	MemSetAligned(memo->content, 0, (DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
	memo->max_level = 0;

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
	printf("calling init cache");

	if (joincache != NULL && enable_memo_propagation)
		readAndFillFromFile(joincache, join_cache_ptr);
	printContentRelations((CacheM *) join_cache_ptr);
	if (enable_memo && !enable_memo_propagation) {

		join_cache_prop_ptr->type = M_JoinCache;
		join_cache_prop_ptr->length = DEFAULT_MAX_JOIN_SIZE;
		join_cache_prop_ptr->content = (dlist_head *) palloc0((DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));
		MemSetAligned(join_cache_prop_ptr->content, 0, (DEFAULT_MAX_JOIN_SIZE) * sizeof(dlist_head));

		for (i = 0; i < DEFAULT_MAX_JOIN_SIZE; i++) {
			dlist_init(&join_cache_prop_ptr->content[i]);
		}

	}
	printf("Join cache ending\n-----------------------\n");

	//printf("New join cache state:\n-----------------------\n");
	//printContentRelations((CacheM *) join_cache_ptr);
	printf("End\n-----------------------\n");
	if (enable_memo && enable_memo_propagation) {
		set_memo_join_sizes();
		printf("New memo cache state :\n-----------------------\n");

		printMemoCache();
		printf("End\n-----------------------\n");
	}

}

void update_cached_joins(List *joinname, int level, double rows) {

	dlist_mutable_iter iter;
	int index = list_length(joinname) - 1;
	//printContentRelations((CacheM *) join_cache_ptr);

	dlist_foreach_modify (iter, &join_cache_ptr->content[index]) {

		MemoRelation *cachedjoin = dlist_container(MemoRelation,list_node, iter.cur);
		if (equalSet(list_copy(cachedjoin->relationname), list_copy(joinname)) && cachedjoin->level == level) {

			dlist_delete(&(cachedjoin->list_node));
			cachedjoin->rows = rows;
			add_node(memo_query_ptr, cachedjoin, list_length(joinname));

		}

	}

}
void discard_existing_joins(void) {

	dlist_mutable_iter iter;
	dlist_mutable_iter iter2;

	int i;

	for (i = 1; i < join_cache_ptr->length; ++i) {

		dlist_foreach_modify (iter, &join_cache_ptr->content[i]) {
			MemoRelation *cachedjoin = dlist_container(MemoRelation,list_node, iter.cur);
			dlist_foreach_modify (iter2, &memo_query_ptr->content[i]) {
				MemoRelation *existingjoin = dlist_container(MemoRelation,list_node, iter2.cur);

				if (equals(cachedjoin, existingjoin) && cachedjoin->level == existingjoin->level
						&& existingjoin->nodeType != 999) {

					MemoInfoData1 resultClause;

					cmp_lists(&resultClause, cachedjoin->clauses, existingjoin->clauses);
					dlist_delete(&(cachedjoin->list_node));

					if (!isFullMatched(&resultClause)) {
						cachedjoin->rows = existingjoin->rows;
						add_relation(cachedjoin, list_length(cachedjoin->relationname));
					}
					break;
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
static List * parse_restricList(char *restricList) {
	char *next = "";
	if (restricList == NULL || !strcmp(restricList, "<>")) {
		return NIL;

	}
	return parse_clause_list(restricList, &next);

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
			if (IsACache(sink,MemoQuery) && tmprelation->nodeType == 0) {
				tmprelation->nodeType = 999;

			}

			if (sscanf(str.data, "%*d\t%*d\t%*d\t%*s\t%*d\t%*d\t%*d\t%*d\t%*d\t%[^\n]", cquals) == 1) {

				tmprelation->relationname = build_string_list(srelname, M_NAME);
				dequals = debackslash(cquals, tmprelation->clauseslength);
				tmprelation->clauses = parse_restricList(dequals);
				//tmprelation->clauses = build_string_list(dequals, M_CLAUSE);
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

		if (cache->max_level < rellen)
			cache->max_level = rellen;
		dlist_push_tail(&cache->content[rellen - 1], &node->list_node);

		cache->size = cache->size + 1;
	}

}
void attach_join_to_cache(RelOptInfo *rel, int level) {
	dlist_iter iter;
	int index = list_length(rel->rel_name) - 1;
	//printContentRelations((CacheM *) join_cache_ptr);
	Assert(index > 0);
	dlist_foreach (iter, &join_cache_ptr->content[index]) {

		MemoRelation *cachedjoin = dlist_container(MemoRelation,list_node, iter.cur);
		if (equalSet(list_copy(cachedjoin->relationname), list_copy(rel->rel_name)) && cachedjoin->level == level) {

			cachedjoin->rel = rel;

		}

	}

}
void score_join_cardinality(RelOptInfo *rel, List *restrictList) {
	/* We attend to build a score a system in order ti trust a given calculated cardinality
	 // we have three possible cases:
	 *
	 * a) left-deep plans: joins with right hand relations as base relations and left hand relations as joins.
	 * b) right-deep plans: joins with left hand relations as base relations and right hand relations as joins.
	 * a) mixed plans: joins with right hand  left relations as joins relations.
	 *
	 * The sccore are computed based in the rinfo confidence and the nature of relations participating in the join and theirs confidence.
	 *
	 * A may score will be obtained always if:
	 * 1) rinfos were calculated by cardinality injection.
	 * 3) the right and left relations have the maximum cardinality score.
	 *
	 * The actual score weight of a relation will be proportional to its size and relatif to the second relation
	 * what means, that the cardinality of a not injected relation.
	 *
	 * we intend to propagate the error of a cardinality in upper calculations by multiplying the errors.
	 *
	 *
	 *
	 */

}
void get_relation_size(MemoInfoData1 *result, PlannerInfo *root, RelOptInfo *rel, List *quals, int isParam,
		SpecialJoinInfo * sjinfo) {
	MemoRelation * memo_rel = NULL;
	double mrows = 1;
	List *final_clauses = NIL;
	int level = rel->rtekind > RTE_SUBQUERY ? root->query_level : root->query_level + rel->rtekind;
	List* final_quals = rel->rtekind == RTE_JOIN ? NIL : quals;
	int varid = isParam == 1 ? rel->relid : 0;
	result->loops = 1;
	result->found = -1;
	result->rows = -1;

	memo_rel = get_Memorelation(result, list_copy(rel->rel_name), level, final_quals, isParam);
	//printf("Result %d\n", result->found);

	//print_relation(memo_rel);

	if (memo_rel) {
		result->loops = memo_rel->loops;
		result->rows = memo_rel->rows;

		mrows = clamp_row_est(result->rows / result->loops);
	}

	switch (result->found) {

		case FULL_MATCHED:
			printf(" FULL Matched  relation! :\n");
			if (list_length(quals) == 1 && varid == 0 && enable_selectivity_injection) {
				if (sjinfo) {

					RestrictInfo * rinfo = (RestrictInfo *) linitial(quals);
					Selectivity s1 = mrows / (sjinfo->outer_rows * sjinfo->inner_rows);
					if (!rinfo->memo_cahed) {
						if (sjinfo->jointype == JOIN_INNER && sjinfo->inner_checked && sjinfo->outer_checked) {
							if (rinfo->norm_selec != s1) {
								printMemo(rinfo);
								printf("sel updated %.10f!\n", s1);
								rinfo->norm_selec = s1;
								rinfo->memo_cahed = true;
							}
						} else {
							if (rinfo->outer_selec != s1) {
								printMemo(rinfo);

								printf("sel updated %.10f!\n", s1);

								rinfo->outer_selec = s1;
								rinfo->memo_cahed = true;
							}
						}
						fflush(stdout);
					}
				}
			}
			result->rows = mrows;
			final_clauses = NIL;
			break;

		case MATCHED_LEFT: {
			ListCell * lc;
			if (!sjinfo || sjinfo->jointype == JOIN_INNER) {
				printf(" MATCHED_LEFT  relation! :\n");

				/*				printMemo(result.unmatches);*/
				result->rows = mrows;
				foreach(lc,result->unmatches) {

					final_clauses = lappend(final_clauses, ((MemoClause *) lfirst(lc))->parent);

				}
			}
			break;
		}
		case MATCHED_RIGHT: {
			if (!sjinfo) {
				// if We parameterized baserel in memo and we look for
				// an unparameterized path try to clamp the rows as the
				// the fraction removed by the filter.

				// First we verify that we are in such case.
				if (isParam == 0) {
					//we are in the unparameterized case so get the fraction
					// of tupless passing the index filter:

					double removedrowsfrac = memo_rel->rows / (memo_rel->rows + memo_rel->removed_rows);
					if (removedrowsfrac < 1)
						mrows = rel->tuples * removedrowsfrac;
					else
						mrows = 1;
				}

				printf(" MATCHED_RIGHT  relation! :\n");

				final_clauses = NIL;
				result->rows = rel->tuples * clauselist_selectivity(root, quals, varid, JOIN_INNER, NULL);
				result->rows = result->rows >= mrows ? result->rows : mrows;
			}
			break;
		}
		default: {
			/*printMemo(rel->rel_name);
			 printf(" UNMATCHED  relation! :\n");*/
			if (!sjinfo) {
				final_clauses = quals;
				//printMemo(final_clauses);
				result->rows = rel->tuples;
				//printf("result->rows : %lf\n", result->rows);

			} else {
				/*if (enable_selectivity_injection) {
				 if (list_length(quals) == 1 && varid == 0 && enable_selectivity_injection) {

				 }

				 }

				 if (enable_memo_propagation) {
				 //attach_join_to_cache(rel, level);

				 }*/

			}
			break;
		}
	}
	if (!sjinfo || result->found == FULL_MATCHED
			|| (sjinfo && result->found == MATCHED_LEFT && sjinfo->jointype == JOIN_INNER)) {
		result->rows = result->rows * clauselist_selectivity(root, final_clauses, varid, JOIN_INNER, sjinfo);

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
static MemoClause * parse_clause1(char * clause, char **next) {
	MemoClause *clause_ptr = (MemoClause *) palloc(sizeof(MemoClause));
	char * args = NULL;
	int arg_len = 7;

	clause_ptr->type = T_MemoClause;
	clause_ptr->opno = 0;
	clause_ptr->args = NIL;
	clause_ptr->parent = NULL;

	if (sscanf(clause, ":opno %u", &clause_ptr->opno) == 1) {
		args = strstr(clause, " :args ");

		if (args != NULL) {

			args = &args[arg_len];

			clause_ptr->args = parse_clause_list(args, next);
			*next = (*next + 4);
			//*next = strstr((*next), "{");
			//if(*next == n)

		}
	} else if (strcmp(clause, "<>") == 0) {
		return NULL;

	} else {

		printf("syntax error when parsing clause: %s \n", clause);

		return NULL;
	}

	return clause_ptr;

}

static Value * parse_args1(char * argm, char **next) {
	int i = 0;
	int balanced = 0;
	Value * result = NULL;
	StringInfoData buff;
	initStringInfo(&buff);
	if (argm[i] == '{') {
		i++;
		balanced++;
		while (balanced != 0 && argm[i] != '\0') {

			if (argm[i] == '}')
				balanced--;
			else if (argm[i] == '{')
				balanced++;
			else if (balanced < 0) {
				printf("Error parsing arg");
				break;

			}
			appendStringInfoChar(&buff, argm[i]);
			i++;
		}
		(&buff)->len--;
		(&buff)->data[(&buff)->len] = '\0';

		if (balanced == 0) {

			*next = &argm[i];

			result = makeString(buff.data);
		}
	}
	return result;
}
static void * parse_item_list(char * element, char **next) {

	char *start = (char*) palloc0(3 * sizeof(char));
	start[2] = '\0';

	if (element == NULL) {
		pfree(start);

		return NULL;
	}

	strncpy(start, element, 2);
	if (!strcmp(start, "{ ")) {

		pfree(start);
		switch (element[2]) {

			case ' ':
				return parse_clause1(&element[3], next);
				break;
			default:
				return parse_args1(&element[0], next);
				break;
		}

	}
	pfree(start);
	return NULL;

}

static List * parse_clause_list(char * lclause, char **next) {
	List *result = NIL;
	char *tmp;
	void * element;
	if (lclause == NULL || lclause[0] != '(' || lclause[1] == '\0') {

		return NIL;

	}
	tmp = &lclause[1];

	while (element = parse_item_list(tmp, next), tmp != NULL && tmp[0] == '{' && element != NULL) {

		result = lappend(result, element);
		tmp = *next;
	}
	if (*next[0] != ')') {
		printf("Malformed List! %s\n ", *next);
		return NIL;

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

	if (sscanf(clause, ":opno %u", &clause_ptr->opno) == 1) {
		args = strstr(clause, ":args (");
		/*		switch(clause_ptr->opno){

		 case AND_EXPR:
		 case OR_EXPR:
		 case NOT_EXPR:
		 parse_cl
		 default:


		 }*/
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
	const ListCell *item_b;
	result->unmatches = NIL;
	result->matches = NIL;

	foreach(item_b, b) {
		if (!list_member_remove(&a, lfirst(item_b))) {

			//printf("member not found\n");
			//printMemo(lfirst(item_b));
			result->unmatches = lappend(result->unmatches, lfirst(item_b));
		} else {

			result->matches = lappend(result->matches, lfirst(item_b));

		}

	}

	result->found = UNMATCHED;

	if ((list_length(lright) == list_length(lleft)) && (list_length(lleft) == list_length(result->matches))
			&& (result->unmatches == NIL)) {
		result->found = FULL_MATCHED;
		return;

	}
	if (list_length(lleft) <= list_length(lright)) {

		if (result->unmatches != NIL && !list_length(a))
			result->found = MATCHED_LEFT;
		else
			result->found = UNMATCHED;

		return;

	}
	if (list_length(lleft) >= list_length(lright)) {
		{
			if (list_length(result->matches) != list_length(lright))
				result->found = UNMATCHED;
			else if (result->unmatches == NIL)
				result->found = MATCHED_RIGHT;
		}
		return;

	}
}
static void try_partial_join(MemoRelation *relation1, MemoRelation *relation2) {
	MemoRelation *found_rel = NULL;
	MemoRelation *newRelation = NULL;

	//find the outer rel name;

	List *diff_name = list_difference(relation2->relationname, relation1->relationname);

	contains(NULL, &found_rel, (CacheM *) memo_query_ptr, diff_name, NIL, relation1->level, 1);
	if (found_rel != NULL) {
		List *newname = list_union(list_copy(relation1->relationname), list_copy(diff_name));
		double rows = relation1->rows * clamp_row_est(found_rel->rows / found_rel->loops);
		List *newclauses = list_union(relation1->clauses, found_rel->clauses);
		newRelation = create_memo_realation(relation1->level, false, newname, rows, 1, NIL);
		newRelation->nodeType = -1;
		newRelation->clauses = newclauses;
		add_relation(newRelation, list_length(newRelation->relationname));

	}

}

void check_NoMemo_queries(void) {
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
		int k;
		found_new = false;

		for (i = 1; i < (join_cache_ptr->max_level - 1); i++) {

			dlist_foreach_modify (iter, &join_cache_ptr->content[i]) {
				MemoRelation *target = dlist_container(MemoRelation,list_node, iter.cur);
				rellen = list_length(target->relationname);

				for (k = 2; k < (memo_query_ptr->max_level); k++) {

					relation1 = NULL;
					relation2 = NULL;
					commonrelation = NULL;
					query = find_seeder_relations(&relation1, &relation2, &commonrelation,
							list_copy(target->relationname), list_copy(target->clauses), target->level, k);
					//check for candidates matches existence
					if (relation1 != NULL && relation2 != NULL && query != NULL) {
						// delete the current NoMemoRelation from the un_cache

						//Call the calculation function to get estimated rows and merk the RelOptInfo
						//struct
						found_new = true;
						set_estimated_join_rows(relation1, relation2, commonrelation, target);
						dlist_delete(&(target->list_node));
						//printf("Founds rows :\n%lf\n**********************\n", target->rows);

						add_node(query, target, rellen);

						/*optional step*/

						try_partial_join(relation1, relation2);
						break;

					}
				}
				//printf("\n");
			}
			discard_existing_joins();

		}
	}

}
static MemoQuery * find_seeder_relations(MemoRelation **relation1, MemoRelation **relation2,
		MemoRelation **commonrelation, List * targetName, List * targetClauses, int level, int joinlen) {

	dlist_iter iter;
	dlist_iter iter1;

//	char *str1, *str2 = NULL;

	if (list_length(targetName) == 1) {
		printf("got 1");
		return NULL;

	}

	dlist_foreach(iter, &memo_cache_ptr->content) {

		MemoQuery *query = dlist_container(MemoQuery,list_node, iter.cur);

		// we fetch the joins with the expected rellen number of relations
		dlist_foreach (iter1, &query->content[joinlen-1]) {
			MemoInfoData1 resultName;
			List *tmp1 = NIL;
			MemoRelation * commonrel = NULL;
			List *clauses = targetClauses;

			MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter1.cur);
			// ATTENTION: the candidate relation must be at the same query level!
			if (memorelation->level == level) {
				if (list_length(memorelation->relationname) < list_length(targetName))
					cmp_lists(&resultName, memorelation->relationname, targetName);
				else
					cmp_lists(&resultName, targetName, memorelation->relationname);

				// Check if we had the expected number of matches in join

				//If *relation is null we are in out first loop
				/*
				 printf("Matches: ");
				 printMemo(resultName.matches);*/
				if (list_length(resultName.matches) == list_length(memorelation->relationname))
					continue;

				//printf("Entering in 1 loop with %d matches \n", resultName_ptr->nbmatches);
				// In the firstfind_seeder_relations pass we need to find a relation who differs from the target
				// relation just from one base relation

				contains(NULL, &commonrel, (CacheM *) query, resultName.matches, NIL, level, 3);
				if (commonrel != NULL && !commonrel->isParameterized) {
					/*bool diff_realIndex = false;
					 bool com_realIndex = false;*/
					if (list_length(memorelation->relationname) > 1)
						clauses = list_union(memorelation->clauses, clauses);

					//if ((list_length(resultName_ptr->matches) == (joinlen - 1))) {
					//Mark the first relation and build the resulted expected target join
					//relation for matching
					*relation1 = memorelation;
					*commonrelation = commonrel;

					tmp1 = list_union(targetName, memorelation->relationname);
					/*

					 printf("Found relation1 relations : \n");
					 printMemo(tmp1);
					 printMemo(clauses);
					 */

					contains(NULL, relation2, (CacheM*) memo_query_ptr, tmp1, clauses, level, 5);
					if (relation2)
						return query;
					else {
						*relation1 = NULL;

						continue;
					}
					//}
				}

			}
		}

	}
	return NULL;
}
bool lcontains(RelOptInfo *rel, List *clauses) {
	ListCell *lc;
	List *b = restictInfoToMemoClauses(list_copy(clauses));
	foreach(lc,rel->all_restrictList) {
		List *a = restictInfoToMemoClauses(list_copy(lfirst(lc)));
		if (equalSet(a, list_copy(b))) {
			list_free(a);
			return true;
			break;
		}
	}

	list_free(b);
	return false;
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

			switch (isParam) {
				case 0:
				case 1:
					lquals = restictInfoToMemoClauses(clauses);
					if (result != NULL) {

						cmp_lists(result, memorelation->clauses, lquals);

						if (isFullMatched(result)) {
							list_free(lquals);
							*relation = &(*memorelation);
							return;
						}
						if (result->found == MATCHED_RIGHT) {
							rightmatched = memorelation;

						}
						if (result->found == MATCHED_LEFT) {
							leftmatched = memorelation;

						}

					}
					continue;

				case 2:

					if (memorelation->nodeType != -1 && memorelation->nodeType != 999) {

						if (result != NULL)
							result->found = FULL_MATCHED;
						*relation = &(*memorelation);
						return;
					}
					continue;

				case 3:
					if (result != NULL)
						result->found = FULL_MATCHED;
					*relation = &(*memorelation);
					return;

				case 5:
					if (equalSet(list_copy(clauses), list_copy(memorelation->clauses))) {
						*relation = &(*memorelation);
						return;
					}
					continue;

				default:
					elog(ERROR, "2 unrecognized option for contains: %d", isParam);
					break;

			}

		}

	}
	if (lquals != NIL)
		list_free(lquals);
	if (result) {
		if (leftmatched) {
			result->found = MATCHED_LEFT;
			*relation = leftmatched;

		} else {
			if (rightmatched) {
				result->found = MATCHED_RIGHT;
				*relation = rightmatched;

			}

		}
	}
}
static void set_estimated_join_rows(MemoRelation *relation1, MemoRelation *relation2, MemoRelation * commonrelation,
		MemoRelation *target) {
	double rows;

	rows = (clamp_row_est(relation2->rows / relation2->loops) / clamp_row_est(relation1->rows / relation1->loops))
			* clamp_row_est(commonrelation->rows / commonrelation->loops);
	target->rows = rows <= 0 ? 1 : rows;
	target->loops = 1;
	target->isParameterized = commonrelation->isParameterized;
	if (target->rel) {

		target->rows = rows;
	}
	/*printf("Injected new estimated size  %lf for : \n", rows);
	 print(target->relationname);
	 printf("\n");
	 fflush(stdout);*/

}
void store_join(List *lrelName, int level, List *clauses, double rows, bool isParam) {
	MemoRelation *relation;
	StringInfoData str;
	initStringInfo(&str);
	if (list_length(clauses) == 1 && list_length(lrelName) > 2) {

		relation = NULL;

	}
	explicitNode(rte_ref_ptr, clauses, &str);

	relation = newMemoRelation();
	if (list_length(lrelName) > 0) {
		Assert(lrelName!=NIL);

//appendStringInfoString(&str, " ");

//appendStringInfoString(&str, " ");
		relation->rows = rows;
		relation->level = level;
		relation->relationname = list_copy(lrelName);
		relation->str_clauses = str.data;
		relation->isParameterized = isParam;

		if (enable_memo && !enable_memo_propagation)

			add_node(join_cache_prop_ptr, relation, list_length(lrelName));
		else
			add_node(join_cache_ptr, relation, list_length(lrelName));

	}
	/*printf("Relation Stored\n -------------------------------- \n");
	 print_relation(str1, str2, relation);
	 printf("Ending printing join cache\n -------------------------------- \n");*/

}
void export_join(FILE *file) {
	StringInfoData str;
	CachedJoins *cptr = NULL;
	dlist_iter iter;
	int i = 0;
	if (enable_memo && !enable_memo_propagation)

		cptr = join_cache_prop_ptr;
	else
		cptr = join_cache_ptr;

	if (cptr->size != 0 && file != NULL) {
		initStringInfo(&str);

		for (i = 0; i < cptr->length; ++i) {

			dlist_foreach (iter, &cptr->content[i]) {
				MemoRelation *memorelation = dlist_container(MemoRelation,list_node, iter.cur);
				buildSimpleStringList(&str, memorelation->relationname);

				fprintf(file, "124\t%d\t%d\t%s\t%.0f\t0\t1\t0\t%d\t%s\n", memorelation->isParameterized,
						memorelation->level, str.data, memorelation->rows, (int) strlen(memorelation->str_clauses),
						memorelation->str_clauses);

				resetStringInfo(&str);

			}

		}

	}

}
void printMemoCache(void) {

	printContentRelations((CacheM*) memo_query_ptr);
	printf("Cache size : %d realtions.\n", memo_query_ptr->size);
	printf("Cache max level : %d .\n", memo_query_ptr->max_level);
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

	if (enable_memo) {
		discard_existing_joins();
		check_NoMemo_queries();

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

int equals(MemoRelation *rel1, MemoRelation *rel2) {

	return equalSet(list_copy(rel1->relationname), list_copy(rel2->relationname));

}

void set_plain_rel_sizes_from_memo(PlannerInfo *root, RelOptInfo *rel, Path *path, double *loop_count, bool isIndex) {

	if (path->param_info) {
		/*if (!path->param_info->memo_checked && enable_memo_recosting && enable_memo_propagation) {
		 path->param_info->ppi_rows = get_parameterized_baserel_size(root, rel, path->param_info->ppi_clauses);
		 path->param_info->memo_checked = true;
		 }*/
		path->restrictList = list_copy(path->param_info->restrictList);
		path->rows = path->param_info->ppi_rows;

	} else {
		/*if (!rel->base_rel_checked && enable_memo_recosting && enable_memo_propagation) {
		 set_baserel_size_estimates(root, rel);
		 rel->base_rel_checked = true;
		 }*/
		path->restrictList = list_copy(path->parent->restrictList);
		path->rows = path->parent->rows;

	}
	path->isParameterized = path->param_info != NULL;
}
static void update_inner_indexpath(PlannerInfo *root, JoinPath * jpath, Path *innerpath) {
	if (enable_memo_propagation) {

		if (!jpath->joinrestrictinfo) {
			switch (innerpath->type) {

				case T_IndexPath: {
					IndexPath * index = (IndexPath *) innerpath;
					if (index->path.param_info) {
						index->path.rows = clamp_row_est(jpath->path.parent->rows / jpath->outerjoinpath->rows);
						index->path.param_info->ppi_rows = index->path.rows;
						index->path.param_info->memo_checked = true;
						index->indexinfo->loop_count = jpath->outerjoinpath->rows;

					}
					break;
				}
				case T_BitmapHeapPath: {
					BitmapHeapPath * bitmappath = (BitmapHeapPath *) innerpath;
					update_inner_indexpath(root, jpath, bitmappath->bitmapqual);
					if (bitmappath->path.param_info) {
						bitmappath->path.rows = clamp_row_est(jpath->path.parent->rows / jpath->outerjoinpath->rows);
						bitmappath->path.param_info->ppi_rows = bitmappath->path.rows;
						bitmappath->path.param_info->memo_checked = true;
						bitmappath->loop = jpath->outerjoinpath->rows;

					}
					break;
				}
				default:
					break;
			}

		}

	}

}
void set_join_sizes_from_memo(PlannerInfo *root, RelOptInfo *rel, JoinPath *pathnode) {

	pathnode->path.isParameterized = pathnode->path.param_info != NULL;

}

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
void update_and_recost(PlannerInfo *root, RelOptInfo *joinrel) {
	if (enable_memo_recosting)
		recost_paths(root, joinrel);
	add_recosted_paths(joinrel);
}
void recost_rel_path(PlannerInfo *root, RelOptInfo *baserel) {

	/*
	 ListCell *lc;
	 printf("Theres %d path for : ", list_length(baserel->pathlist));
	 printMemo(baserel->rel_name);
	 foreach(lc,baserel->pathlist) {

	 Path *basepath = (Path *) lfirst(lc);

	 recost_path_recurse(root, basepath);

	 }
	 set_cheapest(baserel);
	 */

}

static void recost_join_child(PlannerInfo *root, Path * childpath) {
	switch (childpath->type) {

		case T_Path:
		case T_IndexPath:
		case T_BitmapHeapPath:
		case T_MaterialPath:
		case T_UniquePath:
			recost_path_recurse(root, childpath);
			break;
		default:
			break;

	}

}

static void recost_join_children(PlannerInfo *root, JoinPath * jpath) {
	if (jpath->path.type == T_NestPath && enable_memo_recosting)
		update_inner_indexpath(root, jpath, jpath->innerjoinpath);

	//Base relation cases
	recost_join_child(root, jpath->innerjoinpath);
	recost_join_child(root, jpath->outerjoinpath);

}
static void recost_path_recurse(PlannerInfo *root, Path * path) {
	JoinCostWorkspace *workspace = NULL;
	double oldcost = path->total_cost;
	double newcost = 0;
	switch (path->type) {

		case T_Path:

			//set_plain_rel_sizes_from_memo(root, path->parent, path, NULL, false);

			cost_seqscan(path, root, path->parent, path->param_info);

			break;
		case T_IndexPath:

			//set_plain_rel_sizes_from_memo(root, path->parent, &((IndexPath *) path)->path, NULL, false);

			cost_index((IndexPath *) path, root, ((IndexPath *) path)->indexinfo->loop_count);

			break;
		case T_BitmapHeapPath:
			///set_plain_rel_sizes_from_memo(root, path->parent, &((BitmapHeapPath *) path)->path, NULL, false);

			recost_path_recurse(root, ((BitmapHeapPath *) path)->bitmapqual);

			cost_bitmap_heap_scan(&((BitmapHeapPath *) path)->path, root, path->parent,
					((BitmapHeapPath *) path)->path.param_info, ((BitmapHeapPath *) path)->bitmapqual,
					((BitmapHeapPath *) path)->loop);

			break;
		case T_MergePath:
			recost_join_children(root, (JoinPath *) path);
			workspace = ((MergePath *) path)->jpath.workspace;

			initial_cost_mergejoin(root, workspace, ((MergePath *) path)->jpath.jointype,
					((MergePath *) path)->path_mergeclauses, ((MergePath *) path)->jpath.outerjoinpath,
					((MergePath *) path)->jpath.innerjoinpath, ((MergePath *) path)->outersortkeys,
					((MergePath *) path)->innersortkeys, NULL);
			final_cost_mergejoin(root, (MergePath *) path, workspace, NULL);

			break;
		case T_HashPath: {

			HashPath *hpath = (HashPath *) path;

			SemiAntiJoinFactors semifactors;

			recost_join_children(root, (JoinPath *) path);

			workspace = ((HashPath *) path)->jpath.workspace;
			semifactors.match_count = workspace->match_count;
			semifactors.outer_match_frac = workspace->outer_match_frac;

			initial_cost_hashjoin(root, workspace, hpath->jpath.jointype, hpath->path_hashclauses,
					hpath->jpath.outerjoinpath, hpath->jpath.innerjoinpath, NULL, &semifactors);
			final_cost_hashjoin(root, hpath, workspace, NULL, &semifactors);

			break;
		}
		case T_NestPath: {

			SemiAntiJoinFactors semifactors;
			recost_join_children(root, (JoinPath *) path);
			workspace = ((NestPath *) path)->workspace;
			semifactors.match_count = workspace->match_count;
			semifactors.outer_match_frac = workspace->outer_match_frac;

			initial_cost_nestloop(root, workspace, ((NestPath *) path)->jointype, ((NestPath *) path)->outerjoinpath,
					((NestPath *) path)->innerjoinpath, NULL, &semifactors);
			final_cost_nestloop(root, ((NestPath *) path), workspace, NULL, &semifactors);

			break;
		}
		case T_MaterialPath:

			recost_join_child(root, ((MaterialPath *) path)->subpath);
			cost_material(&((MaterialPath *) path)->path, ((MaterialPath *) path)->subpath->startup_cost,
					((MaterialPath *) path)->subpath->total_cost, ((MaterialPath *) path)->subpath->rows,
					((MaterialPath *) path)->subpath->parent->width);

			break;
		case T_UniquePath: {

			UniquePath *upath = (UniquePath *) path;
			recost_join_child(root, upath->subpath);

			switch (upath->umethod) {

				case UNIQUE_PATH_SORT:
					cost_sort(&upath->path, root, NIL, upath->subpath->total_cost, upath->path.parent->rows,
							upath->path.parent->width, 0.0, work_mem, -1.0);

					break;

				case UNIQUE_PATH_HASH:

					cost_agg(&upath->path, root, AGG_HASHED, NULL, list_length(upath->uniq_exprs), upath->path.rows,
							upath->subpath->startup_cost, upath->subpath->total_cost, upath->path.parent->rows);

					break;
				default:
					break;

			}

			break;

		}

		default:
			elog(ERROR, "1 unrecognized node type: %d", (int) path->type);
			break;
	}
	newcost = path->total_cost;
	if (newcost - oldcost != 0) {
		printMemo(path->parent->rel_name);

		printf("Old path %d cost : %lf    ,", path->type, oldcost);

		printf("New path %d cost : %lf\n,", path->type, newcost);
		fflush(stdout);
	}

}

void restimate_plain_rel_path(PlannerInfo *root, RelOptInfo *baserel) {

	ListCell *lc;

	foreach(lc,baserel->pathlist) {

		Path *basepath = (Path *) lfirst(lc);

		if (!basepath->param_info) {
			//Resize the unparameterized paths for this relation

			basepath->rows = basepath->parent->rows;

		} else {
			//if we has paraeterized path not checked by memo
			// then restimate cardinality

			if (!basepath->param_info->memo_checked) {

				basepath->param_info->ppi_rows = get_parameterized_baserel_size(root, basepath->parent,
						basepath->param_info->ppi_clauses);
				basepath->memo_checked = true;

			}

		}
	}

}
void recost_paths(PlannerInfo *root, RelOptInfo *joinrel) {
	ListCell *lc;

	foreach(lc,joinrel->tmp_pathlist) {

		Path *joinpath = (Path *) lfirst(lc);
		JoinPath *jpath = (JoinPath *) joinpath;
		if (jpath->invalid == false) {
			recost_path_recurse(root, joinpath);

		}
	}

}

static Relids get_requiredouter(Path *path) {

	switch (path->type) {
		case T_MergePath:

			return ((MergePath *) path)->jpath.required_outer;
			break;
		case T_HashPath:

			return ((HashPath *) path)->jpath.required_outer;
			break;
		case T_NestPath:
			return ((NestPath *) path)->required_outer;
			break;
		case T_MaterialPath:
			return get_requiredouter(((MaterialPath *) path)->subpath);

			break;
		default:

			elog(ERROR, "2 unrecognized node type: %d", (int) path->pathtype);

			return NULL;
			break;
	}
	return NULL;
}

void invalide_removed_path(RelOptInfo *rel, Path* path) {

	if (rel->rtekind == RTE_JOIN) {
		ListCell *lc;

		foreach(lc,((JoinPath *)path)->path.parent_paths) {
			JoinPath *joinpath = (JoinPath *) lfirst(lc);
			joinpath->invalid = true;
			invalide_removed_path(joinpath->path.parent, (Path*) joinpath);
		}
	}
}

void attach_child_joinpath(Path *parent_path, Path *child_path) {

	if (child_path->parent->rtekind != RTE_JOIN) {
		return;
	}
	switch (child_path->type) {
		case T_MergePath: {
			MergePath * joinpath = (MergePath *) child_path;
			joinpath->jpath.path.parent_paths = lappend(joinpath->jpath.path.parent_paths, parent_path);
			break;
		}
		case T_HashPath: {
			HashPath * joinpath = (HashPath *) child_path;
			joinpath->jpath.path.parent_paths = lappend(joinpath->jpath.path.parent_paths, parent_path);
			break;
		}
		case T_NestPath: {
			NestPath * joinpath = (NestPath *) child_path;
			joinpath->path.parent_paths = lappend(joinpath->path.parent_paths, parent_path);

			break;

		}
		case T_MaterialPath:
		case T_UniquePath:
			attach_child_joinpath(parent_path, ((UniquePath *) child_path)->subpath);
			break;
		default:
			elog(ERROR, "2 unrecognized node type: %d", (int) child_path->type);
			break;

	}

}
void add_recosted_paths(RelOptInfo *joinrel) {

	ListCell *lc;

	foreach(lc,joinrel->tmp_pathlist) {
		Path *joinpath = (Path *) lfirst(lc);
		List *pathkeys = joinpath->pathkeys;
		Relids required_outer = get_requiredouter(joinpath);
		Cost startup_cost = joinpath->startup_cost;
		Cost total_cost = joinpath->total_cost;

		if (add_path_precheck_final(joinrel, startup_cost, total_cost, pathkeys, required_outer)) {

			add_path_final(joinrel, joinpath);

		}

	}

}
