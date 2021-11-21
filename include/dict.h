#define MAX_NAME_LEN	(264)
#define INITIAL_SIZE (1024*1024*2)

struct elt {
    char key[MAX_NAME_LEN];
    int next; 
    int value;
};

struct dict {
    int size;           /* size of the pointer table */
    int n;              /* number of elements stored */
	int first_av;
	int pad;
};

typedef struct dict *Dict;

void DictCreate(Dict d, int nSize, struct elt ** p_elt_list, int ** p_ht_table);
int DictInsertAuto(Dict d, const char *key, struct elt ** p_elt_list, int ** p_ht_table);
int DictInsert(Dict d, const char *key, const int value, struct elt ** p_elt_list, int ** p_ht_table);
int DictSearch(Dict d, const char *key, struct elt ** p_elt_list, int ** p_ht_table);
void DictDelete(Dict d, const char *key, struct elt ** p_elt_list, int ** p_ht_table);
