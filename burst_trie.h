/**
 *	burst_trie.h
 *	final version
 *	Amal Cao (amalcao@tju.edu.cn)
 *	(since Mar. 15th. 2009)
 *
 *	License:	BSD Open Source License.
 *				
 *	CHANGES LIST:
 *
 *	DATE				CONTENT
 *	Mar. 27th			1) Change the TrieNode's definition, 
 *						   add a info section to hold the container nodes'
 *						   current max size;
 *						2) Add a new macro to define the min size of alloc section.
 *
 *	Mar. 25th			Change the TrieCursor's definition.
 *
 *	Mar. 24th			Change the KeyVal's definition.
 *	Mar. 23rd			End Debug, the first stable version.
 *	Mar. 15th			Start
 *
 */

#ifndef _BURST_TRIE_H_
#define _BURST_TRIE_H_

#define MIN_CONT	1	

#define	INT_CNT_SIZE 16
#define CH_CNT_SIZE 8
#define INT_TREE_WIDTH 256
#define CH_TREE_WIDTH 64
#define INT_CONT_SIZE 256
#define CH_CONT_SIZE 12 

#define Index	next.index
#define Cont	next.cont
#define Nil		next.nil

#define Counter info.counter
#define MaxSize	info.max_size

#define Left	ptr0.left
#define Right	ptr1.right
#define Head	ptr0.head
#define	Rear	ptr1.rear

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "server.h"

typedef enum {
	BT_SUCCESS,
	BT_KEY_NF,
	BT_ENTRY_E,
	BT_ENTRY_NE,
	BT_END,
	BT_ERROR
} BurstTrieErrCode;

typedef enum {
	TRIE,
	CONTAINER,
	NIL
} TrieType;

typedef union {
	int64_t	intkey;
	int32_t	shortkey;
	char	*charkey;
	uint8_t units[8];
} KeyVal;

typedef struct TrieRecord {
	char	*payload;
	struct	TrieRecord *next;
} TrieRecord;

typedef struct TrieLeaf {
	KeyVal		keyval;
	TrieRecord	*record;
} TrieLeaf;

typedef struct TrieNode {
	TrieType	type;
	int			size;
	union {
		int8_t	counter[INT_CNT_SIZE];
		int		max_size; 	
	} info;
	union {
		struct TrieNode 	*left;
		int			head;
	} ptr0;
	union {
		struct TrieNode	*right;
		int			rear;
	} ptr1;
	union {
		struct TrieNode	**index;
		TrieLeaf	*cont;
		TrieLeaf	*nil;
	} next;
} TrieNode;


/*The universal definition of burst trie. */
typedef struct BurstTrie {
	KeyType		type;
	TrieNode	*root;
	int			max_depth;
	int			counter_size;
	int			container_size;
	int			tree_width;
	int			trie_num;
} BurstTrie;


typedef struct {
	TrieNode		*trie;
	TrieRecord		*record;
	int				pos;
} TrieCursor;



//functions list

BurstTrieErrCode initTrieNode(BurstTrie *bt, TrieNode **trie, TrieType type, int depth);

BurstTrieErrCode getCharKey(BurstTrie *bt, TrieCursor *cursor, Key *key);

#endif
