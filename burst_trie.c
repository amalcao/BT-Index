/**
 *	burst_trie.c
 *	final version
 *	Amal Cao (amalcao@tju.edu.cn)
 *	(since Mar. 15th. 2009)
 *
 *	License:	BSD Open Source License.
 *				
 *	CHANGES LIST:
 *
 *	DATE				CONTENT
 *	Mar. 28th			1) Add a Re-Size method to control the memory waist:
 *						   When alloc the container space for the i-th depth node,
 *						   just alloc MAX_CONATINER_SIZE / 2^i first;
 *						   When more space will be needed during the insertion,
 *						   realloc for it again.
 *						2) Correct a bug in the insertBurstTrie(...).
 *						3) Change keyCmp(...) function, send cmp as an argument.
 *
 *	Mar. 25th			Re-write the get/getNext functions.	
 *	Mar. 24th			1) Pass values (rather than pointers) to keyCmp fuction;
 *						2) Change the getIndex function.
 *
 * 	Mar. 23rd			End Debug, the first stable version.
 *	Mar. 22nd			First version is completed.
 *	Mar. 16th			Start
 *
 */

#include "burst_trie.h"

#define cpyKeyVal(to, from)	(to).intkey = (from).intkey

inline BurstTrieErrCode reSizeContainer(TrieNode *trie, int container_size, int depth)
{
	if (depth == 0 || trie->MaxSize >= container_size)
		return BT_ERROR;
	int size = (container_size >> depth);
	if (size < MIN_CONT)
		size = MIN_CONT;

	size += trie->MaxSize;
	TrieLeaf *tmp = trie->Cont;

	if ((trie->Cont = realloc(tmp, sizeof(TrieLeaf)*size)) == NULL) 
		return BT_ERROR;
	
	memset(&(trie->Cont[trie->MaxSize]), 0, sizeof(TrieLeaf)*(size-trie->MaxSize));
	trie->MaxSize = size;

	return BT_SUCCESS;
	
}

void inline setKeyVal(KeyVal *keyval, Key *key)
{
	switch (key->type) {
		case SHORT:
		case INT:	keyval->intkey = key->keyval.intkey; return;
		case VARCHAR:	keyval->charkey = &(key->keyval.charkey[0]); return;
	}
}
/**
 * Use to get the position in the trie in-node index.
 */
uint8_t inline getIndex(int depth, KeyVal keyval, KeyType type)
{
	uint8_t pos;
	int tmp;
	
	switch (type) {
		case SHORT:
			tmp = (int8_t)keyval.units[3-depth];
	
			if (depth == 0) {
				tmp += INT_TREE_WIDTH/2;
			}
			return (uint8_t)tmp;
			
		case INT:
		
			tmp = (int8_t)keyval.units[7-depth];
			
			if (depth == 0) {
				tmp += INT_TREE_WIDTH/2;
			}
			return (uint8_t)tmp;
			
		case VARCHAR:
			pos = (uint8_t)(keyval.charkey[depth]);
			if (pos != 0) {
				pos -= 64;
			}
			return pos;
	} 

	return pos;
}


/**
 * Compare two KeyVal type elements key1 and key2.
 * if key1 > key2 return a postive number;
 * else if key1 < key2 return a negtive one;
 * else 0 should be returned.
 **/
void inline keyCmp(KeyVal key1, KeyVal key2, int depth, KeyType type, int64_t *cmp) 
{
	switch (type) {
		case SHORT:
			*cmp = key1.shortkey - key2.shortkey;
			break;
		case INT:
			*cmp = key1.intkey - key2.intkey;
			break;
		case VARCHAR:
			*cmp = strcmp(key1.charkey+depth, key2.charkey+depth);
			break;
		//default :
			
	}
	
}

/**
 * release the memory space of the deleted record link.
 */
BurstTrieErrCode freeRecordLink(TrieRecord *record) 
{
	TrieRecord *tmp = NULL, *ptr = record;

	while (ptr) {
		tmp = ptr;
		ptr = ptr->next;
		free(tmp->payload);
		free(tmp);
	}

	return BT_SUCCESS;
}
/**
 * Insert a new record to the rear of a record link.
 **/

BurstTrieErrCode insertRecordLink(TrieRecord **record, char **payload)
{
	TrieRecord *ptr = *record, *newrecord = NULL;
	int cmp = 1;

	if (ptr != NULL) {
		while (ptr->next != NULL && (cmp = strcmp(ptr->payload, *payload)) != 0) {
			ptr = ptr->next;
		}

		if (ptr->next != NULL || (cmp = strcmp(ptr->payload, *payload)) == 0)
			return BT_ENTRY_E;
	}

	newrecord = malloc(sizeof(TrieRecord));
	newrecord->payload = malloc(sizeof(char)*MAX_PAYLOAD_LEN);
	strcpy(newrecord->payload, *payload);
	newrecord->next = NULL;

	*payload = newrecord->payload;

	if (*record == NULL)
		*record = newrecord;
	else
		ptr->next = newrecord;

	return BT_SUCCESS;
}

/**
 *	Delete a record form a record link.
 **/
BurstTrieErrCode deleteRecordLink(TrieRecord **record, char *payload, TrieRecord **del)
{
	if (*record == NULL)
		return BT_ENTRY_NE;
	
	TrieRecord *ptr = *record, *pre = *record;

	if (payload == NULL) {
		//delete all the records!
		*del = *record;
		*record = NULL;
		return BT_SUCCESS;
	}

	while (ptr != NULL) {
		int cmp = strcmp(ptr->payload, payload);

		if (cmp == 0) {
			*del = ptr;
			if (pre == *record)
				*record = ptr->next;
			else
				pre->next = ptr->next;

			(*del)->next = NULL;

			return BT_SUCCESS;
		}
	} //while

	if (payload != NULL) //not found the expected one!
		return BT_ENTRY_NE;

}
/**
 * Create a BurstTrie tree, also is the head pointer.
 * */
BurstTrieErrCode createBurstTrie(BurstTrie **bt, KeyType type)
{
	*bt = (BurstTrie*)malloc(sizeof(BurstTrie));
		
	//Init the burst tire tree.
	(*bt)->type = type;
	(*bt)->trie_num = 0;

	switch (type) {
		case SHORT:
			(*bt)->max_depth = 3;
			(*bt)->container_size = INT_CONT_SIZE;
			(*bt)->tree_width = INT_TREE_WIDTH;
			(*bt)->counter_size = INT_CNT_SIZE;
			break;
		case INT:
			(*bt)->max_depth = 7;
			(*bt)->container_size = INT_CONT_SIZE;
			(*bt)->tree_width = INT_TREE_WIDTH;
			(*bt)->counter_size = INT_CNT_SIZE;
			break;
		case VARCHAR:
			(*bt)->max_depth = MAX_PAYLOAD_LEN;
			(*bt)->container_size = CH_CONT_SIZE;
			(*bt)->tree_width = CH_TREE_WIDTH;
			(*bt)->counter_size = CH_CNT_SIZE;
			break;
	}

	int nodeType = CONTAINER;
	if (initTrieNode(*bt, &((*bt)->root), nodeType, 0) != BT_SUCCESS) {
		return BT_ERROR;//Will not happen now.
	}

	return BT_SUCCESS;
}


/**
 * Init a new burst trie node, if no memory yet, alloc.
 * */
//update 26-03-2009
//send depth to the function.
BurstTrieErrCode initTrieNode(BurstTrie *bt, TrieNode **trie, TrieType type, int depth)
{
	int size;
	*trie = (TrieNode*)malloc(sizeof(TrieNode));
	
	memset(*trie, 0, sizeof(TrieNode));
	(*trie)->type = type;
	
	switch (type) {
		case TRIE:
			/*Not support now!*/
			break;
		case CONTAINER:
			size = ((bt->container_size) >> depth);
			if (size < MIN_CONT)
				size = MIN_CONT;
			(*trie)->Cont =
				(TrieLeaf*)malloc(size*sizeof(TrieLeaf));
			memset((*trie)->Cont, 0, size*sizeof(TrieLeaf));
			(*trie)->MaxSize = size;
			break;
		case NIL:
			(*trie)->Nil = 
				(TrieLeaf*)malloc(sizeof(TrieLeaf));
			memset((*trie)->Cont, 0, sizeof(TrieLeaf));
			break;
	}
		
	return BT_SUCCESS;
}

/**
 * Get a (Key, Payload) pair from the trie.
 * If Payload is a null pointer, return all the record with the given key.
	
 * And also return the cursor for scanning the trie.
 **/
 BurstTrieErrCode getCursor(BurstTrie *bt, TrieCursor *cursor, Key *key) 
 {

#ifdef _FULL_VERSION_
 	if (bt == NULL || bt->root == NULL) {
		perror("Null burst trie pointer! In getCursor.\n");
		return BT_ERROR;
	}

	if (bt->type != key->type) {
	
		perror("Key type not match! In getCursor.\n");
		return BT_ERROR;
	}
#endif
	//If there is no element in this burst trie tree.
	if (bt->root->size == 0) {
		cursor->trie = bt->root;
		cursor->pos = -1;
		return BT_END;
	}
//	if (bt->root->type == VARCHAR)
//		return getCharKey(bt, cursor, key);


	TrieNode *trie = bt->root, *pretrie = NULL;
	TrieLeaf *tmp = NULL;
	KeyVal keyval;
	int depth = 0, pos, mid, right, left, i;
	int64_t cmp = 0;
		
	setKeyVal(&keyval, key);

	while (trie->type == TRIE) {
		pos = getIndex(depth, keyval, bt->type);
		depth ++;

		pretrie = trie;
		trie = trie->Index[pos];
		
		if (trie == NULL) {
			cursor->pos = pos;
			cursor->trie = pretrie;
			cursor->record = NULL;			
			return BT_KEY_NF;
		}
		
	}//while

	cursor->trie = trie;
	//The case of the nil node.
	//The string end: '\0'.
	/**if (trie->type == NIL) {
		cursor->pos = 0;
		cursor->record = trie->Nil->record;

		return BT_SUCCESS;
	}*/

	//The case of the container node. 
	//Binary search.
	left = mid = 0;
	right = trie->size - 1;

	
	while (left <= right) {
		mid = (left + right) / 2;
		tmp = &(trie->Cont[mid]);
		keyCmp(keyval, (tmp->keyval), depth, bt->type, &cmp);

		if (cmp < 0)
			right = mid - 1;
		else if (cmp > 0)
			left = mid + 1;
		else {
			cursor->pos = mid;
			cursor->record = tmp->record;

			return BT_SUCCESS;
		}
	} //while

	//If not found the key:
	cursor->pos = right;
	cursor->record = NULL;

	return BT_KEY_NF;
}

BurstTrieErrCode getCharKey(BurstTrie *bt, TrieCursor *cursor, Key *key) 
{

	TrieNode *trie = bt->root, *pretrie = NULL;
	TrieLeaf *tmp = NULL;
	char *keyval = &(key->keyval.charkey[0]);
	int depth = 0, pos, mid, right, left, cmp, i;
		

	while (trie->type == TRIE) {
		pos = *keyval ++;
		depth ++;

		if (pos != 0)
			pos -= 64;

		pretrie = trie;
		trie = trie->Index[pos];
		
		if (trie == NULL) {
			cursor->pos = pos;
			cursor->trie = pretrie;
			cursor->record = NULL;			
			return BT_KEY_NF;
		}
		
	}//while

	cursor->trie = trie;
	//The case of the nil node.
	//The string end: '\0'.
	if (trie->type == NIL) {
		cursor->pos = 0;
		cursor->record = trie->Nil->record;

		return BT_SUCCESS;
	}

	//The case of the container node. 
	//Binary search.
	/**
	left = mid = 0;
	right = trie->size - 1;

	
	while (left <= right) {
		mid = (left + right) / 2;
		tmp = &(trie->Cont[mid]);
		cmp = strcmp(keyval, (tmp->keyval.charkey + depth));

		if (cmp < 0)
			right = mid - 1;
		else if (cmp > 0)
			left = mid + 1;
		else {
			cursor->pos = mid;
			cursor->record = tmp->record;

			return BT_SUCCESS;
		}
	} //while
*/
	for (i=0; i<trie->size; i++) {
		tmp = &(trie->Cont[i]);
		cmp = strcmp(keyval, tmp->keyval.charkey+depth);
		if (cmp < 0)
			break;
		else if (cmp == 0) {
			cursor->pos = i;
			cursor->record = tmp->record;

			return BT_SUCCESS;
		}
	}
	//If not found the key:
	cursor->pos = --i;
	cursor->record = NULL;

	return BT_KEY_NF;
}

/**
 *	Get the key and record by the given cursor,
 *	and change the cursor for the next search.
 **/

BurstTrieErrCode getNextCursor(BurstTrie *bt, TrieCursor *cursor, Key *nextKey)
{
#ifdef _FULL_VERSION_
	if (bt == NULL || bt->root == NULL || cursor == NULL) {
		perror("\n");
		return BT_EEROR;
	}
#endif
	
	if (cursor->trie == NULL)
		return BT_END;
	
	TrieNode *trie = cursor->trie, *pretrie = NULL;
	TrieLeaf *tmp = NULL;
	unsigned int pos = cursor->pos;
	int	container_size = bt->container_size,
		tree_width = bt->tree_width,
		counter_size = bt->counter_size,
		counter_unit = tree_width / counter_size;

	pos ++;

	if (trie->type != TRIE) {
		if (pos >= trie->size) {
			trie = trie->Right;
			pos = 0;
			if (trie == NULL)
				return BT_END;
		}
	}	
	else {
		//now is the case for trie node.
		int i, j, found = 0, after = 0;
		
		if (trie->Rear >= pos) {
			for (i=pos/counter_unit; i<counter_size; i++) {
				if (trie->Counter[i] > 0) {
					for (j=i*counter_unit; j<tree_width; j++) {
						if (trie->Index[j]) {
							trie = trie->Index[j];
							found = after = 1;
							break;
						}
					}

					break;
				}

			} //for i
		}
		else if ((j = trie->Rear) >= 0) {
			trie = trie->Index[j];
			found = 1;
		}

		if (found == 0)
			return BT_END;

		while (trie->type == TRIE) {
			if (after == 1)
				j = trie->Head;
			else
				j = trie->Rear;

			trie = trie->Index[j];
		}

		if (after == 0) {
			trie = trie->Right;
			if (trie == NULL)
				return BT_END;
		}

		pos = 0;
	} //else
	
	tmp = &(trie->Cont[pos]);
	if (bt->type == VARCHAR) {
		strcpy(&(nextKey->keyval.charkey[0]), tmp->keyval.charkey);
	}
	else {
		nextKey->keyval.intkey = tmp->keyval.intkey;
	}
	nextKey->type = bt->type;

	//update 03-25-2009
	cursor->record = tmp->record;
	cursor->pos = pos;
	cursor->trie = trie;

	return BT_SUCCESS;

}

/**
 *	Delete the (Key, payload) pair from the trie.
 *	if a null payload sended, delete all the record of the Key.
 *
 **/

 BurstTrieErrCode deleteBurstTrie(BurstTrie *bt, Key *key, char *payload, TrieRecord **del)
 {
 #ifdef _FULL_VERSION_
 	if (bt == NULL || bt->root == NULL) {
		perror("\n");
		return BT_ERROR;
	}
#endif
	if (bt->root->size == 0)
		return BT_ENTRY_NE;
	
	*del = NULL;

	TrieNode *trie = bt->root, **trie_stack;
	TrieLeaf *tmp = NULL;
	TrieRecord *record = NULL;
	KeyVal	keyval;
	int	max_depth = bt->max_depth,
		container_size = bt->container_size,
		tree_width = bt->tree_width,
		counter_size = bt->counter_size,
		counter_unit = tree_width / counter_size;
	unsigned int pos, prepos, *pos_stack;
	int depth = 0, left, right, mid, i, j, ret;
	int64_t cmp;

	setKeyVal(&keyval, key);

	//alloc the stack space
	trie_stack = malloc(max_depth*sizeof(TrieNode*));
	pos_stack = malloc(max_depth*sizeof(unsigned int));

	while (trie->type == TRIE) {
		pos = getIndex(depth, keyval, bt->type);

		trie_stack[depth] = trie;
		pos_stack[depth] = pos;
		depth ++;

		trie = trie->Index[pos];
		if (trie == NULL) {
			free(trie_stack);
			free(pos_stack);
			return BT_ENTRY_NE;
		}
	}


	if (trie->type == NIL) {
	//if it is the nil node:
		record = trie->Nil->record;

		if (deleteRecordLink(&record, payload, del) != BT_SUCCESS) {
			free(trie_stack);
			free(pos_stack);
			return BT_ENTRY_NE;
		}
		trie->Nil->record = record;

		if (record != NULL) {
			free(trie_stack);
			free(pos_stack);
			return BT_SUCCESS;
		}
		//all the records have been deleted.
		if (bt->type == VARCHAR)
			free(trie->Nil->keyval.charkey);
		trie->size --;
		
	}
	else {
	//it is a container now.
	//start binary search
		left = mid = cmp = 0;
		right = trie->size - 1;

		while (left <= right) {
			mid = (left + right) / 2;
			tmp = &(trie->Cont[mid]);
			keyCmp(keyval, (tmp->keyval), depth, bt->type, &cmp);

			if (cmp < 0)
				right = mid - 1;
			else if (cmp > 0)
				left = mid + 1;
			else {
				if (deleteRecordLink(&(tmp->record), payload, del) != BT_SUCCESS) { 
					free(trie_stack);
					free(pos_stack);
					return BT_ENTRY_E;
				}
				
				if (tmp->record != NULL) {
					free(trie_stack);
					free(pos_stack);
					return BT_SUCCESS;
				}

				if (bt->type == VARCHAR)
					free(tmp->keyval.charkey);

				trie->size --;
				
				if (trie->size > 0) {
					for (i=mid; i<trie->size; i++) {
						tmp = &(trie->Cont[i+1]);
						memcpy(&(trie->Cont[i]), tmp, sizeof(TrieLeaf));
					}
				}
				break;
			}
		} //while
	
	}// else

	if (*del == NULL) {
		free(trie_stack);
		free(pos_stack);
		return BT_ENTRY_NE;
	}

	//trace back to check delete the parent node if size is 0.
	while (trie->size == 0 && depth > 0) {
		//update double link!
		if (trie->type != TRIE) { //check a bug; trie nodes have no double link! 24-03-2009
			if (trie->Left != NULL)
				(trie->Left)->Right = trie->Right;
			if (trie->Right != NULL)
				(trie->Right)->Left = trie->Left;
		}
		switch (trie->type) {
			case TRIE:
				free(trie->Index);
				break;
			case CONTAINER:
				free(trie->Cont);
				break;
			case NIL:
				free(trie->Nil);
				break;
		}
		free(trie);
	
		depth --;
		trie = trie_stack[depth];
		pos = pos_stack[depth];

		trie->size --;
		trie->Counter[pos/counter_unit] --;
		trie->Index[pos] = NULL;

		if (trie->size == 0) {
			//pos = prepos;
			continue;
		}
			
		//update the head rear pointers!
		if (trie->Head == pos) {
			for (i=pos/counter_unit; i<counter_size; i++) {
				if (trie->Counter[i] != 0) {
					for (j=i*counter_unit; j<tree_width; j++)
						if (trie->Index[j]) {
							trie->Head = j;
							break;
						}
					break;
				}
			}
		} //if 
		if (trie->Rear == pos) {
			for (i=pos/counter_unit; i>=0; i--) {
				if (trie->Counter[i] != 0) {
					for (j=(i+1)*counter_unit-1; j>=0; j--)
						if (trie->Index[j]) {
							trie->Rear = j;
							break;
						}
					break;
				}
			}
		} //if

	} //while

	if (trie->size == 0) {
		trie->type = CONTAINER;
		trie->Left = trie->Right = NULL;
	}

	free(trie_stack);
	free(pos_stack);

	return BT_SUCCESS;

 }
/**
 *	Insert the (Key, payload) pair into the trie.
 *	
 **/

BurstTrieErrCode insertBurstTrie(BurstTrie *bt, Key *key, char **payload)
{

#ifdef _FULL_VERSION_
	if (bt == NULL || bt->root == NULL) {
		perror("\n");
		return BT_ERROR;
	}

#endif

	TrieNode *trie = bt->root, *pretrie = NULL, *tmptrie = NULL;
	TrieLeaf *tmp = NULL, pre, current;
	TrieRecord *record = NULL;
	TrieType type = CONTAINER;
	KeyVal	keyval;
	int	max_depth = bt->max_depth,
		container_size = bt->container_size,
		tree_width = bt->tree_width,
		counter_size = bt->counter_size,
		counter_unit = tree_width / counter_size;
	int depth = 0, left, right, mid, pos, insert, i, j;
	int64_t	cmp = 0;

	setKeyVal(&keyval, key);

	while (trie->type == TRIE) {
		pos = getIndex(depth, keyval, bt->type);
		depth ++;

L00:
		pretrie = trie;
		trie = trie->Index[pos];

		//make the new trie node.
		if (trie == NULL) {
			if (pos == 0 && bt->type == VARCHAR)
				type = NIL;
			else
				type = CONTAINER;

			initTrieNode(bt, &trie, type, depth);

			if (type == NIL) {
				trie->size = 1;//!

				tmp = trie->Nil;
				tmp->keyval.charkey = malloc(MAX_VARCHAR_LEN*sizeof(char));
				strcpy(tmp->keyval.charkey, keyval.charkey);
			}

			//update the double link and insert the new payload.
			//
			tmptrie = pretrie;
			int after = 0;
			if (tmptrie->Counter[pos/counter_unit] > 0) {
				for (j=pos+1; j<tree_width; j++) {
					if (tmptrie->Index[j] != NULL) {
						tmptrie = tmptrie->Index[j];
						after = 1;
						goto L01;
					}
				}
	//			if (after == 1)
	//				break;

				for (j=pos-1; j>=0; j--) {
					if (tmptrie->Index[j] != NULL) {
						tmptrie = tmptrie->Index[j];
						goto L01;
					}
				}
			}
			//else {
				//This section has no trie node.
				//search others..
				if (tmptrie->Rear > pos) {
					for (i=(pos/counter_unit)+1; i<counter_size; i++)
						if (tmptrie->Counter[i] > 0) {
							for (j=i*counter_unit; j<tree_width; j++)
								if (tmptrie->Index[j] != NULL) {
									tmptrie = tmptrie->Index[j];
									after = 1;
									goto L01;
								}
							break;
						}
				}
				else {
					for (i=(pos/counter_unit)-1; i>=0; i--)
						if (tmptrie->Counter[i] > 0) {
							for (j=(i+1)*counter_unit-1; j>=0; j--)
								if (tmptrie->Index[j] != NULL) {
									tmptrie = tmptrie->Index[j];
									goto L01;
								}
							break;
						}
				}

			//}
			//find the position to uopdate the double link
L01:
			while (tmptrie->type == TRIE) {
				if (after == 1) 
					j = tmptrie->Head;
				else   
					j = tmptrie->Rear;

				tmptrie = tmptrie->Index[j];
			}

			if (after == 1) {
				trie->Right = tmptrie;
				trie->Left = tmptrie->Left;
				if (tmptrie->Left != NULL)
					tmptrie->Left->Right = trie;
				tmptrie->Left = trie;

			}
			else {
				trie->Left = tmptrie;
				trie->Right = tmptrie->Right;
				if (tmptrie->Right != NULL)
					tmptrie->Right->Left = trie;
				tmptrie->Right = trie;

			}

			//update the rear & head pointers.
			if (pos < pretrie->Head) 
				pretrie->Head = pos;
			if (pos > pretrie->Rear)
				pretrie->Rear = pos;
			//update the trie node info:
			pretrie->Index[pos] = trie;
			pretrie->size ++;
			pretrie->Counter[ pos/counter_unit ] ++;
		}
	} //while

	//If it is a nil node now:
	if (trie->type == NIL) {
		tmp = trie->Nil;
		return insertRecordLink(&(tmp->record), payload);
	}
	//The container now:
	//Binary search:
	left = mid = 0;
	cmp = 0;
	right = trie->size - 1;

	while (left <= right) {
		mid = (left + right) / 2;
		tmp = &(trie->Cont[mid]);
		
		keyCmp(keyval, (tmp->keyval), depth, bt->type, &cmp);

		if (cmp < 0)
			right = mid - 1;
		else if (cmp > 0)
			left = mid + 1;
		else 
			return insertRecordLink(&(tmp->record), payload);
	} //while

	insert = 0;
	//If a burst not happen:
	if (trie->size < container_size) {

		//update: 03-27-2009
		//for support new feature.
		//
		if (trie->size >= trie->MaxSize)
			reSizeContainer(trie, container_size, depth);

		//update: 03-24-2009
		
		for (i=trie->size; i>left; i--) {
			memcpy(&(trie->Cont[i]), &(trie->Cont[i-1]), sizeof(TrieLeaf));
		} //for i
		// i == left now, insert!
	
		tmp = &(trie->Cont[left]);
		if (bt->type == VARCHAR) {
			tmp->keyval.charkey = malloc(MAX_VARCHAR_LEN*sizeof(char));
			strcpy(tmp->keyval.charkey, keyval.charkey);
		}
		else {
			cpyKeyVal(tmp->keyval, keyval);
		}
		
		if (record == NULL) {	//check a smalll bug! 24-03-2009
			tmp->record = NULL; //!
			insertRecordLink(&(tmp->record), payload);
		}
		else
			tmp->record = record;

		trie->size ++;
		return BT_SUCCESS;
	}

	//A joke :-)
	//
//	return BT_ENTRY_E;

	//burst will happen now.
	//be careful!
	while (depth <= max_depth && trie->size <= container_size) {
		TrieNode **newnext = malloc(tree_width*sizeof(TrieNode*));
		TrieLeaf *oldnext = trie->Cont;
		memset(newnext, 0, tree_width*sizeof(TrieNode*));

		//update 26-03-2009
		trie->MaxSize = 0;
		
		int num = 0;
		unsigned int tpos;
		KeyVal *k = NULL;
		TrieNode *newtrie = NULL, *llink = trie->Left, *rlink = trie->Right;

		//clean the double link.
		trie->Left = trie->Right = NULL;

		//init the rear head pointers
		trie->Head = tree_width;
		trie->Rear = 0;

		insert = 0;
		//get the position of the insert key
		pos = getIndex(depth, keyval, bt->type);
		
		for (i=0; i<trie->size; i++) {
			
			tmp = &(trie->Cont[i]);
			k = &(tmp->keyval);

			tpos = getIndex(depth, *k, bt->type);

			if (newnext[tpos] == NULL) {
				type = ((tpos == 0 && bt->type == VARCHAR) ? NIL : CONTAINER);
				//update 26-03-2009, add a new argument.
				initTrieNode(bt, &newtrie, type, depth+1);

				
				//update the double link.
				newtrie->Left = llink;
				if (llink != NULL)
					llink->Right = newtrie;
				llink = newtrie;

				//update the head link of the parent trie node:
				if (trie->Head > tpos)
					trie->Head = tpos;

				//update the trie info:
				newnext[tpos] = newtrie; //!
				num ++;
				trie->Counter[tpos/counter_unit] ++;
			
				//update 24-03-2009
				if (type == NIL) {
					cpyKeyVal(newtrie->Nil->keyval, *k);
					memcpy(newtrie->Nil, tmp, sizeof(TrieLeaf));
					newtrie->size = 1;

					continue;
				}

			}
			else {
			//if the trie with the same index has been created:
				newtrie = newnext[tpos];
			}

			//insert & compare and determine if the new entry can be inserted:
			if (insert == 0 && pos == tpos) {
				keyCmp(keyval, *k, depth, bt->type, &cmp);

				if (cmp < 0) {
					//update 2 lines, 03-27-2009
					if (newtrie->size >= newtrie->MaxSize)
						reSizeContainer(newtrie, container_size, depth+1);

					TrieLeaf *ptr = &(newtrie->Cont[newtrie->size]);
					if (bt->type == VARCHAR) {
						ptr->keyval.charkey = malloc(MAX_VARCHAR_LEN*sizeof(char));
						strcpy(ptr->keyval.charkey, keyval.charkey);
					}
					else {
						cpyKeyVal(ptr->keyval, keyval);
					}
					if (record == NULL)
						insertRecordLink(&(ptr->record), payload);
					else
						ptr->record = record;

					newtrie->size ++;

					insert = 1;
				}
			}

			
			if (newtrie->size < container_size) {

				//update 2 lines, 03-27-2009
				if (newtrie->size >= newtrie->MaxSize)
					reSizeContainer(newtrie, container_size, depth+1);

				cpyKeyVal(newtrie->Cont[newtrie->size].keyval, *k);
				newtrie->Cont[newtrie->size].record = tmp->record;

				newtrie->size ++;
			}
			else {
				//need to burst again!
				//exchange the last entry with the insert entry.
				cpyKeyVal(keyval, *k);
				record = tmp->record;

				insert = -1;
			}
		}//for

		//update the double link:
		llink->Right = rlink;
		if (rlink != NULL)
			rlink->Left = llink;

		//update the rear link of the parent trie node:
		trie->Rear = tpos;

		//release the memory.
		free(oldnext);

		trie->Index = newnext;
		trie->type = TRIE;
		trie->size = num;

		depth ++;

		if (insert == 1) 
			return BT_SUCCESS;
		else if (insert == -1)//goto next burst loop!
			trie = trie->Index[pos];
		else {
			if (trie->Index[pos] == NULL)
				goto L00;
			else {
				trie = trie->Index[pos];
				if (trie->size < container_size) {		
					//update 2 lines, 03-27-2009
					if (trie->size >= trie->MaxSize)
						reSizeContainer(trie, container_size, depth);

					tmp = &(trie->Cont[ trie->size ]);
					if (bt->type == VARCHAR) {
						tmp->keyval.charkey = malloc(MAX_VARCHAR_LEN*sizeof(char));
						strcpy((tmp->keyval.charkey), keyval.charkey);
					}
					else {
						cpyKeyVal(tmp->keyval, keyval);
					}
					insertRecordLink(&(tmp->record), payload);
					trie->size ++;

					return BT_SUCCESS;
				}
				//else burst again.
			} 
		}

	}//while

	return BT_ERROR;
} 
