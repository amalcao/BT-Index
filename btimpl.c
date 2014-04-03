/**
 *	btimpl.c
 * 	final version
 * 	Amalcao (amalcao@tju.edu.cn)
 * 	(Since:	Mar. 22nd. 2009)
 *	
 * 	License:	BSD OPEN SOURCE LICENSE
 *
 * 	CHANGE LIST:
 *	
 *	DATE			CONTENT
 *
 * 	Mar. 26th		1) Using new get/getNext method.
 * 					2) Using Time-out lock method to detect dead lock;
 * 					3) Replace the mutex lock to read-write lock; 
 *
 * 	Mar. 22nd		Start
 * 	
 * 	
 */



#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "burst_trie.h"

#define IN_TXN_READ		1
#define IN_TXN_WRITE	2
#define IN_TXN_RW		3
#define	NO_GET			4
#define	DEAD_LOCK		8

const long time_limit = 80000000;

pthread_mutex_t DBLINK_LOCK = PTHREAD_MUTEX_INITIALIZER;

typedef enum {
		GET,
		INSERT,
		DELETE
} OpType;

typedef struct OpLink {
		OpType type;
		TrieRecord *rec;
		Key	 key;
		struct OpLink *next;
} OpLink;

typedef struct {
		BurstTrie *dbp;
		pthread_rwlock_t *lock;
		int txnInfo;
		Key lastKey;
		TrieCursor cursor;
		OpLink *opLink;
} IDXState;
	
typedef struct TxnLink {
		IDXState *idx;
		struct TxnLink *next;
} TxnLink;
 
typedef struct TXNState {
		TxnLink *txnLink;
} TXNState;

typedef struct DBLink {
        char *name;
        BurstTrie *dbp;
        pthread_rwlock_t lock;
        struct DBLink *link;
} DBLink;

DBLink *dbLookup = NULL;

ErrCode readLockDB(IDXState *idxState, TXNState *txnState)
{
	/*can not get the lock, return an error.*/
	struct timespec timeout = {0, 0};
	struct timeval now;
	gettimeofday(&now, NULL);
	
	timeout.tv_sec = now.tv_sec;
	timeout.tv_nsec = now.tv_usec*1000 + time_limit;
	if (timeout.tv_nsec > 1000000000) {
		timeout.tv_sec ++;
		timeout.tv_nsec -= 1000000000;
	}

//	timeout.tv_sec = now.tv_sec + 1;
//	timeout.tv_nsec = now.tv_usec*1000;
	if (pthread_rwlock_timedrdlock(idxState->lock, &timeout) != 0)
		return DEADLOCK;
	
	/*get the lock now!*/
	TxnLink *link = malloc(sizeof(TxnLink));
	link->idx = idxState;
	link->next = txnState->txnLink;
	txnState->txnLink = link;
	
	idxState->txnInfo |= IN_TXN_READ;
	idxState->txnInfo |= NO_GET;

	return SUCCESS;
}


ErrCode writeLockDB(IDXState *idxState, TXNState *txnState) 
{
	if ((idxState->txnInfo & IN_TXN_READ) != 0) {
		//Already got the read lock,
		//realse it and try to get the write lock.
		pthread_rwlock_unlock(idxState->lock);

		if (pthread_rwlock_trywrlock(idxState->lock) != 0)
			return DEADLOCK;
	}
	else {
		struct timespec timeout = {0, 0};
		struct timeval now;
		gettimeofday(&now, NULL);

		timeout.tv_sec = now.tv_sec;
		timeout.tv_nsec = now.tv_usec*1000 + time_limit;
		if (timeout.tv_nsec > 1000000000) {
			timeout.tv_sec ++;
			timeout.tv_nsec -= 1000000000;
		}

		//timeout.tv_sec = now.tv_sec + 1;
		//timeout.tv_nsec = now.tv_usec*1000;

		if (pthread_rwlock_timedwrlock(idxState->lock, &timeout) != 0)
			return DEADLOCK;
	
		idxState->txnInfo |= NO_GET;
		/*get the lock now!*/
		TxnLink *link = malloc(sizeof(TxnLink));
		link->idx = idxState;
		link->next = txnState->txnLink;
		txnState->txnLink = link;
	}


	idxState->txnInfo |= IN_TXN_RW;

	return SUCCESS;
}


ErrCode create(KeyType type, char *name)
{
    BurstTrie *dbp = NULL;
    int ret;
    //lock the dblink system
    if ((ret = pthread_mutex_lock(&DBLINK_LOCK)) != 0) {
        printf("can't acquire mutex lock: %d\n", ret);
    }
    
    //make sure that the name specified is not already in use
    DBLink *link = dbLookup;
    while (link != NULL) {
        if (strcmp(name, link->name) == 0) 
            break;
		else 
            link = link->link;
        
    }
    
    if (link != NULL) {
        pthread_mutex_unlock(&DBLINK_LOCK);
        return DB_EXISTS;
    }
    
    /* Initialize the DB handle */
    if (createBurstTrie(&dbp, type) != BT_SUCCESS) {
        pthread_mutex_unlock(&DBLINK_LOCK);
        return FAILURE;
    }
    
    //make a new link object
    DBLink *newLink = (DBLink*)malloc(sizeof(DBLink));
    memset(newLink, 0, sizeof(DBLink));
    
    //populate it
	newLink->name = name;
    newLink->dbp = dbp;
    newLink->link = NULL;
    pthread_rwlock_init(&(newLink->lock), NULL);
    
	//insert it into the linked list headed by dbLookup
    if (dbLookup == NULL) {
        dbLookup = newLink;
    }
	else {
        DBLink *thisLink = dbLookup;

        while (thisLink->link != NULL) {
            thisLink = thisLink->link;
        }
        thisLink->link = newLink;
    }
    
    //unlock the dblink system, because we're done editing it
    pthread_mutex_unlock(&DBLINK_LOCK);
    
    return SUCCESS;
}

ErrCode openIndex(const char *name, IdxState **idxState)
{
    //lock the dblink system
    if (pthread_mutex_lock(&DBLINK_LOCK) != 0) {
        fprintf(stderr, "can't acquire mutex lock!\n");
    }
    
    //look up the DBLink for the index of that name
    DBLink *link = dbLookup;
    while (link != NULL) {
        if (strcmp(name, link->name) == 0) 
            break;
        else 
            link = link->link;
        
    }
    
    //if no link was found, index was never create()d
    if (link == NULL) {
        pthread_mutex_unlock(&DBLINK_LOCK);
        return DB_DNE;
    }
    
    
    //create a IDXState variable for this thread
    IDXState *state = malloc(sizeof(IDXState));
    memset(state, 0, sizeof(IDXState));
    state->dbp = link->dbp;
    state->lock = &(link->lock);
    memset(&(state->cursor), 0, sizeof(TrieCursor));
    state->txnInfo = 0;
    
    *idxState = (IdxState*)state;
    //unlock the dblink system
    pthread_mutex_unlock(&DBLINK_LOCK);
    
    return SUCCESS;
}

ErrCode closeIndex(IdxState *ident)
{
	IDXState *state = (IDXState*)ident;
    free(state);

    return SUCCESS;    
}



ErrCode beginTransaction(TxnState **txn) 
{	
	TXNState *state = (TXNState*)malloc(sizeof(TXNState));
	state->txnLink = NULL;
	
	*txn = (TxnState*)state;

	return SUCCESS;
}


ErrCode abortTransaction(TxnState *txn) 
{
	TXNState *txnState = (TXNState*)txn;
	
	if (txnState == NULL) {
		fprintf(stderr, "cannot abort transaction because none has begun\n");
        return TXN_DNE;
	}
	
	TxnLink *tmp, *txnLink = txnState->txnLink;
	
	while (txnLink != NULL) {
		IDXState *idxState = txnLink->idx;
		OpLink *tLink, *link = idxState->opLink;
		
		while (link) {
			tLink = link;
			link = link->next;
			
			BurstTrie *dbp = idxState->dbp;
			TrieRecord *del = NULL, *rec = tLink->rec;
		//Role back!	
			switch (tLink->type) {				
				case INSERT:
					if (deleteBurstTrie(dbp, &(tLink->key), rec->payload, &del) != BT_SUCCESS) 
						goto abortErr; 
					free(rec);
					freeRecordLink(del);
					break;
				case DELETE:	
					while (rec->next != NULL) {
						if (insertBurstTrie(dbp, &(tLink->key), &(rec->payload)) != BT_SUCCESS) 
							goto abortErr; 
						TrieRecord *tmpRec = rec;
						rec = rec->next;
						free(tmpRec);
					}

				default:	break;
			}

			free(tLink);
		}

		if ((idxState->txnInfo & DEAD_LOCK) == 0) 
			pthread_rwlock_unlock(idxState->lock);
		
		idxState->txnInfo = 0;
		idxState->opLink = NULL;
		tmp = txnLink;
		txnLink = txnLink->next;
		
		free(tmp);
	}
	
	free(txnState);
	
	return SUCCESS;
	
abortErr:
	
	return FAILURE;
} 


ErrCode commitTransaction(TxnState *txn)
{
	TXNState *txnState = (TXNState*)txn;
	
	if (txnState == NULL) {
		fprintf(stderr, "cannot abort transaction because none has begun\n");
        return TXN_DNE;
	}
	
	TxnLink *tmp, *txnLink = txnState->txnLink;
	
	while (txnLink != NULL) {
		IDXState *idxState = txnLink->idx;
		OpLink *tLink, *link = idxState->opLink;
		
		while (link) {
			tLink = link;
			link = link->next;
			if (tLink->rec) {
				switch (tLink->type) {
					case DELETE:
						freeRecordLink(tLink->rec);break;
					case INSERT:
						free(tLink->rec);break;
				}
			}
			free(tLink);
		}

		idxState->txnInfo = 0;
		idxState->opLink = NULL;
		memset(&(idxState->lastKey), 0, sizeof(Key));
		
		pthread_rwlock_unlock(idxState->lock);
		
		tmp = txnLink;
		txnLink = txnLink->next;
		
		free(tmp);
	}
	
	free(txnState);
	
	return SUCCESS;
	
}


ErrCode get(IdxState *ident, TxnState *txn, Record *record)
{
	ErrCode ret;
	IDXState *idxState = (IDXState*)ident;
	BurstTrie *dbp;
	
	if (idxState == NULL || (dbp = idxState->dbp) == NULL) {
		perror("the index is NULL!\n");
		return FAILURE;
	}
	
	
	TXNState *txnState = (TXNState*)txn;
	TrieCursor *cursor = &(idxState->cursor);

	//the get operation is in a transaction.
	if (txnState != NULL) {
		if ((idxState->txnInfo & IN_TXN_READ) != 0 || readLockDB(idxState, txnState) == 0)  {
			
			memcpy(&(idxState->lastKey), &(record->key), sizeof(Key));
			if (getCursor(dbp, cursor, &(record->key)) != BT_SUCCESS) {
				if (idxState->dbp->root->size == 0)
					idxState->txnInfo |= NO_GET;
				idxState->txnInfo &= (~NO_GET); 
				return  KEY_NOTFOUND;
			}
			else {
				idxState->txnInfo &= (~NO_GET);
				strcpy(record->payload, cursor->record->payload);
				cursor->record = cursor->record->next;

				return SUCCESS;
			}
		}
		else {
			idxState->txnInfo |= DEAD_LOCK;
			return DEADLOCK;
		}
	}
	else if (txnState == NULL) { //out of a transaction.
		if (pthread_rwlock_rdlock(idxState->lock) == 0) {
			
			if (getCursor(dbp, cursor, &(record->key)) != BT_SUCCESS) {
				ret = KEY_NOTFOUND;
			}
			else {
				strcpy(record->payload, cursor->record->payload);
				ret = SUCCESS;
			}
			
			pthread_rwlock_unlock(idxState->lock);
			
			return ret;
		}
		else {
			return FAILURE;
		}
	}
	else {
		perror("send a undefined TxnState pointer!\n");
	}
	
	return FAILURE;
}


ErrCode getNext(IdxState *idxState, TxnState *txn, Record *record)
{
	ErrCode ret;
	IDXState *state = (IDXState*)idxState;
	BurstTrie *dbp;

	if (idxState == NULL || (dbp = state->dbp) == NULL) {
		perror("the index is NULL!\n");
		return FAILURE;
	}
	
	TXNState *txnState = (TXNState*)txn;
	TrieCursor *cursor = &(state->cursor);
	//the getNext operation is in a transaction.
	if (txnState != NULL) {
		if ((state->txnInfo & IN_TXN_READ) != 0 || readLockDB(state, txnState) == 0)  {
				
			TrieRecord *p = cursor->record;
			if ((state->txnInfo & NO_GET) == 0 && p != NULL) {
				memcpy(&(record->key), &(state->lastKey), sizeof(Key));
				memcpy(record->payload, p->payload, MAX_PAYLOAD_LEN);
				cursor->record = cursor->record->next;
					
				ret = SUCCESS;
			
			}
			else {
				//Need to scan the index
				cursor->record = NULL;
				if ((state->txnInfo & NO_GET) != 0) {
					cursor->trie = state->dbp->root;
					cursor->pos = -1;
				}
				if (getNextCursor(dbp, cursor, &(record->key)) == BT_SUCCESS) {
					memcpy(&(state->lastKey), &(record->key), sizeof(Key));
					strcpy(record->payload, cursor->record->payload);
					
					cursor->record = cursor->record->next;
					state->txnInfo &= (~NO_GET);
					ret = SUCCESS;
				}
				else {
					state->txnInfo |= NO_GET;
					ret = DB_END;
				}
			}
			
			return ret;
		}
		else {
			state->txnInfo |= DEAD_LOCK;
			return DEADLOCK;
		}
	}
	else if (txnState == NULL) {//out of a transaction.
		if(pthread_rwlock_rdlock(state->lock) == 0) {
			cursor->trie = dbp->root;
			cursor->pos = -1;
			cursor->record = NULL;

			if (getNextCursor(dbp, cursor, &(record->key)) != BT_SUCCESS) {
				ret = KEY_NOTFOUND;
			}
			else {
				strcpy(record->payload, cursor->record->payload);
				ret = SUCCESS;
			}
			
			pthread_rwlock_unlock(state->lock);
			
			return ret;
		}
		else {
			return FAILURE;
		}
	}
	else {
		perror("send a undefined TxnState pointer!\n");
	}
	
	return FAILURE;
	
}


ErrCode insertRecord(IdxState *ident, TxnState *txn, Key *k, const char* payload)
{
	IDXState *idxState = (IDXState*)ident;
	BurstTrie *dbp;
	char *str = (char*)payload;
	int ret;
	
	if (idxState == NULL || (dbp = idxState->dbp) == NULL) {
		perror("the index is NULL!\n");
		return FAILURE;
	}
	
	TXNState *txnState = (TXNState*)txn;
	TrieCursor *cursor = &(idxState->cursor);
	//the insert operation is in a transaction.
	if (txnState != NULL) {
		if ((idxState->txnInfo & IN_TXN_WRITE) != 0 || writeLockDB(idxState, txnState) == 0)  {
				
			if (insertBurstTrie(dbp, k, &str) == BT_SUCCESS) {
				//Update the cursor!
				if ((idxState->txnInfo & NO_GET) == 0) {
					TrieRecord *currentRecord = cursor->record;
					getCursor(dbp, cursor, &(idxState->lastKey));
					cursor->record = currentRecord;
				}
				//add a new transaction entry.
				OpLink *op = (OpLink*)malloc(sizeof(OpLink));
				op->rec = malloc(sizeof(TrieRecord));
				op->rec->payload = str;
				op->rec->next = NULL;
				op->type = INSERT;
				memcpy(&(op->key), k, sizeof(Key));
				op->next = idxState->opLink;
				idxState->opLink = op;
				
				return SUCCESS;
			}
			else {
				return ENTRY_EXISTS;
			}
		}
		else {
			idxState->txnInfo |= DEAD_LOCK;
			return DEADLOCK;
		}
	}
	else if (txnState == NULL) {
		if (pthread_rwlock_wrlock(idxState->lock) == 0) {
			
			if (insertBurstTrie(dbp, k, &str) == 0) 
				ret = SUCCESS;
			
			else 
				ret = ENTRY_EXISTS;

			pthread_rwlock_unlock(idxState->lock);
			
			return ret;
		}
		else {
			return FAILURE;
		}
	}
	else {
		perror("send a undefined TxnState pointer!\n");
	}

	return FAILURE;
}


ErrCode deleteRecord(IdxState *ident, TxnState *txn, Record *theRecord)
{
	IDXState *idxState = (IDXState*)ident;
	BurstTrie *dbp;
	TrieRecord *del = NULL;
	char *str;
	int ret; 
		
	if (idxState == NULL || (dbp = idxState->dbp) == NULL) {
		perror("the index is NULL!\n");
		return FAILURE;
	}

	if (theRecord->payload[0] == '\0')
		str = NULL;
	else 
		str = &(theRecord->payload[0]);
	
		
	TXNState *txnState = (TXNState*)txn;
	TrieCursor *cursor = &(idxState->cursor);
	//the getNext operation is in a transaction.
	if (txnState != NULL) {

		if ((idxState->txnInfo & IN_TXN_WRITE) != 0 || writeLockDB(idxState, txnState) == 0) {
			TrieRecord *nextRecord = NULL;
			
			if ((idxState->txnInfo & NO_GET) == 0 && cursor->record) 
				nextRecord = cursor->record->next;

			if (deleteBurstTrie(dbp, &(theRecord->key), str, &del) == BT_SUCCESS) {
				//Update the cursor!
				if ((idxState->txnInfo & NO_GET) == 0) {
					TrieRecord *currentRecord = cursor->record;
					if (getCursor(dbp, cursor, &(idxState->lastKey)) != BT_SUCCESS) {
						cursor->record = currentRecord;
					}
					else {
						int64_t cmp = 0;

						switch (theRecord->key.type) {
							case SHORT:
								cmp = idxState->lastKey.keyval.shortkey -
										theRecord->key.keyval.shortkey;
								break;
							case INT:
								cmp = idxState->lastKey.keyval.intkey -
										theRecord->key.keyval.intkey;
								break;
							case VARCHAR:
								cmp = strcmp(idxState->lastKey.keyval.charkey,
										theRecord->key.keyval.charkey);
								break;
						}
						if (cmp != 0) {
							cursor->record = currentRecord;
						}
						else {
							if (currentRecord == del && str != 0)
								cursor->record = nextRecord;
							else if (str == 0)
								cursor->record = NULL;
							else
								cursor->record = currentRecord;
						}
					}
				}
			}
			else { 
				return KEY_NOTFOUND;
			}
			
			//add a new transaction entry.
			OpLink *op = (OpLink*)malloc(sizeof(OpLink));
			op->rec = del;
			op->type = DELETE;
			memcpy(&(op->key), &(theRecord->key), sizeof(Key));
			op->next = idxState->opLink;
			idxState->opLink = op;
				
			return SUCCESS;
			
		}
		else {
			idxState->txnInfo |= DEAD_LOCK;
			return DEADLOCK;
		}
	}
	else if (txnState == NULL) {
		if (pthread_rwlock_wrlock(idxState->lock) == 0) {
			
			if (deleteBurstTrie(dbp, &(theRecord->key), str, &del) == 0) {
				freeRecordLink(del);
				ret = SUCCESS;
			}
			else {
				ret = KEY_NOTFOUND;
			}
			
			pthread_rwlock_unlock(idxState->lock);
			
			return ret;
		}
		else {
			return FAILURE;
		}
	}
	else {
		perror("send a undefined TxnState pointer!\n");
	}
	
	return FAILURE;
}
