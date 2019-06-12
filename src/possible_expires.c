#include "possible_expires.h"

extern dictType keylistDictType;

typedef struct redisPossibleExpires {
	dict *possible_expires;			/*key is timeout in seconds, value is expire key*/
	list* cur_possible_expires;		/*current process expires*/
	long long cur_expire_sec;		/*the current processing expire time in second*/
	robj* cur_expire_sec_obj;		/*object presentation of cur_expire_sec*/
} redisPossibleExpires;

static redisPossibleExpires* possibleExpires;
 
void initPossibleExpires( int dbnum ) {
	int i;
	possibleExpires = zmalloc( sizeof( redisPossibleExpires ) * dbnum );
	for( i = 0; i < dbnum; i++ ) {
		possibleExpires[i].possible_expires = dictCreate(&keylistDictType, NULL);
		possibleExpires[i].cur_expire_sec = mstime() / 1000 - 10;
		possibleExpires[i].cur_expire_sec_obj = createObject( OBJ_STRING, sdsfromlonglong( possibleExpires[i].cur_expire_sec ) );; 
		possibleExpires[i].cur_possible_expires = NULL;
	}
}

void addPossibleExpire( int db, robj* key, long long timeout ) {
	robj *tObj = createObject( OBJ_STRING, sdsfromlonglong( timeout ) );	
	list* expireKeys = dictFetchValue( possibleExpires[db].possible_expires, tObj );
	if( expireKeys ) {
		decrRefCount( tObj );
	} else {
		expireKeys = listCreate();
		listSetFreeMethod( expireKeys, decrRefCount );
		dictAdd(possibleExpires[db].possible_expires, tObj, expireKeys );
	}
	incrRefCount( key );
	listAddNodeTail( expireKeys, key );
}


dictEntry* getPossibleExpireKey( int db, dict* expires ) {
	dictEntry* entry = NULL;
	long long last_sec = mstime() / 1000 - 10;
		
	while( possibleExpires[db].cur_possible_expires == NULL ) {				
		if( possibleExpires[db].cur_expire_sec >= last_sec ) {
			return NULL;
		}		
		decrRefCount( possibleExpires[db].cur_expire_sec_obj );
		possibleExpires[db].cur_expire_sec ++;
		possibleExpires[db].cur_expire_sec_obj = createObject( OBJ_STRING, sdsfromlonglong( possibleExpires[db].cur_expire_sec ) );
		possibleExpires[db].cur_possible_expires = dictFetchValue( possibleExpires[db].possible_expires, possibleExpires[db].cur_expire_sec_obj );
	}
	
	while( listLength(possibleExpires[db].cur_possible_expires) > 0 ) {
		listNode* first = listFirst( possibleExpires[db].cur_possible_expires );
		robj* keyobj = listNodeValue( first );
		entry = dictFind( expires, keyobj->ptr );
		listDelNode( possibleExpires[db].cur_possible_expires, first );
		if( entry ) break;

	}
	if( listLength(possibleExpires[db].cur_possible_expires)<= 0 ) {
		dictDelete( possibleExpires[db].possible_expires, possibleExpires[db].cur_expire_sec_obj );
		possibleExpires[db].cur_possible_expires = NULL;
	}
	return entry;
	
}

void emptyPossibleExpire( int db ) {
	if( possibleExpires[db].possible_expires ) {
		dictEmpty( possibleExpires[db].possible_expires, NULL );
	}	
}

//only be called in the slave
void removePossibleKeys( int db ) {
	long long last_sec = mstime() / 1000 - 15;
	if( possibleExpires[db].cur_expire_sec  < last_sec) {
		decrRefCount( possibleExpires[db].cur_expire_sec_obj );
		possibleExpires[db].cur_expire_sec ++;
		possibleExpires[db].cur_expire_sec_obj = createObject( OBJ_STRING, sdsfromlonglong( possibleExpires[db].cur_expire_sec ) );
		possibleExpires[db].cur_possible_expires = dictFetchValue( possibleExpires[db].possible_expires, possibleExpires[db].cur_expire_sec_obj );
		if( possibleExpires[db].cur_possible_expires ) {
			dictDelete( possibleExpires[db].possible_expires, possibleExpires[db].cur_expire_sec_obj );
			possibleExpires[db].cur_possible_expires = NULL;
		}
	}
}

