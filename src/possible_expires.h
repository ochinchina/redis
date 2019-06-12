#ifndef _POSSIBLE_EXPIRES_H
#define _POSSIBLE_EXPIRES_H
#include "server.h"

void initPossibleExpires(int dbnum);
void addPossibleExpire( int db, robj* key, long long timeout );
dictEntry* getPossibleExpireKey( int db, dict* expires );
void emptyPossibleExpire( int db );
void removePossibleKeys( int db );


#endif/*_POSSIBLE_EXPIRES_H*/

