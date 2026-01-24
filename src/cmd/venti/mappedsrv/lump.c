#include "stdinc.h"
#include "dat.h"
#include "fns.h"

void trie_init(void);
unsigned int trie_insert(unsigned char *, uvlong*); 
unsigned int trie_retrieve(unsigned char *, uvlong*); 

int			bootstrap = 0;
int			syncwrites = 0;
int			writestodevnull = 0;
int			verifywrites = 0;

static Packet		*readilump(Lump *u, IAddr *ia, u8int *score);

/*
 * lookup the score in the partition
 */
int
loadientry(Index *ix, u8int *score, int type, IEntry *ie)
{
	unsigned int h;
	int ok;
	ClumpInfo ci;

	ok = -1;

	trace(TraceLump, "loadientry enter");

	h = trie_retrieve(score,&ie->ia.addr);
	if( h != ~0) ok = 0; 
	else
		trace(TraceLump, "loadientry notfound");
	if(ok==0) {
		if( loadclumpinfo(ie->ia.addr, &ci) == 0) {
			memcpy(ie->score,score,VtScoreSize);
			ie->ia.type = ci.type;
			ie->ia.size = ci.uncsize;
			ie->ia.blocks = (ci.size + ClumpSize + (1<<ABlockLog)-1) >> ABlockLog;
		} else ok = -1;
	}
	trace(TraceLump, "loadientry exit");
	return ok;
}

int
loadclumpinfo(uvlong addr, ClumpInfo *ci)
{
	Arena *arena;
	u64int aa;
	u64int bb;
	arena = amapitoa(mainindex, addr, &aa);
	bb = arena->base + aa;
	unpackclumpinfo(ci, arena->part->mapped+bb+4);
	return 0;
}

int
insertscore(u8int score[VtScoreSize], IAddr *ia, int state, AState *as)
{
	return trie_insert(score,&ia->addr);;
}

int
lookupscore(u8int score[VtScoreSize], int type, IAddr *ia)
{
// cannot use trie_retrieve, because we need ia.type
	IEntry ie;
	int ret = loadientry(mainindex, score, type, &ie);
	*ia = ie.ia;
	if(ret == -1 || ie.ia.type != type) return -1;
	return 0;
}

/*
 * Some of this logic is duplicated in hdisk.c
 */
Packet*
readlump(u8int *score, int type, u32int size, int *cached)
{
	Lump *u;
	Packet *p;
	IAddr ia;
	u32int n;

	trace(TraceLump, "readlump enter");
/*
	qlock(&stats.lock);
	stats.lumpreads++;
	qunlock(&stats.lock);
*/
	if(scorecmp(score, zeroscore) == 0)
		return packetalloc();
	u = lookuplump(score, type);
	if(u->data != nil){
		trace(TraceLump, "readlump lookuplump hit");
		if(cached)
			*cached = 1;
		n = packetsize(u->data);
		if(n > size){
			seterr(EOk, "read too small: asked for %d need at least %d", size, n);
			putlump(u);

			return nil;
		}
		p = packetdup(u->data, 0, n);
		putlump(u);
		return p;
	}

	if(cached)
		*cached = 0;

	if(lookupscore(score, type, &ia) < 0){
		/* ZZZ place to check for someone trying to guess scores */
		seterr(EOk, "no block with score %V/%d exists", score, type);

		putlump(u);
		return nil;
	}
	if(ia.size > size){
		seterr(EOk, "read too small 1: asked for %d need at least %d", size, ia.size);

		putlump(u);
		return nil;
	}

	trace(TraceLump, "readlump readilump");
	p = readilump(u, &ia, score);
	putlump(u);

	trace(TraceLump, "readlump exit");
	return p;
}

/*
 * save away a lump, and return it's score.
 * doesn't store duplicates, but checks that the data is really the same.
 */
int
writelump(Packet *p, u8int *score, int type, u32int creator, uint ms)
{
	Lump *u;
	int ok;

/*
	qlock(&stats.lock);
	stats.lumpwrites++;
	qunlock(&stats.lock);
*/

	packetsha1(p, score);
	if(packetsize(p) == 0 || writestodevnull==1){
		packetfree(p);
		return 0;
	}

	u = lookuplump(score, type);
	if(u->data != nil){
		ok = 0;
		if(packetcmp(p, u->data) != 0){
			uchar nscore[VtScoreSize];

			packetsha1(u->data, nscore);
			if(scorecmp(u->score, score) != 0)
				seterr(EStrange, "lookuplump returned bad score %V not %V", u->score, score);
			else if(scorecmp(u->score, nscore) != 0)
				seterr(EStrange, "lookuplump returned bad data %V not %V", nscore, u->score);
			else
				seterr(EStrange, "score collision %V", score);
			ok = -1;
		}
		packetfree(p);
		putlump(u);
		return ok;
	}

	if(writestodevnull==2){
		packetfree(p);
		return 0;
	}

	ok = writeqlump(u, p, creator, ms);

	putlump(u);
	return ok;
}

int
writeqlump(Lump *u, Packet *p, int creator, uint ms)
{
	ZBlock *flat;
	Packet *old;
	IAddr ia;
	int ok;

	if(lookupscore(u->score, u->type, &ia) == 0){
		if(verifywrites == 0){
			/* assume the data is here! */
			packetfree(p);
			ms = msec() - ms;
			addstat2(StatRpcWriteOld, 1, StatRpcWriteOldTime, ms);
			return 0;
		}

		/*
		 * if the read fails,
		 * assume it was corrupted data and store the block again
		 */
		old = readilump(u, &ia, u->score);
fprint(2, "readilump %llux ia %llux score %V\n", old, ia.addr, u->score);
		if(old != nil){
			ok = 0;
			if(packetcmp(p, old) != 0){
				uchar nscore[VtScoreSize];

				packetsha1(old, nscore);
				if(scorecmp(u->score, nscore) != 0)
					seterr(EStrange, "readilump returned bad data %V not %V", nscore, u->score);
				else
					seterr(EStrange, "score collision %V", u->score);
				ok = -1;
			}
			packetfree(p);
			packetfree(old);

			ms = msec() - ms;
			addstat2(StatRpcWriteOld, 1, StatRpcWriteOldTime, ms);
			return ok;
		}
		logerr(EAdmin, "writelump: read %V failed, rewriting: %r\n", u->score);
	}

	flat = packet2zblock(p, packetsize(p));
	ok = storeclump(mainindex, flat, u->score, u->type, creator, &ia);
	freezblock(flat);
	if(ok == 0)
		insertlump(u, p);
	else
		packetfree(p);

	ms = msec() - ms;
	addstat2(StatRpcWriteNew, 1, StatRpcWriteNewTime, ms);
	return ok;
}

/*
 * convert an arena index to an relative arena address
 */
Arena*
amapitoa(Index *ix, u64int a, u64int *aa)
{
#ifndef DRECK
	int l = a>>48;
	*aa = a & 0xFFFFFFFFFFFFULL;
#else
	int i, r, l, m;

	l = 1;
	r = ix->narenas - 1;
	while(l <= r){
		m = (r + l) / 2;
		if(ix->amap[m].start <= a)
			l = m + 1;
		else
			r = m - 1;
	}
	l--;

	if(a > ix->amap[l].stop){
for(i=0; i<ix->narenas; i++)
	print("arena %d: %llux - %llux\n", i, ix->amap[i].start, ix->amap[i].stop);
print("want arena %d for %llux\n", l, a);
		seterr(ECrash, "unmapped address passed to amapitoa");
		return nil;
	}

	if(ix->arenas[l] == nil){
		seterr(ECrash, "unmapped arena selected in amapitoa");
		return nil;
	}
	*aa = a - ix->amap[l].start;
//	debugarena = l;
#endif
	return ix->arenas[l];
}

static Packet*
readilump(Lump *u, IAddr *ia, u8int *score)
{
	Arena *arena;
	ZBlock *zb;
	Packet *p, *pp;
	Clump cl;
	u64int aa;
	u8int sc[VtScoreSize];

	trace(TraceLump, "readilump enter");
	arena = amapitoa(mainindex, ia->addr, &aa);
	if(arena == nil){
		trace(TraceLump, "readilump amapitoa failed");
		return nil;
	}

	trace(TraceLump, "readilump loadclump");
	zb = loadclump(arena, aa, ia->blocks, &cl, sc, 0);
	if(zb == nil){
		trace(TraceLump, "readilump loadclump failed");
		return nil;
	}

	if(ia->size != cl.info.uncsize){
		seterr(EInconsist, "index and clump size mismatch");
		freezblock(zb);
		return nil;
	}
	if(ia->type != cl.info.type){
		seterr(EInconsist, "index and clump type mismatch");
		freezblock(zb);
		return nil;
	}
	if(scorecmp(score, sc) != 0){
		seterr(ECrash, "score mismatch");
		freezblock(zb);
		return nil;
	}

	trace(TraceLump, "readilump success");
	p = zblock2packet(zb, cl.info.uncsize);
	freezblock(zb);
	pp = packetdup(p, 0, packetsize(p));
	trace(TraceLump, "readilump insertlump");
	insertlump(u, pp);
	trace(TraceLump, "readilump exit");
	return p;
}
