#include "stdinc.h"
#include "dat.h"
#include "fns.h"
#include "whack.h"

/*
 * Write a lump to disk.  Updates ia with an index address
 * for the newly-written lump.  Upon return, the lump will
 * have been placed in the disk cache but will likely not be on disk yet.
 */
int
storeclump(Index *ix, ZBlock *zb, u8int *sc, int type, u32int creator, IAddr *ia)
{
	ZBlock *cb;
	Clump cl;
	u64int a;
	u8int bh[VtScoreSize];
	int size, dsize;

	trace(TraceLump, "storeclump enter", sc, type);
	size = zb->len;
	if(size > VtMaxLumpSize){
		seterr(EStrange, "lump too large");
		return -1;
	}
	if(vttypevalid(type) < 0){
		seterr(EStrange, "invalid lump type");
		return -1;
	}

	if(0){
		scoremem(bh, zb->data, size);
		if(scorecmp(sc, bh) != 0){
			seterr(ECorrupt, "storing clump: corrupted; expected=%V got=%V, size=%d", sc, bh, size);
			return -1;
		}
	}

	cb = alloczblock(size + ClumpSize + U32Size, 0, 0);
	if(cb == nil)
		return -1;

	cl.info.type = type;
	cl.info.uncsize = size;
	cl.creator = creator;
	cl.time = now();
	scorecp(cl.info.score, sc);

	trace(TraceLump, "storeclump whackblock");
	dsize = whackblock(&cb->data[ClumpSize], zb->data, size);
	if(dsize > 0 && dsize < size){
		cl.encoding = ClumpECompress;
	}else{
		if(dsize > size){
			fprint(2, "whack error: dsize=%d size=%d\n", dsize, size);
			abort();
		}
		cl.encoding = ClumpENone;
		dsize = size;
		memmove(&cb->data[ClumpSize], zb->data, size);
	}
	memset(cb->data+ClumpSize+dsize, 0, 4);
	cl.info.size = dsize;

	a = writeiclump(ix, &cl, cb->data);
	trace(TraceLump, "storeclump exit %lld", a);
	freezblock(cb);
	if(a == TWID64)
		return -1;

	ia->addr = a;
	ia->type = type;
	ia->size = size;
	ia->blocks = (dsize + ClumpSize + (1 << ABlockLog) - 1) >> ABlockLog;

/*
	qlock(&stats.lock);
	stats.clumpwrites++;
	stats.clumpbwrites += size;
	stats.clumpbcomp += dsize;
	qunlock(&stats.lock);
*/

	return 0;
}

u32int
clumpmagic(Arena *arena, u64int aa)
{
	u8int buf[U32Size];

	if(readarena(arena, aa, buf, U32Size) == TWID32)
		return TWID32;
	return unpackmagic(buf);
}

/*
 * fetch a block based at addr.
 * score is filled in with the block's score.
 * blocks is roughly the length of the clump on disk;
 * if zero, the length is unknown.
 */
ZBlock*
loadclump(Arena *arena, u64int aa, int blocks, Clump *cl, u8int *score, int verify)
{
	Unwhack uw;
	ZBlock *zb, *cb;
	u8int bh[VtScoreSize], *buf;
	u32int n;
	int nunc;

/*
	qlock(&stats.lock);
	stats.clumpreads++;
	qunlock(&stats.lock);
*/

	if(blocks <= 0)
		blocks = 1;

	trace(TraceLump, "loadclump enter");

	cb = alloczblock(blocks << ABlockLog, 0, 0);
	if(cb == nil)
		return nil;
	n = readarena(arena, aa, cb->data, blocks << ABlockLog);
	if(n < ClumpSize){
		if(n != 0)
			seterr(ECorrupt, "loadclump read less than a header");
		freezblock(cb);
		return nil;
	}
	trace(TraceLump, "loadclump unpack");
	if(unpackclump(cl, cb->data, arena->clumpmagic) < 0){
		seterr(ECorrupt, "loadclump %s %llud: %r", arena->name, aa);
		freezblock(cb);
		return nil;
	}
	if(cl->info.type == VtCorruptType){
		seterr(EOk, "clump is marked corrupt");
		freezblock(cb);
		return nil;
	}
	n -= ClumpSize;
	if(n < cl->info.size){
		freezblock(cb);
		n = cl->info.size;
		cb = alloczblock(n, 0, 0);
		if(cb == nil)
			return nil;
		if(readarena(arena, aa + ClumpSize, cb->data, n) != n){
			seterr(ECorrupt, "loadclump read too little data");
			freezblock(cb);
			return nil;
		}
		buf = cb->data;
	}else
		buf = cb->data + ClumpSize;

	scorecp(score, cl->info.score);

	zb = alloczblock(cl->info.uncsize, 0, 0);
	if(zb == nil){
		freezblock(cb);
		return nil;
	}
	switch(cl->encoding){
	case ClumpECompress:
		trace(TraceLump, "loadclump decompress");
		unwhackinit(&uw);
		nunc = unwhack(&uw, zb->data, cl->info.uncsize, buf, cl->info.size);
		if(nunc != cl->info.uncsize){
			if(nunc < 0)
				seterr(ECorrupt, "decompression of %llud failed: %s", aa, uw.err);
			else
				seterr(ECorrupt, "decompression of %llud gave partial block: %d/%d\n", aa, nunc, cl->info.uncsize);
			freezblock(cb);
			freezblock(zb);
			return nil;
		}
		break;
	case ClumpENone:
		if(cl->info.size != cl->info.uncsize){
			seterr(ECorrupt, "loading clump: bad uncompressed size for uncompressed block %llud", aa);
			freezblock(cb);
			freezblock(zb);
			return nil;
		}
		scoremem(bh, buf, cl->info.uncsize);
		if(scorecmp(cl->info.score, bh) != 0)
			seterr(ECorrupt, "pre-copy sha1 wrong at %s %llud: expected=%V got=%V", arena->name, aa, cl->info.score, bh);
		memmove(zb->data, buf, cl->info.uncsize);
		break;
	default:
		seterr(ECorrupt, "unknown encoding in loadlump %llud", aa);
		freezblock(cb);
		freezblock(zb);
		return nil;
	}
	freezblock(cb);

	if(verify){
		trace(TraceLump, "loadclump verify");
		scoremem(bh, zb->data, cl->info.uncsize);
		if(scorecmp(cl->info.score, bh) != 0){
			seterr(ECorrupt, "loading clump: corrupted at %s %llud; expected=%V got=%V", arena->name, aa, cl->info.score, bh);
			freezblock(zb);
			return nil;
		}
		if(vttypevalid(cl->info.type) < 0){
			seterr(ECorrupt, "loading lump at %s %llud: invalid lump type %d", arena->name, aa, cl->info.type);
			freezblock(zb);
			return nil;
		}
	}

	trace(TraceLump, "loadclump exit");
/*
	qlock(&stats.lock);
	stats.clumpbreads += cl->info.size;
	stats.clumpbuncomp += cl->info.uncsize;
	qunlock(&stats.lock);
*/
	return zb;
}

/*
 * write a clump to an available arena in the index
 * and return the address of the clump within the index.
ZZZ question: should this distinguish between an arena
filling up and real errors writing the clump?
 */
u64int
writeiclump(Index *ix, Clump *c, u8int *clbuf)
{
	u64int a;
	int i;
	IAddr ia;
//	AState as;

	ia.addr = 0;
// this should not happen, but it does ZZZ
	unsigned int h= trie_retrieve(c->info.score,&ia.addr);
if(h!=~0) fprint(2,"h: %ux, %llux %V\n", h, ia.addr, c->info.score);
        if(h!=~0) return ia.addr;
	trace(TraceLump, "writeiclump enter");
	qlock(&ix->writing);
	for(i = ix->mapalloc; i < ix->narenas; i++){
		a = writeaclump(ix->arenas[i], c, clbuf);
		if(a != TWID64){
			ix->mapalloc = i;
			ia.addr = ix->amap[i].start + a;
			ia.type = c->info.type;
			ia.size = c->info.uncsize;
			ia.blocks = (c->info.size + ClumpSize + (1<<ABlockLog) - 1) >> ABlockLog;
//			as.arena = ix->arenas[i];
//			as.aa = ia.addr;
//			as.stats = as.arena->memstats;
			trie_insert(c->info.score,&ia.addr);
			qunlock(&ix->writing);
			trace(TraceLump, "writeiclump exit");
			return ia.addr;
		}
	}
	qunlock(&ix->writing);

	seterr(EAdmin, "no space left in arenas");
	trace(TraceLump, "writeiclump failed");
	return TWID64;
}

/*
 * convert an arena index to an relative arena address
 */
Arena*
amapitoa(Index *ix, u64int a, u64int *aa)
{
#ifdef XXX
	*aa = a & 0xFFFFFFFFULL;
	return ix->arenas[(int)(a>>48)];
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
	assert(*aa < 0x1ULL<<48 );
	return ix->arenas[l];
#endif
}

int
loadclumpinfo(uvlong addr, ClumpInfo *ci)
{
	Arena *arena;
	u64int aa;
	unsigned char buf[ClumpInfoSize];
	arena = amapitoa(mainindex, addr, &aa);
	if(arena!=nil) {
		readarena(arena,aa+4,buf,ClumpInfoSize);
		unpackclumpinfo(ci, buf);
		return 0;
	}
	return -1;
}

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
	else {
		trace(TraceLump, "loadientry notfound");
		addstat(StatBloomFalseMiss, 1);
	}
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

// hic sunt leones
// these are defined here to avoid pulling bitrot from libvs.a
int icacheprefetch = 1;

void emptyicache(void) { }
void icacheclean(IEntry *x) { USED(x); }
void initicache(u32int x) { USED(x); }

/* cannot be removed, bec. hdisk.c */
int
icachelookup(u8int score[VtScoreSize], int type, IAddr *ia)
{
	return lookupscore(score,type,ia);
}

u32int
hashbits(u8int *sc, int bits)
{
        u32int v;
                
        v = (sc[0] << 24) | (sc[1] << 16) | (sc[2] << 8) | sc[3];
        if(bits < 32)
                 v >>= (32 - bits);
        return v;
}

ulong icachedirtyfrac(void) { return 500000; }

void initicachewrite(void) {}

int icachesleeptime = 1000;

void		flushicache(void) {}

void		kickicache(void) {}

int minicachesleeptime = 0;

void		delaykickicache(void) {}

// from lumpqueue.c
int
queuewrite(Lump *u, Packet *p, int creator, uint ms) {return 0;}

