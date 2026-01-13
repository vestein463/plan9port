#include "stdinc.h"
#include "dat.h"
#include "fns.h"
#include <sys/mman.h>

/* this version of arena only works when the arena partition is mmap'ed */
typedef struct ASum ASum;

struct ASum
{
	Arena	*arena;
	ASum	*next;
};

static void	sealarena(Arena *arena);
static int	okarena(Arena *arena);
static int	loadarena(Arena *arena);
static CIBlock	*getcib(Arena *arena, int clump, int writing, CIBlock *rock);
static void	putcib(Arena *arena, CIBlock *cib);
static void	sumproc(void *);

static QLock	sumlock;
static Rendez	sumwait;
static ASum	*sumq;
static ASum	*sumqtail;

int	arenasumsleeptime;
static uchar zero[8192];

int
initarenasum(void)
{
	needzeroscore();  /* OS X */

	qlock(&sumlock);
	sumwait.l = &sumlock;
	qunlock(&sumlock);

	if(vtproc(sumproc, nil) < 0){
		seterr(EOk, "can't start arena checksum slave: %r");
		return -1;
	}
	return 0;
}

/*
 * make an Arena, and initialize it based upon the disk header and trailer.
 */
Arena*
initarena(Part *part, u64int base, u64int size, u32int blocksize)
{
	Arena *arena;

	arena = MKZ(Arena);
	arena->part = part;
	arena->blocksize = blocksize;
	arena->clumpmax = arena->blocksize / ClumpInfoSize;
	arena->base = base + blocksize;
	arena->size = size - 2 * blocksize;

	if(loadarena(arena) < 0){
		seterr(ECorrupt, "arena header or trailer corrupted");
		freearena(arena);
		return nil;
	}
	if(okarena(arena) < 0){
		freearena(arena);
		return nil;
	}

	if(arena->diskstats.sealed && scorecmp(zeroscore, arena->score)==0) {
		fprint(2, "should not happen, arena sealed with score zero\n" );
		sealarena(arena);
	}
	if(arena->diskstats.sealed)
		mprotect(arena->part->mapped+arena->base,arena->size,PROT_READ);

	return arena;
}

void
freearena(Arena *arena)
{
	if(arena == nil)
		return;
	free(arena);
}

Arena*
newarena(Part *part, u32int vers, char *name, u64int base, u64int size, u32int blocksize)
{
	int bsize;
	Arena *arena;

	if(nameok(name) < 0){
		seterr(EOk, "illegal arena name", name);
		return nil;
	}
	arena = MKZ(Arena);
	arena->part = part;
	arena->version = vers;
	if(vers == ArenaVersion4)
		arena->clumpmagic = _ClumpMagic;
	else{
		do
			arena->clumpmagic = fastrand();
		while(arena->clumpmagic==_ClumpMagic || arena->clumpmagic==0);
	}
	arena->blocksize = blocksize;
	arena->clumpmax = arena->blocksize / ClumpInfoSize;
	arena->base = base + blocksize;
	arena->size = size - 2 * blocksize;

	namecp(arena->name, name);

	bsize = sizeof zero;
	if(bsize > arena->blocksize)
		bsize = arena->blocksize;

	if(wbarena(arena)<0 || wbarenahead(arena)<0
	|| writepart(arena->part, arena->base, zero, bsize)<0){
		freearena(arena);
		return nil;
	}

	return arena;
}

int
readclumpinfo(Arena *arena, int clump, ClumpInfo *ci)
{
	CIBlock *cib, r;

	cib = getcib(arena, clump, 0, &r);
	if(cib == nil)
		return -1;
	unpackclumpinfo(ci, &cib->data->data[cib->offset]);
	putcib(arena, cib);
	return 0;
}

int
readclumpinfos(Arena *arena, int clump, ClumpInfo *cis, int n)
{
	CIBlock *cib, r;
	int i;

	/*
	 * because the clump blocks are laid out
	 * in reverse order at the end of the arena,
	 * it can be a few percent faster to read
	 * the clumps backwards, which reads the
	 * disk blocks forwards.
	 */
	for(i = n-1; i >= 0; i--){
		cib = getcib(arena, clump + i, 0, &r);
		if(cib == nil){
			n = i;
			continue;
		}
		unpackclumpinfo(&cis[i], &cib->data->data[cib->offset]);
		putcib(arena, cib);
	}
	return n;
}

/*
 * write directory information for one clump
 * must be called the arena locked
 */
int
writeclumpinfo(Arena *arena, int clump, ClumpInfo *ci)
{
	CIBlock *cib, r;

	cib = getcib(arena, clump, 1, &r);
	if(cib == nil)
		return -1;
//	dirtydblock(cib->data, DirtyArenaCib);
	packclumpinfo(ci, &cib->data->data[cib->offset]);
	putcib(arena, cib);
	return 0;
}

u64int
arenadirsize(Arena *arena, u32int clumps)
{
	return ((clumps / arena->clumpmax) + 1) * arena->blocksize;
}

/*
 * read a clump of data
 * n is a hint of the size of the data, not including the header
 * make sure it won't run off the end, then return the number of bytes actually read
 */
u32int
readarena(Arena *arena, u64int aa, u8int *buf, long n)
{
	u64int a;

	if(n == 0)
		return -1;

	qlock(&arena->lock);
	a = arena->size - arenadirsize(arena, arena->memstats.clumps);
	qunlock(&arena->lock);
	if(aa >= a){
		seterr(EOk, "reading beyond arena clump storage: clumps=%d aa=%lld a=%lld -1 clumps=%lld\n", arena->memstats.clumps, aa, a, arena->size - arenadirsize(arena, arena->memstats.clumps - 1));
		return -1;
	}
	if(aa + n > a)
		n = a - aa;
	memmove(buf, arena->part->mapped+arena->base+aa,n);
	return n;
}

/*
 * write some data to the clump section at a given offset
 * used to fix up corrupted arenas.
 */
u32int
writearena(Arena *arena, u64int aa, u8int *clbuf, u32int n)
{
	u64int a;

	if(n == 0)
		return -1;

	qlock(&arena->lock);
	a = arena->size - arenadirsize(arena, arena->memstats.clumps);
	if(aa >= a || aa + n > a){
		qunlock(&arena->lock);
		seterr(EOk, "writing beyond arena clump storage");
		return -1;
	}
	memmove(arena->part->mapped+aa,clbuf, n);

	qunlock(&arena->lock);
	return n;
}

/*
 * allocate space for the clump and write it,
 * updating the arena directory
ZZZ question: should this distinguish between an arena
filling up and real errors writing the clump?
 */
u64int
writeaclump(Arena *arena, Clump *c, u8int *clbuf)
{
	u32int n;
	u64int aa;

	n = c->info.size + ClumpSize + U32Size;
	qlock(&arena->lock);
	aa = arena->memstats.used;
	if(arena->memstats.sealed
	|| aa + n + U32Size + arenadirsize(arena, arena->memstats.clumps + 1) > arena->size){
		if(!arena->memstats.sealed){
			logerr(EOk, "seal memstats %s", arena->name);
			arena->memstats.sealed = 1;
		}
		qunlock(&arena->lock);
		sealarena(arena);
		return TWID64;
	}
	if(packclump(c, &clbuf[0], arena->clumpmagic) < 0){
		qunlock(&arena->lock);
		return TWID64;
	}
	memmove(arena->part->mapped+arena->base+aa,clbuf, n);

	arena->memstats.used += c->info.size + ClumpSize;
	arena->memstats.uncsize += c->info.uncsize;
	if(c->info.size < c->info.uncsize)
		arena->memstats.cclumps++;
	arena->memstats.clumps++;
	if(arena->memstats.clumps == 0)
		sysfatal("clumps wrapped");
	arena->wtime = now();
	if(arena->ctime == 0)
		arena->ctime = arena->wtime;
//	writeclumpinfo(arena, clump, &c->info);
	qunlock(&arena->lock);
	return aa;
}

/*
 * once sealed, an arena never has any data added to it.
 * it should only be changed to fix errors.
 * this also syncs the clump directory.
 */
static void
sealarena(Arena *arena)
{
	arena->inqueue = 1;
	sumarena(arena);
//	backsumarena(arena);
}

void
backsumarena(Arena *arena)
{
	ASum *as;

	as = MK(ASum);
	if(as == nil)
		return;
	qlock(&sumlock);
	as->arena = arena;
	as->next = nil;
	if(sumq)
		sumqtail->next = as;
	else
		sumq = as;
	sumqtail = as;
	/*
	 * Might get here while initializing arenas,
	 * before initarenasum has been called.
	 */
	if(sumwait.l)
		rwakeup(&sumwait);
	qunlock(&sumlock);
}

static void
sumproc(void *unused)
{
	ASum *as;
	Arena *arena;

	USED(unused);

	for(;;){
		qlock(&sumlock);
		while(sumq == nil)
			rsleep(&sumwait);
		as = sumq;
		sumq = as->next;
		qunlock(&sumlock);
		arena = as->arena;
		free(as);
		sumarena(arena);
	}
}

void
sumarena(Arena *arena)
{
	DigestState s;
	u64int a, e;
	u32int bs;
	u8int score[VtScoreSize];

	bs = MaxIoSize;
	if(bs < arena->blocksize)
		bs = arena->blocksize;
	qlock(&arena->lock);
fprint(2, "memstats.used %ulld, diskstats.used %ulld\n", arena->memstats.used, arena->diskstats.used);
	arena->diskstats = arena->memstats;
	qunlock(&arena->lock);
	syncarena(arena, TWID32, 1, 1);
//	msync(arena->part->mapped+arena->base,arena->size,MS_SYNC);

	/*
	 * read & sum all blocks except the last one
	 */
	memset(&s, 0, sizeof s);
	/*
	 * the last block is special, since it may already have the checksum included
	 */
	a = arena->base - arena->blocksize;
	e = arena->size +2*arena->blocksize - VtScoreSize;
	uchar *trailer = arena->part->mapped + arena->base + arena->size;
	packarena(arena, trailer);
fprint(2, "a %llx, e %llx\n", a, e);
	sha1(arena->part->mapped+a, e, nil, &s );
	sha1(zeroscore, VtScoreSize, nil, &s);
	sha1(nil, 0, score, &s);

	/*
	 * check for no checksum or the same
	 */
	if(scorecmp(score, arena->part->mapped+a+e) != 0
	&& scorecmp(zeroscore, arena->part->mapped+a+e) != 0)
		logerr(EOk, "overwriting mismatched checksums for arena=%s, found=%V calculated=%V",
			arena->name, arena->part->mapped+a+e, score);
	qlock(&arena->lock);
	scorecp(arena->score, score);
	if(scorecmp(score, arena->part->mapped+a+e) != 0)
		scorecp(arena->part->mapped+a+e, score);
	wbarena(arena);
	qunlock(&arena->lock);
}

/*
 * write the arena trailer block to the partition
 */
int
wbarena(Arena *arena)
{
	int bad;
	uchar *trailer = arena->part->mapped + arena->base + arena->size;
	bad = okarena(arena)<0 || packarena(arena, trailer)<0;
	scorecp(trailer+arena->blocksize-VtScoreSize, arena->score);
	msync(trailer,arena->blocksize,MS_SYNC);
	if(bad)
		return -1;
	return 0;
}

int
wbarenahead(Arena *arena)
{
	ArenaHead head;
	int bad;

	namecp(head.name, arena->name);
	head.version = arena->version;
	head.size = arena->size + 2 * arena->blocksize;
	head.blocksize = arena->blocksize;
	head.clumpmagic = arena->clumpmagic;
	/*
	 * this writepart is okay because it only happens
	 * during initialization.
	 */
	bad = packarenahead(&head, arena->part->mapped+arena->base - arena->blocksize)<0 ||
	      flushpart(arena->part)<0;
	if(bad)
		return -1;
	return 0;
}

/*
 * read the arena header and trailer blocks from disk
 */
static int
loadarena(Arena *arena)
{
	ArenaHead head;
	ZBlock *b;

	b = alloczblock(arena->blocksize, 0, arena->part->blocksize);
	if(b == nil)
		return -1;
	if(readpart(arena->part, arena->base + arena->size, b->data, arena->blocksize) < 0){
		freezblock(b);
		return -1;
	}
	if(unpackarena(arena, b->data) < 0){
		freezblock(b);
		return -1;
	}
	if(arena->version != ArenaVersion4 && arena->version != ArenaVersion5){
		seterr(EAdmin, "unknown arena version %d", arena->version);
		freezblock(b);
		return -1;
	}
	scorecp(arena->score, &b->data[arena->blocksize - VtScoreSize]);

	if(readpart(arena->part, arena->base - arena->blocksize, b->data, arena->blocksize) < 0){
		logerr(EAdmin, "can't read arena header: %r");
		freezblock(b);
		return 0;
	}
	if(unpackarenahead(&head, b->data) < 0)
		logerr(ECorrupt, "corrupted arena header: %r");
	else if(namecmp(arena->name, head.name)!=0
	     || arena->clumpmagic != head.clumpmagic
	     || arena->version != head.version
	     || arena->blocksize != head.blocksize
	     || arena->size + 2 * arena->blocksize != head.size){
		if(namecmp(arena->name, head.name)!=0)
			logerr(ECorrupt, "arena tail name %s head %s",
				arena->name, head.name);
		else if(arena->clumpmagic != head.clumpmagic)
			logerr(ECorrupt, "arena tail clumpmagic 0x%lux head 0x%lux",
				(ulong)arena->clumpmagic, (ulong)head.clumpmagic);
		else if(arena->version != head.version)
			logerr(ECorrupt, "arena tail version %d head version %d",
				arena->version, head.version);
		else if(arena->blocksize != head.blocksize)
			logerr(ECorrupt, "arena tail block size %d head %d",
				arena->blocksize, head.blocksize);
		else if(arena->size+2*arena->blocksize != head.size)
			logerr(ECorrupt, "arena tail size %lud head %lud",
				(ulong)arena->size+2*arena->blocksize, head.size);
		else
			logerr(ECorrupt, "arena header inconsistent with arena data");
	}
	freezblock(b);

	return 0;
}

static int
okarena(Arena *arena)
{
	u64int dsize;
	int ok;

	ok = 0;
	dsize = arenadirsize(arena, arena->diskstats.clumps);
	if(arena->diskstats.used + dsize > arena->size){
		seterr(ECorrupt, "arena %s used > size", arena->name);
		ok = -1;
	}

	if(arena->diskstats.cclumps > arena->diskstats.clumps)
		logerr(ECorrupt, "arena %s has more compressed clumps than total clumps", arena->name);

	/*
	 * This need not be true if some of the disk is corrupted.
	 *
	if(arena->diskstats.uncsize + arena->diskstats.clumps * ClumpSize + arena->blocksize < arena->diskstats.used)
		logerr(ECorrupt, "arena %s uncompressed size inconsistent with used space %lld %d %lld", arena->name, arena->diskstats.uncsize, arena->diskstats.clumps, arena->diskstats.used);
	 */

	/*
	 * this happens; it's harmless.
	 *
	if(arena->ctime > arena->wtime)
		logerr(ECorrupt, "arena %s creation time after last write time", arena->name);
	 */
	return ok;
}

static CIBlock*
getcib(Arena *arena, int clump, int writing, CIBlock *rock)
{
	int mode;
	CIBlock *cib;
	u32int block, off;

	if(clump >= arena->memstats.clumps){
		seterr(EOk, "clump directory access out of range");
		return nil;
	}
	block = clump / arena->clumpmax;
	off = (clump - block * arena->clumpmax) * ClumpInfoSize;
	cib = rock;
	cib->block = block;
	cib->offset = off;

	if(writing){
		if(off == 0 && clump == arena->memstats.clumps-1)
			mode = OWRITE;
		else
			mode = ORDWR;
	}else
		mode = OREAD;

	cib->data = getdblock(arena->part,
		arena->base + arena->size - (block + 1) * arena->blocksize, mode);
	if(cib->data == nil)
		return nil;
	return cib;
}

static void
putcib(Arena *arena, CIBlock *cib)
{
	USED(arena);

	putdblock(cib->data);
	cib->data = nil;
}

/* these are from dcache.c */
DBlock staticdblock;
DBlock *getdblock(Part *part, u64int addr, int mode){
	USED(mode);
//	return part->mapped+addr;
	staticdblock.data= part->mapped+addr;
	return &staticdblock;
}
void flushdcache(void) {
	if( mainindex->arenas[0]==0 || mainindex->arenas[0]->part==0) 
		{ threadexitsall( "flushd failed\n" );}
#ifndef XXX
if( mainindex->arenas[0]->part != config.aparts[0]->part )
	fprint(2, "partition trouble\n");
#endif
	fprint(2, "flushdcache %llx %ulld\n",
		mainindex->arenas[0]->part->mapped,mainindex->arenas[0]->part->size);
	msync(mainindex->arenas[0]->part->mapped,mainindex->arenas[0]->part->size,MS_SYNC);
	fsync(mainindex->arenas[0]->part->fd);
}
void initdcache(u32int n) {}
void kickdcache(void) {}
void emptydcache(void) {}
void dirtydblock(DBlock *d,int n) {}
void putdblock(DBlock *b) {}

