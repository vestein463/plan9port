/*
 * Rebuild the index from scratch, in place.
 */
#include "stdinc.h"
#include "dat.h"
#include "fns.h"
unsigned int trie_insert(unsigned char *, IAddr *);
void trie_print(int);

int		errors;

u64int	totalclumps;
Channel	*arenadonechan;
Index	*ix;

u64int	arenaentries;
u64int	skipentries;
u64int	indexentries;

static void	arenapartproc(void*);
#ifdef MAIN
void
usage(void)
{
	fprint(2, "usage: buildtrie [-v] venti.conf\n");
	threadexitsall("usage");
}

void trie_init( void);
void
threadmain(int argc, char *argv[])
{
	Config conf;
	int vflag=0;

	ventifmtinstall();
	ARGBEGIN{
	case 'v':
		++vflag;
		break;
	default:
		usage();
		break;
	}ARGEND

	if(argc != 1)
		usage();

	if(initventi(argv[0], &conf) < 0)
		sysfatal("can't init venti: %r");
	trie_init();
	if( vflag) trie_print(0);
	threadexitsall(nil);
}
#endif

void
trie_init( void) {
	int fd, i, napart, nfinish;
	u32int bcmem;
	Part *p;

	ix = mainindex;
	ix->bloom = nil;

#if 0
	/*
	 * safety first - only need read access to arenas
	 */
	p = nil;
	for(i=0; i<ix->narenas; i++){
		if(ix->arenas[i]->part != p){
			p = ix->arenas[i]->part;
			if((fd = open(p->filename, OREAD)) < 0)
				sysfatal("cannot reopen %s: %r", p->filename);
			dup(fd, p->fd);
			close(fd);
		}
	}
#endif

	/*
	 * need a block for every arena
	 */
#ifdef MAIN
	bcmem = maxblocksize * (mainindex->narenas + 16);
	if(0) fprint(2, "initialize %d bytes of disk block cache\n", bcmem);
	initdcache(bcmem);
#endif
	totalclumps = 0;
	for(i=0; i<ix->narenas; i++)
		totalclumps += ix->arenas[i]->diskstats.clumps;

	/* start arena procs */
	p = nil;
	napart = 0;
	nfinish = 0;
	arenadonechan = chancreate(sizeof(void*), 0);
	for(i=0; i<ix->narenas; i++){
		if(ix->arenas[i]->part != p){
			p = ix->arenas[i]->part;
			vtproc(arenapartproc, p);
			++napart;
		}
	}

	/* wait for arena procs to finish */
	for(; nfinish<napart; nfinish++)
		recvp(arenadonechan);
	fprint(2, "%T done arenaentries=%,lld indexed=%,lld (nskip=%,lld)\n",
		arenaentries, indexentries, skipentries);
}

/*
 * Read through an arena partition and send each of its IEntries
 * to the appropriate index section.  When finished, send on
 * arenadonechan.
 */
enum
{
	ClumpChunks = 32*1024,
};
static void
arenapartproc(void *v)
{
	int i, j, n, nskip;
	u32int clump;
	u64int addr, tot;
	Arena *a;
	ClumpInfo *ci, *cis;
	IEntry ie;
	Part *p;

	p = v;
	threadsetname("arenaproc %s", p->name);

	nskip = 0;
	tot = 0;
	cis = MKN(ClumpInfo, ClumpChunks);
	for(i=0; i<ix->narenas; i++){
		a = ix->arenas[i];
		if(a->part != p)
			continue;
		if(a->memstats.clumps)
			fprint(2, "%T arena %s: %d entries\n",
				a->name, a->memstats.clumps);
		/*
		 * Running the loop backwards accesses the
		 * clump info blocks forwards, since they are
		 * stored in reverse order at the end of the arena.
		 * This speeds things slightly.
		 */
		addr = ix->amap[i].start + a->memstats.used;
		for(clump=a->memstats.clumps; clump > 0; clump-=n){
			n = ClumpChunks;
			if(n > clump)
				n = clump;
			if(readclumpinfos(a, clump-n, cis, n) != n){
				fprint(2, "%T arena %s: directory read: %r\n", a->name);
				errors = 1;
				break;
			}
			for(j=n-1; j>=0; j--){
				ci = &cis[j];
				ie.ia.type = ci->type;
				ie.ia.size = ci->uncsize;
				addr -= ci->size + ClumpSize;
				ie.ia.addr = addr;
				ie.ia.blocks = (ci->size + ClumpSize + (1<<ABlockLog)-1) >> ABlockLog;
				scorecp(ie.score, ci->score);
				if(ci->type == VtCorruptType)
					nskip++;
				else{
					(void)trie_insert(ie.score,&ie.ia);
					tot++;
				}
			}
		}
		if(addr != ix->amap[i].start)
			fprint(2, "%T arena %s: clump miscalculation %lld != %lld\n", a->name, addr, ix->amap[i].start);
	}
	static Lock l;
	lock(&l);
	arenaentries += tot;
	skipentries += nskip;
	unlock(&l);

	free(cis);
	sendp(arenadonechan, p);
}
