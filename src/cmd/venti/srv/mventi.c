#ifdef PLAN9PORT
#include <u.h>
#include <signal.h>
#endif
#include "stdinc.h"
#include "dat.h"
#include "fns.h"

//#define XXX

int debug=0;
int nofork=0;
extern int nowrci;
int mainstacksize = 256*1024;
VtSrv *ventisrv;
Config config;

void trie_init(void);
unsigned int trie_insert(unsigned char *, uvlong*); 
unsigned int trie_retrieve(unsigned char *, uvlong*); 

static void ventiserver(void*);
static void fmtindex(Config *conf, Index *ix);

void
usage(void)
{
	fprint(2, "usage: mventi [-Ldrs] [-a address] [-B blockcachesize] [-c config] "
"[-C lumpcachesize] [-h httpaddress] [-I initialclumps] [-W webroot]\n");
	threadexitsall("usage");
}

int
threadmaybackground(void)
{
	return 1;
}

static ArenaPart*
configarenas(char *file)
{
	ArenaPart *ap;
	Part *part;

	if(0) fprint(2, "configure arenas in %s\n", file);
	part = initpart(file, ORDWR|ODIRECT);
	if(part == nil)
		return nil;
	ap = initarenapart(part);
	if(ap == nil)
		werrstr("%s: %r", file);
	return ap;
}

void
threadmain(int argc, char *argv[])
{
	char *configfile, *haddr, *vaddr, *webroot;
	u32int mem, bcmem, minbcmem;

	traceinit();
	threadsetname("main");
	maxblocksize =4096;
	vaddr = "tcp!127.1!17034";
	haddr = "tcp!127.1!8901";
	configfile = nil;
	webroot = nil;
	mem = 64*1024*1024;
	bcmem = 10*1024*1024;
	ARGBEGIN{
	case 'a':
		vaddr = EARGF(usage());
		break;
	case 'B':
		bcmem = unittoull(EARGF(usage()));
		break;
	case 'c':
		configfile = EARGF(usage());
		break;
	case 'C':
		mem = unittoull(EARGF(usage()));
		break;
	case 'D':
		settrace(EARGF(usage()));
		break;
	case 'd':
		debug = 1;
		nofork = 1;
		break;
	case 'h':
		haddr = EARGF(usage());
		break;
	case 'L':
		ventilogging = 1;
		break;
	case 'r':
		readonly = 1;
		break;
	case 's':
		nofork = 1;
		break;
	case 't':
		nowrci = 1;
		break;
	case 'w':			/* compatibility with old venti */
		queuewrites = 1;
		break;
	case 'W':
		webroot = EARGF(usage());
		break;
	default:
		usage();
	}ARGEND

	if(argc) {
		config.index = estrdup("main");
		config.naparts = argc;
		config.aparts = MKN(ArenaPart*, config.naparts);
	}
	int i = 0;
	while(argc) {
		argc--;
		config.aparts[i] = configarenas(argv[0]);
		i++;
		argv++;
	}

	if(!nofork)
		rfork(RFNOTEG);

#ifdef PLAN9PORT
	{
		/* sigh - needed to avoid signals when writing to hungup networks */
		struct sigaction sa;
		memset(&sa, 0, sizeof sa);
		sa.sa_handler = SIG_IGN;
		sigaction(SIGPIPE, &sa, nil);
	}
#endif

	ventifmtinstall();
	trace(TraceQuiet, "venti started");
	fprint(2, "%T venti: ");

	if(configfile != nil) {
	fprint(2, "conf...");
	if(initventi(configfile, &config) < 0)
		sysfatal("can't init server: %r");

	if(mem == 0)
		mem = config.mem;
	if(bcmem == 0)
		bcmem = config.bcmem;
	if(haddr == nil)
		haddr = config.haddr;
	if(vaddr == nil)
		vaddr = config.vaddr;
	if(vaddr == nil)
		vaddr = "tcp!*!venti";
	if(webroot == nil)
		webroot = config.webroot;
	if(queuewrites == 0)
		queuewrites = config.queuewrites;
	fprint(2, "confdone...");
	} else {
		mainindex = initindex(config.index, 0, 0);
	}

	if(haddr){
		fprint(2, "httpd %s...", haddr);
		if(httpdinit(haddr, webroot) < 0)
			fprint(2, "warning: can't start http server: %r");
	}
	fprint(2, "init...");

	if(mem == 0xffffffffUL)
		mem = 1 * 1024 * 1024;

	/*
	 * lump cache
	 */
	if(0) fprint(2, "initialize %d bytes of lump cache for %d lumps\n",
		mem, mem / (8 * 1024));
	initlumpcache(mem, mem / (8 * 1024));

	/*
	 * block cache: need a block for every arena and every process
	 */
	minbcmem = maxblocksize *
		(mainindex->narenas + mainindex->nsects*4 + 16);
	if(bcmem < minbcmem)
		bcmem = minbcmem;
	if(0) fprint(2, "initialize %d bytes of disk block cache\n", bcmem);
	initdcache(bcmem);

	trie_init();

	if(initarenasum() < 0)
		fprint(2, "warning: can't initialize arena summing process: %r");

	fprint(2, "announce %s...", vaddr);
	ventisrv = vtlisten(vaddr);
	if(ventisrv == nil)
		sysfatal("can't announce %s: %r", vaddr);

	fprint(2, "serving.\n");
	if(nofork)
		ventiserver(nil);
	else
		vtproc(ventiserver, nil);

	threadexits(nil);
}

static void
vtrerror(VtReq *r, char *error)
{
	r->rx.msgtype = VtRerror;
	r->rx.error = estrdup(error);
}

static void
ventiserver(void *v)
{
	Packet *p;
	VtReq *r;
	char err[ERRMAX];
	uint ms;
	int cached, ok;

	USED(v);
	threadsetname("ventiserver");
	trace(TraceWork, "start");
	while((r = vtgetreq(ventisrv)) != nil){
		trace(TraceWork, "finish");
		trace(TraceWork, "start request %F", &r->tx);
		trace(TraceRpc, "<- %F", &r->tx);
		r->rx.msgtype = r->tx.msgtype+1;
		addstat(StatRpcTotal, 1);
		if(0) print("req (arenas[0]=%p sects[0]=%p) %F\n",
			mainindex->arenas[0], mainindex->sects[0], &r->tx);
		switch(r->tx.msgtype){
		default:
			vtrerror(r, "unknown request");
			break;
		case VtTread:
			ms = msec();
			r->rx.data = readlump(r->tx.score, r->tx.blocktype, r->tx.count, &cached);
			ms = msec() - ms;
			addstat2(StatRpcRead, 1, StatRpcReadTime, ms);
			if(r->rx.data == nil){
				addstat(StatRpcReadFail, 1);
				rerrstr(err, sizeof err);
				vtrerror(r, err);
			}else{
				addstat(StatRpcReadBytes, packetsize(r->rx.data));
				addstat(StatRpcReadOk, 1);
				if(cached)
					addstat2(StatRpcReadCached, 1, StatRpcReadCachedTime, ms);
				else
					addstat2(StatRpcReadUncached, 1, StatRpcReadUncachedTime, ms);
			}
			break;
		case VtTwrite:
			if(readonly){
				vtrerror(r, "read only");
				break;
			}
			p = r->tx.data;
			r->tx.data = nil;
			addstat(StatRpcWriteBytes, packetsize(p));
			ms = msec();
			ok = writelump(p, r->rx.score, r->tx.blocktype, 0, ms);
			ms = msec() - ms;
			addstat2(StatRpcWrite, 1, StatRpcWriteTime, ms);

			if(ok < 0){
				addstat(StatRpcWriteFail, 1);
				rerrstr(err, sizeof err);
				vtrerror(r, err);
			}
			break;
		case VtTsync:
//			flushqueue();
			flushdcache();
			break;
		}
		trace(TraceRpc, "-> %F", &r->rx);
		vtrespond(r);
		trace(TraceWork, "start");
	}
	flushdcache();
	threadexitsall(0);
}

/*
 * Index, mapping scores to log positions.
 */
Index*
initindex(char *name, ISect **sects, int n)
{
	USED(n);
	USED(sects);
	USED(name);
	Index *ix;
	ix = MKZ(Index);

	fmtindex(&config, ix);
	return ix;
}

static void
fmtindex(Config *conf, Index *ix)
{
	u32int narenas;
	AMap *amap;
	u64int addr;
	ArenaPart *ap;
	Arena **arenas;
	namecp(ix->name, "main");

        narenas = 0;
        for(int i = 0; i < conf->naparts; i++){
                ap = conf->aparts[i];
                narenas += ap->narenas;
        }

	amap = MKNZ(AMap, narenas);
	arenas = MKNZ(Arena*, narenas);
	ix->amap = amap;
	ix->arenas = arenas;

	addr = IndexBase;
	int n = 0;
//	if(0) fprint(2,"amap %llx %llx ix-amap %llx\n", amap, arenas, ix->amap);
	for(int i = 0; i < conf->naparts; i++){
		ap = conf->aparts[i];
		for(int j = 0; j < ap->narenas; j++){
			if(n >= narenas)
				sysfatal("too few slots in index's arena set");

			arenas[n] = ap->arenas[j];
			if(n < ix->narenas){
				if(arenas[n] != ix->arenas[n])
					sysfatal("mismatched arenas %s and %s at slot %d",
						arenas[n]->name, ix->arenas[n]->name, n);
				amap[n] = ix->amap[n];
				if(amap[n].start != addr)
					sysfatal("mis-located arena %s in index %s", arenas[n]->name, ix->name);
				addr = amap[n].stop;
			}else{
				amap[n].start = addr;
				addr += ap->arenas[j]->size;
				amap[n].stop = addr;
				namecp(amap[n].name, ap->arenas[j]->name);
				if(0) fprint(2, "add arena %s at [%lld,%lld)\n",
					amap[n].name, amap[n].start, amap[n].stop);
			}
			n++;
		}
	}
	ix->narenas = narenas;
}

ISect*
initisect(Part *part)
{
	USED(part);
	ISect *is;
	is = MKZ(ISect);
	return is;
}

void
freeisect(ISect *is)
{
	if(is == nil)
		return;
	free(is);
}

void
freeindex(Index *ix)
{
	int i;

	if(ix == nil)
		return;
	free(ix->amap);
	free(ix->arenas);
	if(ix->sects)
		for(i = 0; i < ix->nsects; i++)
			freeisect(ix->sects[i]);
	free(ix->sects);
	free(ix->smap);
	free(ix);
}
