#include <u.h>
#include <libc.h>
#include <venti.h>
#include <diskfs.h>

#define SCORE "ffs:ef6c183eabb7caafed7f85a3447a12accfbbeb8b"
Fsys *fsys;
u8int *zero;

void doit(int i);

void
main(int argc, char **argv)
{
	char *pref;
	uchar score[VtScoreSize];
	Disk *disk;
	VtCache *c;
	VtConn *z;

	fmtinstall('V', vtscorefmt);

	if(vtparsescore(SCORE, &pref, score) < 0)
		sysfatal("bad score '%s'", argv[0]);
	if((z = vtdial(nil)) == nil)
		sysfatal("vtdial: %r");
	if(vtconnect(z) < 0)
		sysfatal("vtconnect: %r");
	if((c = vtcachealloc(z, 16384*32)) == nil)
		sysfatal("vtcache: %r");
	if((disk = diskopenventi(c, score)) == nil)
		sysfatal("diskopenventi: %r");
	if((fsys = fsysopen(disk)) == nil)
		sysfatal("fsysopen: %r");
	doit(0); }

void doit(int i) {
	Block *b;
	zero = emalloc(fsys->blocksize);
	for(i=0; i<fsys->nblock; i++){
		if((b = fsysreadblock(fsys, i)) != nil){
			if(pwrite(1, b->data, fsys->blocksize,
			    (u64int)fsys->blocksize*i) != fsys->blocksize)
				fprint(2, "error writing block %lud: %r\n", i);
			blockput(b);
		}else 
			if(pwrite(1, zero, fsys->blocksize,
			    (u64int)fsys->blocksize*i) != fsys->blocksize)
				fprint(2, "error writing block %lud: %r\n", i);
	}
}
