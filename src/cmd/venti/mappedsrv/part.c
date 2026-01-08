#include "stdinc.h"
#include <ctype.h>
#include "dat.h"
#include "fns.h"
#include <sys/mman.h>
#ifndef PROT_MAX
#define PROT_MAX(x) 0
#endif

u32int	maxblocksize;
#pragma weak readonly
int	readonly;

int findsubpart(Part *part, char *name);

static int
strtoullsuf(char *p, char **pp, int rad, u64int *u)
{
	u64int v;

	if(!isdigit((uchar)*p))
		return -1;
	v = strtoull(p, &p, rad);
	switch(*p){
	case 'k':
	case 'K':
		v *= 1024;
		p++;
		break;
	case 'm':
	case 'M':
		v *= 1024*1024;
		p++;
		break;
	case 'g':
	case 'G':
		v *= 1024*1024*1024;
		p++;
		break;
	case 't':
	case 'T':
		v *= 1024*1024;
		v *= 1024*1024;
		p++;
		break;
	}
	*pp = p;
	*u = v;
	return 0;
}

static int
parsepart(char *name, char **file, char **subpart, u64int *lo, u64int *hi)
{
	char *p;

	*file = estrdup(name);
	*lo = 0;
	*hi = 0;
	*subpart = nil;
	if((p = strrchr(*file, ':')) == nil)
		return 0;
	*p++ = 0;
	if(isalpha(*p)){
		*subpart = p;
		return 0;
	}
	if(*p == '-')
		*lo = 0;
	else{
		if(strtoullsuf(p, &p, 0, lo) < 0){
			free(*file);
			return -1;
		}
	}
	if(*p == '-')
		p++;
	if(*p == 0){
		*hi = 0;
		return 0;
	}
	if(strtoullsuf(p, &p, 0, hi) < 0 || *p != 0){
		free(*file);
		return -1;
	}
	return 0;
}

#undef min
#define min(a, b) ((a) < (b) ? (a) : (b))
Part*
initpart(char *name, int mode)
{
	Part *part;
	Dir *dir;
	char *file, *subname;
	u64int lo=0, hi=0;

	if(parsepart(name, &file, &subname, &lo, &hi) < 0){
		werrstr("cannot parse name %s", name);
		return nil;
	}
	trace(TraceDisk, "initpart %s file %s lo 0x%llx hi 0x%llx", name, file, lo, hi);
	part = MKZ(Part);
	part->name = estrdup(name);
	part->filename = estrdup(file);
	if(readonly){
		mode &= ~(OREAD|OWRITE|ORDWR);
		mode |= OREAD;
	}
	part->fd = open(file, mode);
	if(part->fd < 0){
		if((mode&(OREAD|OWRITE|ORDWR)) == ORDWR)
			part->fd = open(file, (mode&~ORDWR)|OREAD);
		if(part->fd < 0){
			freepart(part);
			fprint(2, "can't open partition='%s': %r\n", file);
			seterr(EOk, "can't open partition='%s': %r", file);
			fprint(2, "%r\n");
			free(file);
			return nil;
		}
		mode &= ~(ORDWR|OWRITE);
		mode |= OREAD;
		fprint(2, "warning: %s opened for reading only\n", name);
	}
	part->offset = lo;
	dir = dirfstat(part->fd);
	if(dir == nil){
		freepart(part);
		seterr(EOk, "can't stat partition='%s': %r", file);
		free(file);
		return nil;
	}
	if(dir->length == 0){
		free(dir);
		dir = dirstat(file);
		if(dir == nil || dir->length == 0) {
			freepart(part);
			seterr(EOk, "can't determine size of partition %s", file);
			free(file);
			return nil;
		}
	}
	if(dir->length < hi || dir->length < lo){
		freepart(part);
		seterr(EOk, "partition '%s': bounds out of range (max %lld)", name, dir->length);
		free(dir);
		free(file);
		return nil;
	}
	if(hi == 0)
		hi = dir->length;
	part->size = hi - part->offset;
#ifdef CANBLOCKSIZE
	{
		struct statfs sfs;
		if(fstatfs(part->fd, &sfs) >= 0 && sfs.f_bsize > 512)
			part->fsblocksize = sfs.f_bsize;
	}
#endif

	part->fsblocksize = min(part->fsblocksize, MaxIo);

	if(subname && findsubpart(part, subname) < 0){
		werrstr("cannot find subpartition %s", subname);
		freepart(part);
		return nil;
	}
	free(dir);
	fprint(2, "%d ", part->fd);
	if(mode&(OWRITE|ORDWR)) {
		part->mapped = mmap(NULL, part->size,
		PROT_MAX(PROT_READ|PROT_WRITE)|PROT_READ|PROT_WRITE, MAP_SHARED , part->fd, part->offset);
	fprint(2, "writable %s map: %llx, size: %lld, offset: %lld\n",
		part->name, part->mapped,part->size,part->offset);
	} else {
		part->mapped = mmap(NULL, part->size,
		PROT_MAX(PROT_READ)|PROT_READ, MAP_SHARED , part->fd, part->offset);
	fprint(2, "readable %s map: %llx, size: %lld, offset: %lld\n",
		part->name, part->mapped,part->size,part->offset);
	}
	if(part->mapped==(void*)(-1)) {
		werrstr("mapping failed\n" );
		freepart(part);
		return nil;
	}
	return part;
}

int
flushpart(Part *part)
{
	msync(part->mapped,0,MS_SYNC);
	fsync(part->fd);
	return 0;
}

void
freepart(Part *part)
{
	if(part == nil)
		return;
	flushpart(part);
	if(part->mapped) { munmap( part->mapped, part->size); part->mapped=0; }
	if(part->fd >= 0)
		close(part->fd);
	free(part->name);
	free(part);
}

void
partblocksize(Part *part, u32int blocksize)
{
	if(part->blocksize)
		sysfatal("resetting partition=%s's block size", part->name);
	part->blocksize = blocksize;
	if(blocksize > maxblocksize)
		maxblocksize = blocksize;
}

#ifndef PLAN9PORT
static int sdreset(Part*);
static int threadspawnl(int[3], char*, char*, ...);
#endif

int
readpart(Part *part, u64int offset, u8int *buf, u32int count)
{
	if(offset >= part->size || offset+count > part->size){
		seterr(EStrange, "out of bounds %s offset 0x%llux count %ud to partition %s size 0x%llux",
			"read", offset, count, part->name, part->size);
		return -1;
	}
	memmove(buf, part->mapped+offset, count);
	return count;
}

int
writepart(Part *part, u64int offset, u8int *buf, u32int count)
{
	if(offset >= part->size || offset+count > part->size){
		seterr(EStrange, "out of bounds %s offset 0x%llux count %ud to partition %s size 0x%llux",
			"write", offset, count, part->name, part->size);
		return -1;
	}
	a_wr(part->mapped+offset, buf, count);
	return count;
}

ZBlock*
readfile(char *name)
{
	Part *p;
	ZBlock *b;

	p = initpart(name, OREAD);
	if(p == nil)
		return nil;
	b = alloczblock(p->size, 0, p->blocksize);
	if(b == nil){
		seterr(EOk, "can't alloc %s: %r", name);
		freepart(p);
		return nil;
	}
	if(readpart(p, 0, b->data, p->size) < 0){
		seterr(EOk, "can't read %s: %r", name);
		freepart(p);
		freezblock(b);
		return nil;
	}
	if(p->mapped) { munmap( p->mapped,0); p->mapped=0; }
	close(p->fd);
	freepart(p);
	return b;
}

/*
 * Search for the Plan 9 partition with the given name.
 * This lets you write things like /dev/ad4:arenas
 * if you move a disk from a Plan 9 system to a FreeBSD system.
 *
 * God I hope I never write this code again.
 */
#define MAGIC "plan9 partitions"
static int
tryplan9part(Part *part, char *name)
{
	uchar buf[512];
	char *line[40], *f[4];
	int i, n;
	vlong start, end;

	/*
	 * Partition table in second sector.
	 * Could also look on 2nd last sector and last sector,
	 * but those disks died out long before venti came along.
	 */
	if(readpart(part, 512, buf, 512) != 512)
		return -1;

	/* Plan 9 partition table is just text strings */
	if(strncmp((char*)buf, "part ", 5) != 0)
		return -1;

	buf[511] = 0;
	n = getfields((char*)buf, line, 40, 1, "\n");
	for(i=0; i<n; i++){
		if(getfields(line[i], f, 4, 1, " ") != 4)
			break;
		if(strcmp(f[0], "part") != 0)
			break;
		if(strcmp(f[1], name) == 0){
			start = 512*strtoll(f[2], 0, 0);
			end = 512*strtoll(f[3], 0, 0);
			if(start  < end && end <= part->size){
				part->offset += start;
				part->size = end - start;
				return 0;
			}
			return -1;
		}
	}
	return -1;
}

#define	GSHORT(p)	(((p)[1]<<8)|(p)[0])
#define	GLONG(p)	((GSHORT(p+2)<<16)|GSHORT(p))

typedef struct Dospart Dospart;
struct Dospart
{
	uchar flag;		/* active flag */
	uchar shead;		/* starting head */
	uchar scs[2];		/* starting cylinder/sector */
	uchar type;		/* partition type */
	uchar ehead;		/* ending head */
	uchar ecs[2];		/* ending cylinder/sector */
	uchar offset[4];		/* starting sector */
	uchar size[4];		/* length in sectors */
};


int
findsubpart(Part *part, char *name)
{
	int i;
	uchar buf[512];
	u64int size;
	Dospart *dp;

	/* See if this is a Plan 9 partition. */
	if(tryplan9part(part, name) >= 0)
		return 0;

	/* Otherwise try for an MBR and then narrow to Plan 9 partition. */
	if(readpart(part, 0, buf, 512) != 512)
		return -1;
	if(buf[0x1FE] != 0x55 || buf[0x1FF] != 0xAA)
		return -1;
	dp = (Dospart*)(buf+0x1BE);
	size = part->size;
	for(i=0; i<4; i++){
		if(dp[i].type == '9'){
			part->offset = 512LL*GLONG(dp[i].offset);
			part->size = 512LL*GLONG(dp[i].size);
			if(tryplan9part(part, name) >= 0)
				return 0;
			part->offset = 0;
			part->size = size;
		}
		/* Not implementing extended partitions - enough is enough. */
	}
	return -1;
}
