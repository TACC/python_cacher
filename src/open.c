#include <stdio.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
//#include <dirent.h>
#include <pthread.h>
#include <errno.h>
#include <pwd.h>

#include <sys/mman.h>

#include "dict.h"
#include "xxhash.h"
#include "hook.h"

#define NONE_EXISTING_FILE	(-1)
#define UNDER_PREPARING		(-2)

#define INVALID_INODE	(-1)
#define OUT_OF_SPACE	(-111)
#define PTHREAD_MUTEXATTR_FLAG_PSHARED (0x80000000)	// int 
#define FAKE_DIR_FD	(20000000)	// >= this number will be treated as a fake fd. Used for dir

#define MAX_NUM_TARGET_DIR	(8)

#define MAX_NUM_LOCK	(100)

#define MAX_NUM_FILE	(1024*256)
#define MAX_NUM_DIR		(1024*8)
#define MAX_NUM_ENTRY_DIR		(1024)
//#define MAX_ENTRY_LEN	(96)
#define MAX_ENTRY_BUFF_LEN	(1024*20)
#define MAX_NEW_FILE_NAME_LEN	(48)
#define TAG_INIT_DONE	(0x13572468)
#define MAX_BYTES_PER_FILE_TO_CACHE	(16*1024*1024)

#define N_CACHE_DIR		(48)

static struct timespec tim1, tim2;
static int uid;
static unsigned long nMaxOpen=0;
static int Python_IO_Debug=0;
static int Python_IO_Cache_Nonexisting_File=0;
static int Python_IO_Cache_CWD=0;
static char szCWD[1024]="";

static int nTargetDir=0;
static char szTargetDir[MAX_NUM_TARGET_DIR][256];	// the characteristic path that will be mapped.
static char szLocalDir[256];	// the new path to store all files
static char mutex_name[64];
static char szHostName[256];

typedef struct
{
  int fd;                     /* File descriptor.  */
  size_t allocation;          /* Space allocated for the block.  */
  size_t size;                /* Total valid data in the block.  */
  size_t offset;              /* Current offset into the block.  */
  off_t filepos;              /* Position of next entry to read.  */
}DIR;

typedef struct
 {
    unsigned long int d_ino;
    unsigned long int d_off;
    unsigned short int d_reclen;
    unsigned char d_type;
    char d_name[256];           /* We must not include limits.h! */
}dirent;

typedef struct	{
	char szNewName[MAX_NEW_FILE_NAME_LEN];	// A real new name is used if file is physically copied to local storage. NULL if only stat is stored. -1 means file/dir does not exist!
	struct stat st;	// aligned
}FILE_INFO;

typedef struct	{
	int nEntries;	// the number of entries in this dir
	int Offset;
	int padding[2];
	int OffsetList[MAX_NUM_ENTRY_DIR];	// the offset in szEntryList
	char szEntryList[MAX_ENTRY_BUFF_LEN];
}DIR_INFO;

static int shm_fd;	// fd for mutex
static void *p_shm=NULL;	// ptr to shared memory
static pthread_mutex_t *p_futex_open_stat=NULL;		// ptr for pthread_mutex_t
static pthread_mutex_t *p_futex_dirlist=NULL;		// ptr for pthread_mutex_t

static Dict p_Hash_File=NULL;
static struct elt *elt_list_file=NULL;
static int *ht_table_file=NULL;

static Dict p_Hash_Dir=NULL;
static struct elt *elt_list_dir=NULL;
static int *ht_table_dir=NULL;

static int nSize_Shared_Data=0;
static int size_pt_mutex_t=0;

static int *nFileRec=NULL, *nFileStat=NULL, *nDir=NULL, *pInit_Done;
static long int *nFileSize=NULL;
static long int *nFileRec_Acc=NULL;

static FILE_INFO *pFile_Info=NULL;
static DIR_INFO *pDir_Info=NULL;
static int *Stop_Sign_List=NULL;
// shared data: Mutex_open_stat, Mutex_dir, nFile, nFileSize, nFileStat, Hash table, FILE_INFO, STAT, DIR_INFO. 

//void init_hook();
//void Register_A_Hook(char *module_name, char *Func_Name, void *NewFunc_Addr, long int *ptr_Call_Org);	// so file name, the function you want to intercept, the new function user supply, to store the pointer for calling original function
//void Install_Hook(void);
//void Uninstall_Hook(void);

static void Get_Exe_Name(int pid, char szName[]);
static void Take_a_Short_Nap(int nsec);

static int Is_Target_Dir_Included(const char szFileName[], int *Is_CWD_Match);
static int Is_File_py_pyc_so(const char szFileName[]);
static int Is_Target_FileName_Excluded_in_Open(const char szFileName[]);
static int Is_Target_FileName_Excluded_in_Stat(const char szFileName[]);
static void Parse_Target_Dir_String(char szStr[]);

static void Init_Shared_Data(void);
static void Init_Cache_Dir(void);
//static void Append_Msg(const char *szMsg);
static void Get_Parent_Dir(void);
static void Get_Home_Dir(void);

static ssize_t read_all(int fd, void *buf, size_t count);
static ssize_t write_all(int fd, const void *buf, size_t count);
static void Get_Hostname(void);

static int pid;

typedef int (*org_open)(const char *pathname, int oflags, ...);
org_open real_open_ld=NULL, real_open_libc=NULL, real_open_pthread=NULL;

typedef int (*org_xstat)(int vers, const char *filename, struct stat *buf);
org_xstat real_xstat_ld=NULL, real_xstat_libc=NULL;

typedef DIR * (*org_opendir)(const char *name);
org_opendir real_opendir=NULL;

typedef int (*org_closedir)(DIR *dirp);
org_closedir real_closedir=NULL;

typedef dirent * (*org_readdir)(DIR *dirp);
org_readdir real_readdir=NULL;

typedef void (*org_rewinddir)(DIR *dirp);
org_rewinddir real_rewinddir=NULL;

typedef int (*org_rename)(const char *szOldName, const char *szNewName);
org_rename real_rename=NULL;

static void Save_File_in_Local(const char szFileName[], char szNewName[], int fd, struct stat *file_stat, org_open real_open, int file_idx);	// using existing fd to read the whole file and save into local local storage. Return new fd with local file
static void Save_File_Stat_in_Memory(const char szFileName[], struct stat *file_stat, int file_idx);
static int Save_Dir_Info_in_Memory(const char szPathName[], DIR *pDir);

/*
char szMsg[256];
static void Append_Msg(const char *szMsg)
{
	int fd, nLen;
	char szBuff[512], szName[16];

	printf("%s", szMsg);
	sprintf(szName, "dbg_%d.txt", pid);
	sprintf(szBuff, "%s\n", szMsg);
	nLen = strlen(szBuff);

	if(real_open_libc)	{
		fd = real_open_libc(szName, O_WRONLY);
	}
	else	{
		fd = open(szName, O_WRONLY);
	}

	lseek(fd, 0, SEEK_END);
	write(fd, szBuff, nLen);
	close(fd);
}
*/

int my_open_ld(const char *pathname, int oflags, ...)
{
	int mode = 0, two_args=1, ret, ret_stat, file_idx, To_Save=0, idx_lock, Is_CWD_Match;
	struct stat file_stat;
	char szNewName[128];
//	unsigned long long h;

	if (oflags & O_CREAT)	{
		va_list arg;
		va_start (arg, oflags);
		mode = va_arg (arg, int);
		va_end (arg);
		two_args=0;

		if(Python_IO_Debug)	printf("DBG> my_open_ld noncached, %s. Creating. oflags = %o\n", pathname, oflags);
		ret = real_open_ld(pathname, oflags, mode);

		if(Python_IO_Cache_Nonexisting_File && (ret>=0) )	{	// Check whether this file is in our cache. If found, remove it. 
			
			if( Is_Target_Dir_Included(pathname, &Is_CWD_Match) && (! Is_Target_FileName_Excluded_in_Open(pathname)) )	{
				file_idx = DictSearch(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
				if(file_idx >= 0)	{	// record found. Remove it!
//					h = XXH64(pathname, strlen(pathname), 0);
//					idx_lock = h % MAX_NUM_LOCK;
					idx_lock = 0;

					pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));
					if(pFile_Info[file_idx].szNewName[0] > 0)	{	// both file and stat are in record
						unlink(pFile_Info[file_idx].szNewName);	// remove local file in cache
					}
					DictDelete(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
					(*nFileRec) = (*nFileRec) - 1;
					pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
					if(Python_IO_Debug)	printf("DBG> my_open_ld removing %s from cache due to update\n", pathname);
				}
			}
		}
		return ret;
	}

	if( two_args && Is_Target_Dir_Included(pathname, &Is_CWD_Match) && ( ! Is_Target_FileName_Excluded_in_Open(pathname) ) )	{
		file_idx = DictSearch(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
		if(file_idx >= 0)	{
			if(pFile_Info[file_idx].szNewName[0] > 0)	{
				if(Python_IO_Debug)	printf("DBG> my_open_ld cached, %s  oflags = %o\n", pathname, oflags);
				return real_open_ld(pFile_Info[file_idx].szNewName, oflags);	// both file and stat are in record
			}
			else if(pFile_Info[file_idx].szNewName[0] == 0)	{	// only stat in rec. Need to copy file locally
				To_Save = 1;
			}
			else if(pFile_Info[file_idx].szNewName[0] == NONE_EXISTING_FILE)	{
				if(Python_IO_Debug)	printf("DBG> my_open_ld cached, %s  oflags = %o file DOES NOT exist.\n", pathname, oflags);
				errno = ENOENT;	// No such file or directory
				return -1;
			}
			else if(pFile_Info[file_idx].szNewName[0] == UNDER_PREPARING)	{
				if(Python_IO_Debug)	printf("DBG> my_open_ld cached, %s  oflags = %o. File is not ready yet. Read original file.\n", pathname, oflags);
				if(two_args)	{
					ret = real_open_ld(pathname, oflags);
				}
				else	{
					ret = real_open_ld(pathname, oflags, mode);
				}
			}
		}
		else {
			To_Save = 1;
		}
	}
	if(Python_IO_Debug)	printf("DBG> my_open_ld noncached, %s  oflags = %o\n", pathname, oflags);

	if(two_args)	{
		ret = real_open_ld(pathname, oflags);
	}
	else	{
		ret = real_open_ld(pathname, oflags, mode);
	}

	if(To_Save)	{
		if(ret >= 0)	{
			ret_stat = stat(pathname, &file_stat);
			if( (file_stat.st_mode & S_IFMT) == S_IFREG)	{	// regular file
				if( file_stat.st_size <= MAX_BYTES_PER_FILE_TO_CACHE)	{	// Cache the file if it is not too large
					if(file_idx >= 0)	Save_File_in_Local(pathname, szNewName, ret, &file_stat, real_open_ld, file_idx);
					else	Save_File_in_Local(pathname, szNewName, ret, &file_stat, real_open_ld, -1);	// put an invalid file_idx
					ret = real_open_ld(szNewName, oflags);
				}
			}
		}
		else	{
			if(Python_IO_Cache_Nonexisting_File && (errno == ENOENT) )	{
				if(Is_CWD_Match)	{
					if(Is_File_py_pyc_so(pathname) == 0)	{
						return ret;	// Do NOT cache files other than .py, .pyc and .so
					}
				}

//				h = XXH64(pathname, strlen(pathname), 0);
//				idx_lock = h % MAX_NUM_LOCK;
				idx_lock = 0;
				
				pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));
				file_idx = DictInsertAuto(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
				pFile_Info[file_idx].szNewName[0] = NONE_EXISTING_FILE;
				(*nFileRec) = (*nFileRec) + 1;
				(*nFileRec_Acc) = (*nFileRec_Acc) + 1;
				pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
				if(Python_IO_Debug)	printf("DBG> my_open_ld caching none-existing file %s\n", pathname);
				errno = ENOENT;
			}
		}
	}

	return ret;
}

int my_open_libc(const char *pathname, int oflags, ...)
{
	int mode = 0, two_args=1, ret, ret_stat, file_idx, To_Save=0, idx_lock, Is_CWD_Match;
	struct stat file_stat;
	char szNewName[128];
//	unsigned long long h;

	if (oflags & O_CREAT)	{
		va_list arg;
		va_start (arg, oflags);
		mode = va_arg (arg, int);
		va_end (arg);
		two_args=0;

		if(Python_IO_Debug)	printf("DBG> my_open_libc noncached, %s. Creating. oflags = %o\n", pathname, oflags);
		ret = real_open_libc(pathname, oflags, mode);

		if(Python_IO_Cache_Nonexisting_File && (ret>=0) )	{	// Check whether this file is in our cache. If found, remove it. 
			
			if( Is_Target_Dir_Included(pathname, &Is_CWD_Match) && (! Is_Target_FileName_Excluded_in_Open(pathname)) )	{
				file_idx = DictSearch(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
				if(file_idx >= 0)	{	// record found. Remove it!
//					h = XXH64(pathname, strlen(pathname), 0);
//					idx_lock = h % MAX_NUM_LOCK;
					idx_lock = 0;

					pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));
					if(pFile_Info[file_idx].szNewName[0] > 0)	{	// both file and stat are in record
						unlink(pFile_Info[file_idx].szNewName);	// remove local file in cache
					}
					DictDelete(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
					(*nFileRec) = (*nFileRec) - 1;
					pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
					if(Python_IO_Debug)	printf("DBG> my_open_libc removing %s from cache due to update\n", pathname);
				}
			}
		}
		return ret;
	}

	if( two_args && Is_Target_Dir_Included(pathname, &Is_CWD_Match) && ( ! Is_Target_FileName_Excluded_in_Open(pathname) ) )	{
		file_idx = DictSearch(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
		if(file_idx >= 0)	{
			if(pFile_Info[file_idx].szNewName[0] > 0)	{
				if(Python_IO_Debug)	printf("DBG> my_open_libc cached, %s  oflags = %o\n", pathname, oflags);
				return real_open_libc(pFile_Info[file_idx].szNewName, oflags);	// both file and stat are in record
			}
			else if(pFile_Info[file_idx].szNewName[0] == 0)	{	// only stat in rec. Need to copy file locally
				To_Save = 1;
			}
			else if(pFile_Info[file_idx].szNewName[0] == NONE_EXISTING_FILE)	{
				if(Python_IO_Debug)	printf("DBG> my_open_libc cached, %s  oflags = %o file DOES NOT exist.\n", pathname, oflags);
				errno = ENOENT;	// No such file or directory
				return -1;
			}
			else if(pFile_Info[file_idx].szNewName[0] == UNDER_PREPARING)	{
				if(Python_IO_Debug)	printf("DBG> my_open_libc cached, %s  oflags = %o. File is not ready yet. Read original file.\n", pathname, oflags);
				if(two_args)	{
					ret = real_open_libc(pathname, oflags);
				}
				else	{
					ret = real_open_libc(pathname, oflags, mode);
				}
			}
		}
		else {
			To_Save = 1;
		}
	}
	if(Python_IO_Debug)	printf("DBG> my_open_libc noncached, %s  oflags = %o\n", pathname, oflags);

	if(two_args)	{
		ret = real_open_libc(pathname, oflags);
	}
	else	{
		ret = real_open_libc(pathname, oflags, mode);
	}

	if(To_Save)	{
		if(ret >= 0)	{
			ret_stat = stat(pathname, &file_stat);
			if( (file_stat.st_mode & S_IFMT) == S_IFREG)	{	// regular file
				if( file_stat.st_size <= MAX_BYTES_PER_FILE_TO_CACHE)	{	// Cache the file if it is not too large
					if(file_idx >= 0)	Save_File_in_Local(pathname, szNewName, ret, &file_stat, real_open_libc, file_idx);
					else	Save_File_in_Local(pathname, szNewName, ret, &file_stat, real_open_libc, -1);	// put an invalid file_idx
					ret = real_open_libc(szNewName, oflags);
				}
			}
		}
		else	{
			if(Python_IO_Cache_Nonexisting_File && (errno == ENOENT) )	{
				if(Is_CWD_Match)	{
					if(Is_File_py_pyc_so(pathname) == 0)	{
						return ret;	// Do NOT cache files other than .py, .pyc and .so
					}
				}

//				h = XXH64(pathname, strlen(pathname), 0);
//				idx_lock = h % MAX_NUM_LOCK;
				idx_lock = 0;
				
				pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));
				file_idx = DictInsertAuto(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
				pFile_Info[file_idx].szNewName[0] = NONE_EXISTING_FILE;
				(*nFileRec) = (*nFileRec) + 1;
				(*nFileRec_Acc) = (*nFileRec_Acc) + 1;
				pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
				if(Python_IO_Debug)	printf("DBG> my_open_libc caching none-existing file %s\n", pathname);
				errno = ENOENT;
			}
		}
	}

	return ret;
}

int my_open_pthread(const char *pathname, int oflags, ...)
{
	int mode = 0, two_args=1, ret, ret_stat, file_idx, To_Save=0, idx_lock, Is_CWD_Match;
	struct stat file_stat;
	char szNewName[128];
//	unsigned long long h;

	if (oflags & O_CREAT)	{
		va_list arg;
		va_start (arg, oflags);
		mode = va_arg (arg, int);
		va_end (arg);
		two_args=0;

		if(Python_IO_Debug)	printf("DBG> my_open_pthread noncached, %s. Creating. oflags = %o\n", pathname, oflags);
		ret = real_open_pthread(pathname, oflags, mode);

		if(Python_IO_Cache_Nonexisting_File && (ret>=0) )	{	// Check whether this file is in our cache. If found, remove it. 
			
			if( Is_Target_Dir_Included(pathname, &Is_CWD_Match) && (! Is_Target_FileName_Excluded_in_Open(pathname)) )	{
				file_idx = DictSearch(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
				if(file_idx >= 0)	{	// record found. Remove it!
//					h = XXH64(pathname, strlen(pathname), 0);
//					idx_lock = h % MAX_NUM_LOCK;
					idx_lock = 0;

					pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));
					if(pFile_Info[file_idx].szNewName[0] > 0)	{	// both file and stat are in record
						unlink(pFile_Info[file_idx].szNewName);	// remove local file in cache
					}
					DictDelete(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
					(*nFileRec) = (*nFileRec) - 1;
					pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
					if(Python_IO_Debug)	printf("DBG> my_open_pthread removing %s from cache due to update\n", pathname);
				}
			}
		}
		return ret;
	}

	if( two_args && Is_Target_Dir_Included(pathname, &Is_CWD_Match) && ( ! Is_Target_FileName_Excluded_in_Open(pathname) ) )	{
		file_idx = DictSearch(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
		if(file_idx >= 0)	{
			if(pFile_Info[file_idx].szNewName[0] > 0)	{
				if(Python_IO_Debug)	printf("DBG> my_open_pthread cached, %s  oflags = %o\n", pathname, oflags);
				return real_open_pthread(pFile_Info[file_idx].szNewName, oflags);	// both file and stat are in record
			}
			else if(pFile_Info[file_idx].szNewName[0] == 0)	{	// only stat in rec. Need to copy file locally
				To_Save = 1;
			}
			else if(pFile_Info[file_idx].szNewName[0] == NONE_EXISTING_FILE)	{
				if(Python_IO_Debug)	printf("DBG> my_open_pthread cached, %s  oflags = %o file DOES NOT exist.\n", pathname, oflags);
				errno = ENOENT;	// No such file or directory
				return -1;
			}
			else if(pFile_Info[file_idx].szNewName[0] == UNDER_PREPARING)	{
				if(Python_IO_Debug)	printf("DBG> my_open_pthread cached, %s  oflags = %o. File is not ready yet. Read original file.\n", pathname, oflags);
				if(two_args)	{
					ret = real_open_pthread(pathname, oflags);
				}
				else	{
					ret = real_open_pthread(pathname, oflags, mode);
				}
			}
		}
		else {
			To_Save = 1;
		}
	}
	if(Python_IO_Debug)	printf("DBG> my_open_pthread noncached, %s  oflags = %o\n", pathname, oflags);

	if(two_args)	{
		ret = real_open_pthread(pathname, oflags);
	}
	else	{
		ret = real_open_pthread(pathname, oflags, mode);
	}

	if(To_Save)	{
		if(ret >= 0)	{
			ret_stat = stat(pathname, &file_stat);
			if( (file_stat.st_mode & S_IFMT) == S_IFREG)	{	// regular file
				if( file_stat.st_size <= MAX_BYTES_PER_FILE_TO_CACHE)	{	// Cache the file if it is not too large
					if(file_idx >= 0)	Save_File_in_Local(pathname, szNewName, ret, &file_stat, real_open_pthread, file_idx);
					else	Save_File_in_Local(pathname, szNewName, ret, &file_stat, real_open_pthread, -1);	// put an invalid file_idx
					ret = real_open_pthread(szNewName, oflags);
				}
			}
		}
		else	{
			if(Python_IO_Cache_Nonexisting_File && (errno == ENOENT) )	{
				if(Is_CWD_Match)	{
					if(Is_File_py_pyc_so(pathname) == 0)	{
						return ret;	// Do NOT cache files other than .py, .pyc and .so
					}
				}

//				h = XXH64(pathname, strlen(pathname), 0);
//				idx_lock = h % MAX_NUM_LOCK;
				idx_lock = 0;
				
				pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));
				file_idx = DictInsertAuto(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
				pFile_Info[file_idx].szNewName[0] = NONE_EXISTING_FILE;
				(*nFileRec) = (*nFileRec) + 1;
				(*nFileRec_Acc) = (*nFileRec_Acc) + 1;
				pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
				if(Python_IO_Debug)	printf("DBG> my_open_pthread caching none-existing file %s\n", pathname);
				errno = ENOENT;
			}
		}
	}

	return ret;
}

int my_xstat_ld(int vers, const char *filename, struct stat *stat_buf)
{
	int file_idx, To_Save=0, ret, Is_CWD_Match;

	if( Is_Target_Dir_Included(filename, &Is_CWD_Match) && ( ! Is_Target_FileName_Excluded_in_Stat(filename) ) )	{
		file_idx = DictSearch(p_Hash_File, filename, &elt_list_file, &ht_table_file);
		if(file_idx >= 0)	{
			if(Python_IO_Debug)	printf("DBG> my_xstat_ld() cached %s\n", filename);
			memcpy(stat_buf, &(pFile_Info[file_idx].st), sizeof(struct stat));	// read stat in memory
			if( stat_buf->st_ino == INVALID_INODE)	{
				errno = stat_buf->st_dev;
				return stat_buf->st_uid;
			}
			else	return 0;
		}
		else {
			To_Save = 1;
		}
	}

	if(Python_IO_Debug)	printf("DBG> my_xstat_ld() non-cached %s\n", filename);
	ret = real_xstat_ld(vers, filename, stat_buf);
	if(ret != 0)	{
		stat_buf->st_ino = INVALID_INODE;	// invalid stat flag
		stat_buf->st_dev = errno;	// errno
		stat_buf->st_uid = ret;		// return value
	}

//	if( (ret==0) && To_Save )	{	// no error to call stat() and need to save
	if( To_Save )	{	// no error to call stat() and need to save
		Save_File_Stat_in_Memory(filename, stat_buf, -1);	// Add record!
	}

	return ret;
}

int my_xstat_libc(int vers, const char *filename, struct stat *stat_buf)
{
	int file_idx, To_Save=0, ret, Is_CWD_Match;


	if( Is_Target_Dir_Included(filename, &Is_CWD_Match) && ( ! Is_Target_FileName_Excluded_in_Stat(filename) ) )	{
		file_idx = DictSearch(p_Hash_File, filename, &elt_list_file, &ht_table_file);
		if(file_idx >= 0)	{
			if(pFile_Info[file_idx].szNewName[0] >= 0)	{
				if(Python_IO_Debug)	printf("DBG> my_xstat_libc() cached %s\n", filename);
				memcpy(stat_buf, &(pFile_Info[file_idx].st), sizeof(struct stat));	// read stat in memory
				if( stat_buf->st_ino == INVALID_INODE)	{
					errno = stat_buf->st_dev;
					return stat_buf->st_uid;
				}
				else	return 0;
			}
			else if(pFile_Info[file_idx].szNewName[0] == NONE_EXISTING_FILE)	{
				if(Python_IO_Debug)	printf("DBG> my_xstat_libc() cached %s as a non-existing file\n", filename);
				errno = ENOENT;
				return -1;
			}
			else	{
				if(Python_IO_Debug)	printf("DBG> my_xstat_libc() Unexpected condition.\n");
			}
		}
		else {
			To_Save = 1;
		}
	}

	if(Python_IO_Debug)	printf("DBG> my_xstat_libc() non-cached %s\n", filename);
	ret = real_xstat_libc(vers, filename, stat_buf);
	if(ret != 0)	{
		stat_buf->st_ino = INVALID_INODE;	// invalid stat flag
		stat_buf->st_dev = errno;	// errno
		stat_buf->st_uid = ret;		// return value
	}

//	if( (ret==0) && To_Save )	{	// no error to call stat() and need to save.
	if( To_Save )	{	// Caching non-existing files too! Could be a little risky!
		Save_File_Stat_in_Memory(filename, stat_buf, -1);	// Add record
	}

	return ret;
}

DIR * my_opendir(const char *name)
{
	int dir_idx, To_Save=0, Is_CWD_Match;
	DIR *p_Dir;

//	printf("DBG> my_opendir() %s\n", name);

	if( Is_Target_Dir_Included(name, &Is_CWD_Match) )	{
		dir_idx = DictSearch(p_Hash_Dir, name, &elt_list_dir, &ht_table_dir);
		if(dir_idx >= 0)	{	// exist
			p_Dir = (DIR *)malloc(sizeof(DIR));
			p_Dir->fd = FAKE_DIR_FD + dir_idx;
			p_Dir->size = pDir_Info[dir_idx].nEntries;
			p_Dir->offset = 0;	// starting from the first entry

			if(Python_IO_Debug)	printf("DBG> my_opendir() cached %s\n", name);
			return p_Dir;
		}
		else {
			To_Save = 1;
		}
	}

	p_Dir = real_opendir(name);
	if(Python_IO_Debug)	printf("DBG> my_opendir() noncached %s\n", name);

	if( p_Dir && To_Save )	{	// read dir entries and save
		dir_idx = Save_Dir_Info_in_Memory(name, p_Dir);
		real_closedir(p_Dir);

		p_Dir = (DIR *)malloc(sizeof(DIR));
		p_Dir->fd = FAKE_DIR_FD + dir_idx;
		p_Dir->size = pDir_Info[dir_idx].nEntries;
		p_Dir->offset = 0;	// starting from the first entry
	}

	return p_Dir;
}

int my_closedir(DIR *dirp)
{
	if(dirp->fd >= FAKE_DIR_FD)	{
		dirp->fd = -1;
		free(dirp);
		return 0;
	}
	else	return real_closedir(dirp);
}

__thread dirent one_dirent;
dirent * my_readdir(DIR *dirp)
{
	if(dirp->fd >= FAKE_DIR_FD)	{
		if(dirp->offset < dirp->size )	{
			int offset_in_buff, dir_idx;
			dir_idx = dirp->fd - FAKE_DIR_FD;
			offset_in_buff = pDir_Info[dir_idx].OffsetList[dirp->offset];
			strcpy(one_dirent.d_name, (pDir_Info[dir_idx].szEntryList + offset_in_buff));
			dirp->offset ++;
			return &one_dirent;
		}
		else	return NULL;
	}
	else	return (dirent *)(real_readdir(dirp));
}

void my_rewinddir(DIR *dirp)
{
	if(dirp->fd >= FAKE_DIR_FD)	{
		dirp->offset = 0;
	}
	else	real_rewinddir(dirp);
	return;
}

int my_rename(const char *szOldName, const char *szNewName)
{
	int file_idx, fd, ret, ret_stat, Is_CWD_Match;
	char *pSub;
	char szNewLocalFileName[128];
	struct stat file_stat;

	ret = real_rename(szOldName, szNewName);
	if(ret == 0)	{	// success real_rename()
		pSub = strstr(szNewName, ".pyc");
		if(pSub)	{
			if( pSub[4]==0 )	{	// Ending with ".pyc"
				if( Is_Target_Dir_Included(szNewName, &Is_CWD_Match) )	{
					file_idx = DictSearch(p_Hash_File, szNewName, &elt_list_file, &ht_table_file);
					if(file_idx >= 0)	{	// already in our record. Need to update the file
						ret_stat = stat(szNewName, &file_stat);
						if(ret_stat != 0)	{
							printf("DBG> WARNING: Fail to get stat info for file %s in my_rename().\n", szNewName);
							return ret;
						}

						if(pFile_Info[file_idx].szNewName[0])	{	// file saved in cache? Then update the file
							if( (file_stat.st_mode & S_IFMT) == S_IFREG)	{	// regular file
								fd = real_open_pthread(szNewName, O_RDONLY);
								if(fd < 0)	{
									printf("DBG> WARNING: Fail to get stat info for file %s in my_rename().\n", szNewName);
									return ret;
								}
								Save_File_in_Local(szNewName, szNewLocalFileName, fd, &file_stat, real_open_pthread, file_idx);
							}
						}
						else	{	// only update stat info
							Save_File_Stat_in_Memory(szNewName, &file_stat, file_idx);
						}
						if(Python_IO_Debug)	printf("DBG> my_rename() Update file %s in our cache\n", szNewName);
					}
				}
			}
		}
	}

	return ret;
}

static __attribute__((constructor)) void init_myhook()
{
	char szExeName[1024], *szTargetDirEnv, *szLocalDir_Local, *szPython_IO_Debug, *szPython_Cache_Nonexisting_File, *szPython_Cache_CWD, *szPythonPath;

	pid = getpid();
	Get_Exe_Name(pid, szExeName);

	if( strstr(szExeName, "python")  )	{	// Only intercept "python" processes!!!
//		init_hook();

		Get_Hostname();
		if( (szHostName[0] != 'c') || (szHostName[4] != '-') )	{	// NOT a compute node
			return;
		}

		size_pt_mutex_t = sizeof(pthread_mutex_t);
		nMaxOpen = sysconf(_SC_OPEN_MAX);
		uid = getuid();	// ~ 10 us

		szTargetDirEnv = getenv("PYTHON_IO_TargetDir");
		if(szTargetDirEnv)	Parse_Target_Dir_String(szTargetDirEnv);

		szPythonPath = getenv("PYTHONPATH");
		if(szPythonPath)	{
			if(strstr(szPythonPath, "python"))	{
				if(Python_IO_Debug)	printf("DBG> PYTHONPATH = %s\n", szPythonPath);
				strcpy(szTargetDir[nTargetDir], szPythonPath);	// append PYTHONPATH
				nTargetDir++;
			}
		}

		szLocalDir_Local = getenv("PYTHON_IO_LocalDir");
		if( szLocalDir_Local==NULL )	{
			strcpy(szLocalDir, "/dev/shm");
		}
		else	strcpy(szLocalDir, szLocalDir_Local);

		szPython_IO_Debug = getenv("PYTHON_IO_DEBUG");
		if(szPython_IO_Debug)	{
			if(strcmp(szPython_IO_Debug, "1")==0)	Python_IO_Debug = 1;
		}

		szPython_Cache_Nonexisting_File = getenv("PYTHON_IO_CACHE_NONEXISTING_FILE");
		if(szPython_Cache_Nonexisting_File)	{
			if(strcmp(szPython_Cache_Nonexisting_File, "1")==0)	Python_IO_Cache_Nonexisting_File = 1;
		}

		szPython_Cache_CWD = getenv("PYTHON_IO_CACHE_CWD");
		if(szPython_Cache_CWD)	{
			if(strcmp(szPython_Cache_CWD, "1")==0)	Python_IO_Cache_CWD = 1;
		}
		getcwd(szCWD, 1023);

		Get_Parent_Dir();	// append parent dir into the target dir list
		Get_Home_Dir();	// append HOME local python dir into the target dir list
		
		Init_Shared_Data();
		
                register_a_hook("ld", "open64", (void*)my_open_ld, (long int *)(&real_open_ld));
                register_a_hook("libc", "open64", (void*)my_open_libc, (long int *)(&real_open_libc));
                register_a_hook("libpthread", "open64", (void*)my_open_pthread, (long int *)(&real_open_pthread));

                register_a_hook("ld", "__xstat64", (void*)my_xstat_ld, (long int *)(&real_xstat_ld));
                register_a_hook("libc", "__xstat64", (void*)my_xstat_libc, (long int *)(&real_xstat_libc));

                register_a_hook("libc", "opendir", (void*)my_opendir, (long int *)(&real_opendir));
                register_a_hook("libc", "closedir", (void*)my_closedir, (long int *)(&real_closedir));
                register_a_hook("libc", "readdir", (void*)my_readdir, (long int *)(&real_readdir));
                register_a_hook("libc", "rewinddir", (void*)my_rewinddir, (long int *)(&real_rewinddir));

                register_a_hook("libc", "rename", (void*)my_rename, (long int *)(&real_rename));

                install_hook();
	}
}

static __attribute__((destructor)) void finalize_myhook()
{
	uninstall_hook();
}

void Get_Exe_Name(int pid, char szName[])
{
	FILE *fIn;
	char szPath[1024], *ReadLine;
	
	sprintf(szPath, "/proc/%d/cmdline", pid);
	fIn = fopen(szPath, "r");
	if(fIn == NULL)	{
		printf("Fail to open file: %s\nQuit\n", szPath);
		exit(1);
	}
	
	ReadLine = fgets(szName, 1023, fIn);
	fclose(fIn);
	
	if(ReadLine == NULL)	{
		printf("Fail to determine the executable file name.\nQuit\n");
		exit(1);
	}
}

void Verify_Files(void)
{
	int i, j, idx, nBytes_Read_Org, nBytes_Read_Saved, FileSize, fd, same;
	char *szBuff, *szBuffSaved;

	szBuff = malloc(200000000);
	szBuffSaved = malloc(200000000);

	for(i=0; i<p_Hash_File->n; i++)	{
		idx = elt_list_file[i].value;
		if(pFile_Info[idx].szNewName[0])	{
			FileSize = pFile_Info[idx].st.st_size;
			fd = open(elt_list_file[i].key, O_RDONLY);
			nBytes_Read_Org = read_all(fd, szBuff, FileSize);
			if(nBytes_Read_Org != FileSize)	{
				printf("nBytes_Read_Org != FileSize for file %s\n", elt_list_file[i].key);
				exit(1);
			}
			close(fd);
			fd = open(pFile_Info[idx].szNewName, O_RDONLY);
			nBytes_Read_Saved = read_all(fd, szBuffSaved, FileSize);
			if(nBytes_Read_Saved != FileSize)	{
				printf("nBytes_Read_Saved != FileSize for file %s\n", pFile_Info[idx].szNewName);
				exit(1);
			}
			close(fd);

			same = 1;
			for(j=0; j<FileSize; j++)	{
				if(szBuffSaved[j] != szBuff[j])	{
					same = 0;
					printf("DBG> Failed in checking file %d: %s %s. \n", i, elt_list_file[i].key, pFile_Info[idx].szNewName);
					break;
				}
			}
			if(same)	printf("DBG> Passed in checking file %d: %s %s. \n", i, elt_list_file[i].key, pFile_Info[idx].szNewName);;
		}
	}

	free(szBuff);
	free(szBuffSaved);
}

static void Init_Shared_Data(void)
{
	int To_Init=0;
	long int i;
	int nBytes_Hash_Table_File, nBytes_Hash_Table_Dir;
	
	sprintf(mutex_name, "/python_io_mutex_%d", uid);

	nBytes_Hash_Table_File = sizeof(struct dict) + sizeof(int)*MAX_NUM_FILE + sizeof(struct elt)*MAX_NUM_FILE;
	nBytes_Hash_Table_Dir = sizeof(struct dict) + sizeof(int)*MAX_NUM_DIR + sizeof(struct elt)*MAX_NUM_DIR;
	
	nSize_Shared_Data = size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*4 + sizeof(long int)*2 + nBytes_Hash_Table_File + nBytes_Hash_Table_Dir 
						+ sizeof(FILE_INFO)*MAX_NUM_FILE + sizeof(DIR_INFO)*MAX_NUM_DIR + sizeof(int)*MAX_NUM_LOCK;
//	printf("DBG> nSize_Shared_Data = %d\n", nSize_Shared_Data);

	shm_fd = shm_open(mutex_name, O_RDWR, 0664);
	
	if(shm_fd < 0) {	// failed
		shm_fd = shm_open(mutex_name, O_RDWR | O_CREAT | O_EXCL, 0664);	// create 
		
		if(shm_fd == -1)	{
			if(errno == EEXIST)	{	// file exists
//				Take_a_Short_Nap(100);
				shm_fd = shm_open(mutex_name, O_RDWR, 0664); // try openning file again
				if(shm_fd == -1)    {
					printf("Fail to create file with shm_open().\n");
					exit(1);
				}
			}
			else	{
				printf("DBG> Unexpected here! errno = %d\n\n", errno);
			}
		}
		else {
			To_Init = 1;
			if (ftruncate(shm_fd, nSize_Shared_Data) != 0) {
				perror("ftruncate");
			}
		}
	}
//	else
//		Take_a_Short_Nap(100);
	
	// Map mutex into the shared memory.
	p_shm = mmap(NULL, nSize_Shared_Data, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
	//  p_shm = mmap(NULL, nSize_Shared_Data, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, shm_fd, 0);
	if (p_shm == MAP_FAILED) {
		perror("mmap");
	}
	
	p_futex_open_stat = (pthread_mutex_t *)p_shm;
	p_futex_dirlist = (pthread_mutex_t *)(p_shm+size_pt_mutex_t*MAX_NUM_LOCK);

	int *p_mutex_attr;
	pthread_mutexattr_t mattr;
	p_mutex_attr = (int *)(&mattr);
	*p_mutex_attr = PTHREAD_MUTEXATTR_FLAG_PSHARED;	// PTHREAD_PROCESS_SHARED !!!!!!!!!!!!!!! Shared between processes

	nFileRec = (int *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK);
	nFileStat = (int *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int));
	nDir = (int *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*2);
	pInit_Done = (int *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*3);
	nFileSize = (long int *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*4);
	nFileRec_Acc = (long int *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*4 + sizeof(long int));

	p_Hash_File = (struct dict *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*4 + sizeof(long int)*2);
	p_Hash_Dir = (struct dict *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*4 + sizeof(long int)*2 + nBytes_Hash_Table_File);

	pFile_Info = (FILE_INFO *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*4 + sizeof(long int)*2 + nBytes_Hash_Table_File + nBytes_Hash_Table_Dir);
	pDir_Info = (DIR_INFO *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*4 + sizeof(long int)*2 + nBytes_Hash_Table_File + nBytes_Hash_Table_Dir + sizeof(FILE_INFO)*MAX_NUM_FILE);

	Stop_Sign_List = (int *)(p_shm + size_pt_mutex_t*2*MAX_NUM_LOCK + sizeof(int)*4 + sizeof(long int)*2 + nBytes_Hash_Table_File + nBytes_Hash_Table_Dir + sizeof(FILE_INFO)*MAX_NUM_FILE + sizeof(DIR_INFO)*MAX_NUM_DIR);

	if(To_Init)	{
		*pInit_Done = 0;
		Init_Cache_Dir();

		DictCreate(p_Hash_File, MAX_NUM_FILE, &elt_list_file, &ht_table_file);
		DictCreate(p_Hash_Dir, MAX_NUM_DIR, &elt_list_dir, &ht_table_dir);

		*nFileRec = 0;
		*nFileStat = 0;
		*nDir = 0;
		*nFileSize = 0;
		*nFileRec_Acc = 0;

		for(i=0; i<MAX_NUM_LOCK; i++)	{
			pthread_mutex_init(&(p_futex_open_stat[i]), &mattr);
			pthread_mutex_init(&(p_futex_dirlist[i]), &mattr);
		}

		*pInit_Done = TAG_INIT_DONE;

		for(i=0; i<MAX_NUM_LOCK; i++)	{
			Stop_Sign_List[i] = 0;
		}
	}
	else	{
//		DictCreate(p_Hash_File, 0, &elt_list_file, &ht_table_file);
//		DictCreate(p_Hash_Dir, 0, &elt_list_dir, &ht_table_dir);
		while( *pInit_Done != TAG_INIT_DONE)	{
			Take_a_Short_Nap(300);
		}
                DictCreate(p_Hash_File, 0, &elt_list_file, &ht_table_file);
                DictCreate(p_Hash_Dir, 0, &elt_list_dir, &ht_table_dir);

	}

//	Verify_Files();
}

static void Close_Shared_Mutex(void)
{
	if( (unsigned long int)p_shm > 0x800000 ) {	// special case for vim 
		if ( munmap(p_shm, nSize_Shared_Data) ) {
			perror("munmap");
		}
	}
	p_shm = NULL;
	if (close(shm_fd)) {
		perror("close");
	}
	shm_fd = 0;	
}


static void Take_a_Short_Nap(int nsec)
{
    tim1.tv_sec = 0;
    tim1.tv_nsec = nsec;
    nanosleep(&tim1, &tim2);
}

inline int Is_Target_Dir_Included(const char szFileName[], int *MatchCWD)
{
	int i;

	for(i=0; i<nTargetDir; i++)	{
		if(strstr(szFileName, szTargetDir[i]))	{
			*MatchCWD = 0;
			return 1;
		}
	}
	if(Python_IO_Cache_CWD && (strstr(szFileName, szCWD)) )	{	
		*MatchCWD = 1;
		return 1;
	}

	return 0;
}

void Parse_Target_Dir_String(char szStr[])
{
	int iPos=0, PrePos=0;

	nTargetDir = 0;
	while(1)	{
		if( (szStr[iPos] == 0) || (szStr[iPos] == ':') )	{
			if( iPos > PrePos )	{
				memcpy(szTargetDir[nTargetDir], szStr+PrePos, iPos-PrePos);
				szTargetDir[nTargetDir][iPos-PrePos] = 0;
				if(szTargetDir[nTargetDir][iPos-PrePos-1] == '/')	{
					szTargetDir[nTargetDir][iPos-PrePos-1] = 0;
				}
				nTargetDir++;
			}
			PrePos = iPos;
			if(szStr[iPos] == 0)	break;
			else	PrePos++;	// skip ':'
		}
		iPos++;
	}
}

inline int Is_File_py_pyc_so(const char szFileName[])
{
	int nlen;

	nlen = strlen(szFileName);
	
	if( strncmp(szFileName + nlen - 3, ".so", 3) == 0 )	{
		return 1;
	}
	else if( strncmp(szFileName + nlen - 3, ".py", 3) == 0 )	{
		return 1;
	}
	else if( strncmp(szFileName + nlen - 4, ".pyc", 4) == 0 )	{
		return 1;
	}
	else	{
		return 0;
	}
}

inline int Is_Target_FileName_Excluded_in_Open(const char szFileName[])
{
	int i=0;

	while(szFileName[i])	{
		if(szFileName[i] == '.')	{
//			if( (szFileName[i+1] == 'p') && (szFileName[i+2] == 'y') && ( (szFileName[i+3] == 0) || ( (szFileName[i+3] == 'c') && (szFileName[i+4] >= '0')  && (szFileName[i+4] <= '9') ) ) )	{
			if( (szFileName[i+1] == 'p') && (szFileName[i+2] == 'y') && ( ( (szFileName[i+3] == 'c') && (szFileName[i+4] >= '0')  && (szFileName[i+4] <= '9') ) ) )	{
				return 1;
			}
		}

		i++;
	}
	return 0;
}

inline int Is_Target_FileName_Excluded_in_Stat(const char szFileName[])
{
	int i=0;

	while(szFileName[i])	{
		if(szFileName[i] == '.')	{
//			if( (szFileName[i+1] == 'p') && (szFileName[i+2] == 'y') && ( (szFileName[i+3] == 0) || ( (szFileName[i+3] == 'c') && (szFileName[i+4] >= '0')  && (szFileName[i+4] <= '9') ) ) )	{
			if( (szFileName[i+1] == 'p') && (szFileName[i+2] == 'y') && (szFileName[i+3] == 'c') && (szFileName[i+4] >= '0')  && (szFileName[i+4] <= '9')  )	{
				return 1;
			}
		}

		i++;
	}
	return 0;
}

static void Save_File_in_Local(const char szFileName[], char szNewName[], int fd, struct stat *file_stat, org_open real_open, int file_idx)
{
	int fd_new, FileSize, mode, IsNewRecord=0;
	int idx_lock, rec_idx, Bytes_Read, Bytes_Written, nFileNameLen;
	char *szBuff;
	unsigned long long h;

	nFileNameLen = strlen(szFileName);
	if(nFileNameLen >= MAX_NAME_LEN)	{
		printf("ERROR: Save_File_in_Local(), nFileNameLen (%d) >= MAX_NAME_LEN\nNeed to increase MAX_NAME_LEN. Quit\n%s\n", nFileNameLen, szFileName);
		exit(1);
	}

	h = XXH64(szFileName, strlen(szFileName), 0);
	FileSize = file_stat->st_size;
	mode = file_stat->st_mode & S_IRWXU;
	idx_lock = 0;
//	idx_lock = h % MAX_NUM_LOCK;
//	pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));

	if(file_idx < 0)	{	// not in our list. Try search hash table again. 
		file_idx = DictSearch(p_Hash_File, szFileName, &elt_list_file, &ht_table_file);	// search again before inserting 
	}

	if(file_idx >= 0)	{	// record exist. Only need to copy file. 
		if(pFile_Info[file_idx].szNewName[0])	{	// file exists. Do nothing. 
			close(fd);
			strcpy(szNewName, pFile_Info[file_idx].szNewName);
			pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
			return;
		}
		else	{	// Only stat exists. Copy file and update szNewName. Do NOT insert new record!!!
			IsNewRecord = 0;
			rec_idx = file_idx;
		}
	}
	else	{				// No record. Add both file and stat. 
		IsNewRecord = 1;
//		rec_idx = *nFileRec;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! NEED CHANGES!!!!!!!
//		(*nFileRec) = (*nFileRec) + 1;	// add a new record
//		(*nFileRec_Acc) = (*nFileRec_Acc) + 1;	// add a new record
		pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));
		(*nFileRec) = (*nFileRec) + 1;	// add a new record
		(*nFileRec_Acc) = (*nFileRec_Acc) + 1;	// add a new record
		rec_idx = DictInsertAuto(p_Hash_File, szFileName, &elt_list_file, &ht_table_file);	// insert new record in hash table
		pFile_Info[rec_idx].szNewName[0] = UNDER_PREPARING;
		memcpy(&(pFile_Info[rec_idx].st), file_stat, sizeof(struct stat));	// Update file stat anyway
		pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
	}
	
	szBuff = (char*)malloc(FileSize);
	Bytes_Read = read_all(fd, szBuff, FileSize);
	close(fd);
	
	if(Bytes_Read != FileSize)	{
		printf("Fatal error> Error to read file %s\nQuit\n", szFileName);
		free(szBuff);
//		pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
		exit(1);
	}
	
	sprintf(szNewName, "%s/pycache_%d/sub_%d/%d", szLocalDir, uid, rec_idx%N_CACHE_DIR, rec_idx);
	fd_new = real_open(szNewName, O_CREAT | O_RDWR, mode);
	if(fd_new < 0)	{
		free(szBuff);
		printf("Fail to open new file for %s. d->n = %d\nQuit\n", szFileName, p_Hash_File->n);
//		pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
		exit(1);
	}
	Bytes_Written = write_all(fd_new, szBuff, FileSize);
	close(fd_new);
	if(Bytes_Read != FileSize)	{
		printf("Fatal error> Error to write local file for %s\nQuit\n", szFileName);
		free(szBuff);
//		pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
		exit(1);
	}
	free(szBuff);
	strcpy(pFile_Info[rec_idx].szNewName, szNewName);
	memcpy(&(pFile_Info[rec_idx].st), file_stat, sizeof(struct stat));	// Update file stat anyway
//	if(IsNewRecord)	{
//		pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));
//		memcpy(&(pFile_Info[rec_idx].st), file_stat, sizeof(struct stat));
//		(*nFileRec) = (*nFileRec) + 1;	// add a new record
//		(*nFileRec_Acc) = (*nFileRec_Acc) + 1;	// add a new record
//		pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));
//	}


	if( *nFileRec >= MAX_NUM_FILE)	{
		printf("ERROR: Save_File_in_Local(), *nFileRec >= MAX_NUM_FILE to increase MAX_NUM_FILE. Quit\n");
		exit(1);
	}
}

static void Save_File_Stat_in_Memory(const char szFileName[], struct stat *file_stat, int file_idx)
{
	unsigned long long h;
	int idx_lock, IsNewRecord=1;

	if(file_idx >= 0)	IsNewRecord = 0;
	h = XXH64(szFileName, strlen(szFileName), 0);
//	idx_lock = h % MAX_NUM_LOCK;
	idx_lock = 0;
	pthread_mutex_lock(&(p_futex_open_stat[idx_lock]));

	if(IsNewRecord)	{
		file_idx = DictSearch(p_Hash_File, szFileName, &elt_list_file, &ht_table_file);	// search again before inserting 
		if(file_idx < 0)	{	// NOT in our list. Read then save into memory
			file_idx = DictInsertAuto(p_Hash_File, szFileName, &elt_list_file, &ht_table_file);
			pFile_Info[file_idx].szNewName[0] = 0;	// only STAT info is stored in memory. File is NOT stored. 
			memcpy(&(pFile_Info[file_idx].st), file_stat, sizeof(struct stat));
			(*nFileRec) = (*nFileRec) + 1;
			(*nFileRec_Acc) = (*nFileRec_Acc) + 1;
		}
//		else	{
//			memcpy(&(pFile_Info[file_idx].st), file_stat, sizeof(struct stat));
//		}
	}
//	else	{
//		memcpy(&(pFile_Info[file_idx].st), file_stat, sizeof(struct stat));
//	}
	pthread_mutex_unlock(&(p_futex_open_stat[idx_lock]));

	if( *nFileRec >= MAX_NUM_FILE)	{
		printf("ERROR: Save_File_Stat_in_Memory(), *nFileRec >= MAX_NUM_FILE to increase MAX_NUM_FILE. Quit\n");
		exit(1);
	}
}

static int Save_Dir_Info_in_Memory(const char szPathName[], DIR *pDir)
{
	unsigned long long h;
	int idx_lock, dir_idx, idx_Rec, nLenStr;
	dirent *ep;

	h = XXH64(szPathName, strlen(szPathName), 0);
	idx_lock = 0;
//	idx_lock = h % MAX_NUM_LOCK;
	pthread_mutex_lock(&(p_futex_dirlist[idx_lock]));

	idx_Rec = *nDir;
	dir_idx = DictSearch(p_Hash_Dir, szPathName, &elt_list_dir, &ht_table_dir);	// search again before inserting 
	if(dir_idx < 0)	{	// NOT in our list. Read then save into memory 
		pDir_Info[idx_Rec].nEntries = 0;
		pDir_Info[idx_Rec].Offset = 0;	// the offset of free buffer

		while (ep = real_readdir(pDir))	{
			nLenStr = strlen(ep->d_name);
			memcpy(pDir_Info[idx_Rec].szEntryList + pDir_Info[idx_Rec].Offset, ep->d_name, nLenStr+1);	// with NULL
			pDir_Info[idx_Rec].OffsetList[pDir_Info[idx_Rec].nEntries] = pDir_Info[idx_Rec].Offset;
			pDir_Info[idx_Rec].Offset += (nLenStr + 1);	// with NULL included
			pDir_Info[idx_Rec].nEntries ++;
			if(pDir_Info[idx_Rec].nEntries > MAX_NUM_ENTRY_DIR)	{
				printf("pDir_Info[idx_Rec].nEntries > MAX_NUM_ENTRY_DIR\nYou need to increase MAX_NUM_ENTRY_DIR\nQuit\n");
				pthread_mutex_unlock(&(p_futex_dirlist[idx_lock]));
				exit(1);
			}
			if(pDir_Info[idx_Rec].Offset > MAX_ENTRY_BUFF_LEN)	{
				printf("pDir_Info[idx_Rec].Offset > MAX_ENTRY_BUFF_LEN\nYou need to increase MAX_ENTRY_BUFF_LEN\nQuit\n");
				pthread_mutex_unlock(&(p_futex_dirlist[idx_lock]));
				exit(1);
			}
//			printf("DBG> Cache dir %s %d %s\n", szPathName, pDir_Info[idx_Rec].nEntries, ep->d_name);
		}
		dir_idx = (*nDir);
		(*nDir) = (*nDir) + 1;
		DictInsert(p_Hash_Dir, szPathName, idx_Rec, &elt_list_dir, &ht_table_dir);	// Insert the record after all dir entries data in position
	}
	pthread_mutex_unlock(&(p_futex_dirlist[idx_lock]));

	if( *nDir >= MAX_NUM_DIR)	{
		printf("ERROR: Save_Dir_Info_in_Memory(), *nDir >= MAX_NUM_DIR to increase MAX_NUM_DIR. Quit\n");
		exit(1);
	}

	return dir_idx;
}

static ssize_t read_all(int fd, void *buf, size_t count)
{
	ssize_t ret, nBytes=0;

	while (count != 0 && (ret = read(fd, buf, count)) != 0) {
		if (ret == -1) {
			if (errno == EINTR)
				continue;
			perror ("read");
			break;
		}
		nBytes += ret;
		count -= ret;
		buf += ret;
	}
	return nBytes;
}

static ssize_t write_all(int fd, const void *buf, size_t count)
{
	ssize_t ret, nBytes=0;
	void *p_buf;

	p_buf = (void *)buf;
	while (count != 0 && (ret = write(fd, p_buf, count)) != 0) {
		if (ret == -1) {
			if (errno == EINTR)	{
				continue;
			}
			else if (errno == ENOSPC)	{	// out of space. Quit immediately!!!
				return OUT_OF_SPACE;
			}

			perror ("write");
			break;
		}
		nBytes += ret;
		count -= ret;
		p_buf += ret;
	}
	return nBytes;
}

static void Init_Cache_Dir(void)
{
	int i;
	char szDirName[128];

	sprintf(szDirName, "%s/pycache_%d", szLocalDir, uid);
	mkdir(szDirName, S_IRWXU);
	
	for(i=0; i<N_CACHE_DIR; i++)	{
		sprintf(szDirName, "%s/pycache_%d/sub_%d", szLocalDir, uid, i);
		mkdir(szDirName, S_IRWXU);
	}
}

static void Get_Parent_Dir(void)
{
	int nLen, i, Count=0;
	char szExeFullPath[512], szBuff[32];

	sprintf(szBuff, "/proc/%d/exe", pid);
	nLen = readlink(szBuff, szExeFullPath, 511);
//	printf("DBG> Exe Name = %s\n", szExeFullPath);
	i = nLen - 1;
	while(i>=0)	{
		if(szExeFullPath[i] == '/')	{
			szExeFullPath[i] = 0;
			Count++;
		}
		if(Count == 2)	{
			break;
		}
		i--;
	}
	if(Python_IO_Debug)	printf("DBG> Parent_Dir = %s\n", szExeFullPath);
	memcpy(szTargetDir[nTargetDir], szExeFullPath, i);	// append python parent directory
	nTargetDir++;
}

static void Get_Home_Dir(void)
{
	char *szHomeDir, szHomeDirBuff[256];
	
	szHomeDir = getenv("HOME");

	if (szHomeDir == NULL) {
		struct passwd *pw = getpwuid(uid);		
		if (pw != NULL) {
			strcpy(szHomeDirBuff, pw->pw_dir);
		}
		else	return;
	}
	else	{
		strcpy(szHomeDirBuff, szHomeDir);
	}
		
	if(Python_IO_Debug)	printf("DBG> Home_Dir = %s\n", szHomeDirBuff);
	strcat(szHomeDirBuff, "/.local/lib/python");
	strcpy(szTargetDir[nTargetDir], szHomeDirBuff);	// append python parent directory
	nTargetDir++;
}

static void Get_Hostname(void)
{
	int i=0;

	gethostname(szHostName, 255);

	while(szHostName[i] != 0)	{
		if(szHostName[i] == '.')	{
			szHostName[i] = 0;	// truncate hostname[]
			return;
		}
		i++;
	}
}
