/*************************************************************************
--------------------------------------------------------------------------
--  Linux hook License
--------------------------------------------------------------------------
--
--  Linux hook is licensed under the terms of the MIT license reproduced
--  below. This means that show_affinity is free software and can be used 
--  for both academic and commercial purposes at absolutely no cost.
--
--  ----------------------------------------------------------------------
--
--  Copyright (C) 2018-2021 Lei Huang (huang@tacc.utexas.edu)
--
--  Permission is hereby granted, free of charge, to any person obtaining
--  a copy of this software and associated documentation files (the
--  "Software"), to deal in the Software without restriction, including
--  without limitation the rights to use, copy, modify, merge, publish,
--  distribute, sublicense, and/or sell copies of the Software, and to
--  permit persons to whom the Software is furnished to do so, subject
--  to the following conditions:
--
--  The above copyright notice and this permission notice shall be
--  included in all copies or substantial portions of the Software.
--
--  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
--  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
--  OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
--  NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
--  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
--  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
--  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
--  THE SOFTWARE.
--
--------------------------------------------------------------------------
*************************************************************************/

/*
About: This is a mini framework to hook the functions in shared libraries under Linux. 
It only works on x86_64 at this time. I might extend it to support Power PC and ARM 
in future. udis86 was adopted to disasseble binary code on x86_64. 

Trampoline is used in hook.
*/


#define _GNU_SOURCE

#include <stdio.h>
#include <execinfo.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>
#include <malloc.h>
#include <pthread.h>
#include <sys/time.h>
#include <errno.h>
#include <elf.h>
#include <termios.h>

#include <stdarg.h>

#include <dlfcn.h>
#include <link.h>
#include <fcntl.h>
#include <execinfo.h>
#include <inttypes.h>
#include <assert.h>

#include "../include/hook.h"
#include "hook_int.h"


static int num_hook=0;
static int num_module=0;
static int num_patch_blk=0, is_uninstalled=0;
static int num_called_ModuleMap=0;

static MODULE_PATCH_INFO module_list[MAX_MODULE];

// variables used by udis86
static ud_t ud_obj;
static int ud_idx;

// The buffer to hold binary code that will be decompiled by udis86. 
static char *bin_code_buff=NULL;

// The flag whethere libc.so is found or not. 
static int found_libc=1;

// The instruction to jump to new function. 
//0:  ff 25 00 00 00 00       jmp    QWORD PTR [rip+0x0]        # 6 <_main+0x6>
// The long int +6 (0x1234567812345678) needs be replaced by the address of new function. 
static unsigned char instruction_bounce[]={0xff,0x25,0x00,0x00,0x00,0x00,  \
                                       0x78,0x56,0x34,0x12,0x78,0x56,0x34,0x12};

// The list of memory blocks alloated to hold patches for hook
static PATCH_BLOCK patch_blk_list[MAX_MODULE];


//start	to compile list of memory blocks in /proc/pid/maps
// The max number of libraries loaded
#define MAX_NUM_LIB	(256)

// The max number of segments in /proc/pid/maps
#define MAX_NUM_SEG	(2048)

static int num_seg=0, num_lib_in_map=0;

// List of min and max addreses of segments in /proc/pid/maps
static uint64_t addr_min[MAX_NUM_SEG], addr_max[MAX_NUM_SEG];

// List of base addresses of loaded libraries 
static uint64_t lib_base_addr[MAX_NUM_LIB];

// List of names of loaded libraries 
static char lib_name_list[MAX_NUM_LIB][MAX_LEN_PATH_NAME];
//end	to compile list of memory blocks in /proc/pid/maps

static char path_ld[512]="";
static char path_libc[512]="";
static char path_libpthread[512]="";


/*
 * determine_lib_path - Determine the full paths of three libraries, ld.so, libc.so and libpthread.so. 
 */

static void determine_lib_path(void)
{
	int i, pid, size_read;
	char read_buff_map[8192], path_file_map[64], *pPos=NULL, *pStart=NULL, *pEnd=NULL, lib_ver_str[32], lib_dir_str[256];
	FILE *fp;
	
	pid = getpid();
	snprintf(path_file_map, sizeof(path_file_map), "/proc/%d/maps", pid);
	fp = fopen(path_file_map, "rb");
	assert(fp != NULL);
	
	size_read = fread(read_buff_map, 1, sizeof(read_buff_map)-1, fp);
	fclose(fp);
	
	pPos = strstr(read_buff_map, "/ld-2.");
	if(pPos == NULL)        {
		found_libc = 0;
		return;
	}
	for(i=0; i<10; i++)     {
		if(strncmp(pPos+i, ".so", 3)==0)        {
			pEnd = pPos+i+3;
			break;
		}
	}
	if(pEnd == NULL)        {
		printf("Fail to determine the ending position of libc path.\nQuit\n");
		exit(1);
	}
	for(i=0; i<100; i++)    {
		if(strncmp(pPos-i, "  /", 3)==0)        {
			pStart = pPos-i+2;
			break;
		}
	}
	if(pEnd == NULL)        {
		printf("Fail to determine the starting position of libc path.\nQuit\n");
		exit(1);
	}
	
	memcpy(path_ld, pStart, pEnd-pStart);
	path_ld[pEnd-pStart] = 0;
	memcpy(lib_ver_str, pPos+4, pEnd-3-(pPos+4));
	lib_ver_str[pEnd-3-(pPos+4)] = 0;
	memcpy(lib_dir_str, pStart, pPos-pStart);
	lib_dir_str[pPos-pStart] = 0;
	
	snprintf(path_libc, sizeof(path_libc), "%s/libc-%s.so", lib_dir_str, lib_ver_str);
	snprintf(path_libpthread, sizeof(path_libpthread), "%s/libpthread-%s.so", lib_dir_str, lib_ver_str);
	
	//printf("path_ld = %s\n", path_ld);
	//printf("lib_ver_str = %s\n", lib_ver_str);
	//printf("lib_dir_str = %s\n", lib_dir_str);
	//printf("path_libc = %s\n", path_libc);
	//printf("path_libpthread = %s\n", path_libpthread);
}

/*
 * query_func_addr - Determine the addresses and code sizes of functions in func_name_list[].
 *   @lib_path: The full path of the shared object file
 *   @func_name_list: A list of function names in this lib to be intercepted
 *   @func_addr_list: A list to hold the addreses of functions
 *   @func_len_list:  A list of hold the size (number of bytes) of functions
 *   @img_base_addr:  The base address of this loaded module
 *   @num_func:       The number of functions in the list of func_name_list[]
 * Returns:
 *   void
 */

static void query_func_addr(const char lib_path[], const char func_name_list[][MAX_LEN_FUNC_NAME], void* func_addr_list[], long int func_len_list[], const long int img_base_addr, const int num_func)
{
	int fd, i, j, k;
	struct stat file_stat;
	void *map_start;
	Elf64_Ehdr *header;
	Elf64_Shdr *sections;
	int strtab_offset=0;
	void *symb_base_addr=NULL;
	int num_sym=0, sym_rec_size=0, sym_offset, rec_addr;
	char *sym_name;
	
	stat(lib_path, &file_stat);
	
	fd = open(lib_path, O_RDONLY);
	if(fd == -1)    {
		printf("Fail to open file %s\nQuit\n", lib_path);
		exit(1);
	}
	
	map_start = mmap(0, file_stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if((long int)map_start == -1)   {
		printf("Fail to mmap file %s\nQuit\n", lib_path);
		exit(1);
	}
	header = (Elf64_Ehdr *) map_start;
	
	sections = (Elf64_Shdr *)((char *)map_start + header->e_shoff);
	
	for (i = 0; i < header->e_shnum; i++)	{
		if ( (sections[i].sh_type == SHT_DYNSYM) || (sections[i].sh_type == SHT_SYMTAB) ) {
			symb_base_addr = (void*)(sections[i].sh_offset + map_start);
			sym_rec_size = sections[i].sh_entsize;
			num_sym = sections[i].sh_size / sections[i].sh_entsize;
			
			for (j = i-1; j < i+2; j++)   {	// tricky here!!!
				if ( (sections[j].sh_type == SHT_STRTAB) ) {
					strtab_offset = (int)(sections[j].sh_offset);
				}
			}
			
			// Hash table should be used to be more efficient. 
			for(j=0; j<num_sym; j++) {
				rec_addr = sym_rec_size*j;
				sym_offset = *( (int *)( symb_base_addr + rec_addr ) ) & 0xFFFFFFFF;
				sym_name = (char *)( map_start + strtab_offset + sym_offset );
				
				for(k=0; k<num_func; k++)	{
					if( strcmp(sym_name, func_name_list[k])==0 )      { 
						func_addr_list[k] =  (void*)( ( (long int)( *((int *)(symb_base_addr + rec_addr + 8)) ) & 0xFFFFFFFF ) + img_base_addr);
						func_len_list[k] = (*((int *)(symb_base_addr + rec_addr + 16))) & 0xFFFFFFFF;
					}
				}
			}
		}
	}
	munmap(map_start, file_stat.st_size);
	close(fd);
}

/*
 * uninstall_hook - Uninstall hooks by cleaning up trampolines.
 * Returns:
 *   void
 */

void uninstall_hook(void)
{
	int i, iBlk, iFunc;
	void *pbaseOrg;
	size_t MemSize_Modify;
	TRAMPOLINE *pTrampoline;
	unsigned long int page_size, mask;
	
	if(found_libc==0) return;
	
	if(is_uninstalled == 1)	{
		return;
	}
	is_uninstalled = 1;
	
	page_size = sysconf(_SC_PAGESIZE);
	mask = ~(page_size - 1);
	
	for(iBlk=0; iBlk<num_patch_blk; iBlk++)	{
		pTrampoline = (TRAMPOLINE *)(patch_blk_list[iBlk].patch_addr);
		
		for(iFunc=0; iFunc<patch_blk_list[iBlk].num_trampoline; iFunc++)	{
			pbaseOrg = (void *)( (long int)(pTrampoline[iFunc].addr_org_func) & mask );	// fast mod
			MemSize_Modify = determine_mem_block_size((const void *)(pTrampoline[iFunc].addr_org_func), page_size);
			
			if(pbaseOrg == NULL)	continue;
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_WRITE | PROT_EXEC) != 0)	{	// two pages to make sure the code works when the modified code is around page boundary
				printf("Error in executing p_mp().\n");
				exit(1);
			}
			memcpy(pTrampoline[iFunc].addr_org_func, pTrampoline[iFunc].org_code, 5);	// save orginal code for uninstall 
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_EXEC) != 0)	{
				printf("Error in executing p_mp().\n");
				exit(1);
			}
		}
	}
	
	for(i=0; i<num_patch_blk; i++)	{
		if(patch_blk_list[i].patch_addr)	{
			if ( munmap(patch_blk_list[i].patch_addr, MIN_MEM_SIZE) ) {
				perror("munmap");
			}
			patch_blk_list[i].patch_addr = 0;
		}
	}
	
}

/*
 * query_lib_name_in_list - Query the index of the name of a library in lib_name_list[].
 *   @lib_name_str: The ibrary name
 * Returns:
 *   The index in lib_name_list[]. (-1) means not found in the list. 
 */

static int query_lib_name_in_list(const char *lib_name_str)
{
	int i;

	// Try exact match first
	for(i=0; i<num_lib_in_map; i++)	{
		if(strcmp(lib_name_str, lib_name_list[i])==0)	{
			return i;
		}
	}
	// Try partial match
	for(i=0; i<num_lib_in_map; i++)	{
		if(strstr(lib_name_list[i], lib_name_str))	{
			return i;
		}
	}
	return (-1);
}

/*
 * query_registered_module - Query the index of the name of a library in module_list[].
 *   @lib_name_str: The ibrary name
 * Returns:
 *   The index in module_list[]. (-1) means not found in the list. 
 */

static int query_registered_module(const char *lib_name_str)
{
	int i;
	
	for(i=0; i<num_module; i++)	{
		if(strcmp(lib_name_str, module_list[i].module_name)==0)	{
			return i;
		}
	}
	return (-1);
}

// The max size of read "/proc/%pid/maps". This size set here should be sufficient 
// for normal applications. 
#define MAX_MAP_SIZE	(524288)

/*
 * get_module_maps - Read "/proc/%pid/maps" and extract the names of modules. 
 */

static void get_module_maps(void)
{
	FILE *fIn;
	char szName[64], szBuf[MAX_MAP_SIZE], szLibName[256];
	int iPos, iPos_Save, ReadItem;
	long int FileSize;
	uint64_t addr_B, addr_E;
	
	//	printf("DBG> num_called_ModuleMap = %d\n", num_called_ModuleMap);
	
	snprintf(szName, sizeof(szName), "/proc/%d/maps", getpid());
	fIn = fopen(szName, "rb");	// non-seekable file. fread is needed!!!
	if(fIn == NULL)	{
		printf("Fail to open file: %s\nQuit\n", szName);
		exit(1);
	}
	
	FileSize = fread(szBuf, 1, MAX_MAP_SIZE, fIn);	// fread can read complete file. read() does not most of time!!!
	fclose(fIn);

	if(FileSize == MAX_MAP_SIZE)	{
		printf("Warning> FileSize == MAX_MAP_SIZE\nYou might need to increase MAX_MAP_SIZE.\n");
	}
	
	szBuf[FileSize] = 0;
	//	printf("%s\n\n", szBuf);
	
	num_seg = 0;
	num_lib_in_map = 0;
	szBuf[FileSize] = 0;
	
	iPos = 0;	// start from the beginging
/*	
	while(iPos >= 0)	{
		ReadItem = sscanf(szBuf+iPos, "%lx%lx", &addr_B, &addr_E);
		if(ReadItem != 2)	{
			printf("Error in reading addresses.\n");
		}
		iPos_Save = iPos;
		iPos = get_position_of_next_line(szBuf, iPos + 38, FileSize);	// find the next line
		if( (iPos - iPos_Save) > 73 )	{	// with a lib name
			ReadItem = sscanf(szBuf+iPos_Save+73, "%s", szLibName);
			if( ReadItem == 1 )	{
				if(strstr(szLibName, "/ld-"))	{
					iPos = iPos_Save;
					break;
				}
			}
		}
	}
*/	
	while(iPos >= 0)	{
		ReadItem = sscanf(szBuf+iPos, "%lx-%lx", &addr_B, &addr_E);
		if(ReadItem == 2)	{
			addr_min[num_seg] = addr_B;
			addr_max[num_seg] = addr_E;
			if(num_seg >= 1)	{
				// merge contacted blocks
				if(addr_min[num_seg] == addr_max[num_seg-1])	{
					addr_max[num_seg-1] = addr_max[num_seg];
					num_seg--;
				}
			}
			num_seg++;
		}
		iPos_Save = iPos;
		iPos = get_position_of_next_line(szBuf, iPos + 38, FileSize);	// find the next line
		if( (iPos - iPos_Save) > 73 )	{	// with a lib name
			ReadItem = sscanf(szBuf+iPos_Save+73, "%s", szLibName);
			if( ReadItem == 1 )	{
				if(strncmp(szLibName, "[stack]", 7)==0)	{
					num_seg--;
					break;
				}
				if(query_lib_name_in_list(szLibName) == -1)	{	// a new name not in list
					strcpy(lib_name_list[num_lib_in_map], szLibName);
					lib_base_addr[num_lib_in_map] = addr_B;
					num_lib_in_map++;
					if(num_lib_in_map >= MAX_NUM_LIB)	{
						printf("Warning> lib_base_addr is FULL. You may need to increase MAX_NUM_LIB.\n");
						break;
					}
				}
			}
		}
		if(num_seg >= MAX_NUM_SEG)	{
			printf("Warning> num_seg >= MAX_NUM_LIB\nYou may want to increase MAX_NUM_LIB.\n");
			break;
		}
	}
}
#undef MAX_MAP_SIZE

/*
 * find_usable_block - Try to find an allocated memory block that is close enough to 
   a give module/shared object.
     @idx_mod: The name of shared library. Both short name ("ld") and full name ("ld-2.17.so") are accepted. 
 * Returns:
 *   The index of memory block for patches. (-1) means not found usable block. 
 */

static int find_usable_block(int idx_mod)
{
	int i;
	long int p_Min, p_Max, p_MemBlk;
	
	p_Min = (long int)(module_list[idx_mod].old_func_addr_min);
	p_Max = (long int)(module_list[idx_mod].old_func_addr_max);
	
	for(i=0; i<num_patch_blk; i++)	{
		//		printf("(0x%llx, 0x%llx)\n", patch_blk_list[i].patch_addr, patch_blk_list[i].patch_addr_end);
		p_MemBlk = ( ( (long int)(patch_blk_list[i].patch_addr) + (long int)(patch_blk_list[i].patch_addr_end)) / 2 );
		if( ( labs(p_Min - p_MemBlk) < NULL_RIP_VAR_OFFSET ) && ( labs(p_Max - p_MemBlk) < NULL_RIP_VAR_OFFSET ) )	{
			return i;
		}
	}
	
	return (-1);
}

/*
 * allocate_memory_block_for_patches - Allocated memory blocks to hold the patches for hook.  
 */

static void allocate_memory_block_for_patches(void)
{
	int i, iSeg, idx_mod, IdxBlk;
	void *p_Alloc;
	uint64_t pCheck;
	
	num_patch_blk = 0;
	
	for(idx_mod=0; idx_mod<num_module; idx_mod++)	{
		if( (module_list[idx_mod].old_func_addr_min == 0) && (module_list[idx_mod].old_func_addr_max == 0) )	{
			continue;
		}
		IdxBlk = find_usable_block(idx_mod);
		if(IdxBlk >= 0)	{
			module_list[idx_mod].idx_patch_blk = IdxBlk;
		}
		else	{	// does not exist
			pCheck = ( (uint64_t)(module_list[idx_mod].old_func_addr_min) + (uint64_t)(module_list[idx_mod].old_func_addr_max) )/2;
			
			iSeg = -1;
			for(i=0; i<num_seg; i++)	{
				if( (pCheck >= addr_min[i]) && (pCheck <= addr_max[i]) )	{
					iSeg = i;
					break;
				}
			}
			
			if(iSeg < 0)	{
				printf("Something wrong! The address you queried is not inside any module!\nQuit\n");
				exit(1);
			}
			p_Alloc = (void*)(addr_max[iSeg]);
			
			if( iSeg < (num_seg - 1) )	{
				if( (addr_min[iSeg+1] - addr_max[iSeg]) < MIN_MEM_SIZE)	{
					printf("Only %" PRIu64 " bytes available.\nQuit\n", addr_min[iSeg+1] - addr_max[iSeg]);
					exit(1);
				}
			}
			
			patch_blk_list[num_patch_blk].patch_addr = mmap(p_Alloc, MIN_MEM_SIZE, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
			if(patch_blk_list[num_patch_blk].patch_addr == MAP_FAILED) {
				printf("Fail to allocate code block at %p with mmap().\nQuit\n", p_Alloc);
				exit(1);
			}
			else if(patch_blk_list[num_patch_blk].patch_addr != p_Alloc) {
				printf("Allocated at %p. Desired at %p\n", patch_blk_list[num_patch_blk].patch_addr, p_Alloc);
			}
			
			patch_blk_list[num_patch_blk].num_trampoline = 0;
			patch_blk_list[num_patch_blk].patch_addr_end = patch_blk_list[num_patch_blk].patch_addr + MIN_MEM_SIZE;
			num_patch_blk++;
		}
	}
	
	return;
}

/*
 * get_position_of_next_line - Determine the offset of the next new line in a string buffer.
 *   @buff: The string buffer
 *   @pos_start: The starting offset to search
 *   @max_buff_size: The max length of buff[]
 * Returns:
 *   The offset of the next new line. (-1) means reaching the end of buffer. 
 */
static int get_position_of_next_line(const char buff[], const int pos_start, const int max_buff_size)
{
	int i=pos_start;
	
	while(i < max_buff_size)	{
		// A new line
		if(buff[i] == 0xA)	{
			i++;
			return ( (i>=max_buff_size) ? (-1) : (i) );
		}
		else	{
			i++;
		}
	}
	return (-1);
}

/*
 * determine_mem_block_size - Determine we need to change the permission of one or two pages 
 * for a given address.
 *   @addr: The address of the entry of original function. We will change it to a jmp instruction. 
 *   @page_size: The size of one page in current system. 
* Returns:
 *   The number of bytes we need to change permission with mprotect(). 
 */
static size_t determine_mem_block_size(const void *addr, const unsigned long int page_size)
{
	unsigned long int res, addr_code;
	
	addr_code = (unsigned long int)addr;
	res = addr_code % page_size;
	if( (res + 5) > page_size )	{	// close to the boundary of two memory pages 
		return (size_t)(page_size*2);
	}
	else	{
		return (size_t)(page_size);
	}
}

// The max number of instruments of the entry code in original function to analyze 
#define MAX_INSTUMENTS	(24)

/*
 * install_hook - Install hooks by setting up trampolines for all functions registered.
 * Returns:
 *   The number of hooks actually installed. 
 */

int install_hook(void)
{
	int j, idx_mod, iFunc, iFunc2, jMax, ReadItem, *p_int, WithJmp[MAX_PATCH];
	int nInstruction, RIP_Offset, Jmp_Offset, nFunc_InBlk, num_hook_installed=0;
	int OffsetList[MAX_INSTUMENTS];
	char szHexCode[MAX_INSTUMENTS][64], szInstruction[MAX_INSTUMENTS][128];
	char *pSubStr=NULL, *pOpOrgEntry;
	void *pbaseOrg;
	size_t MemSize_Modify;
	TRAMPOLINE *pTrampoline;
	unsigned long int page_size, mask;
	
    if(found_libc==0)	{
		return 0;
	}

	page_size = sysconf(_SC_PAGESIZE);
	mask = ~(page_size - 1);
	
	query_all_org_func_addr();
	allocate_memory_block_for_patches();	
	
	for(idx_mod=0; idx_mod<num_module; idx_mod++)	{
		pTrampoline = (TRAMPOLINE *)(patch_blk_list[module_list[idx_mod].idx_patch_blk].patch_addr);
		nFunc_InBlk = patch_blk_list[module_list[idx_mod].idx_patch_blk].num_trampoline;
		
		for(iFunc=0; iFunc<module_list[idx_mod].num_hook; iFunc++)	{
			if(module_list[idx_mod].is_patch_disabled[iFunc])	{
				continue;
			}
			for(iFunc2=0; iFunc2<iFunc; iFunc2++)	{
				if(module_list[idx_mod].is_patch_disabled[iFunc2] == 0)	{	// a valid patch
					if(module_list[idx_mod].old_func_addr_list[iFunc] == module_list[idx_mod].old_func_addr_list[iFunc2])	{
						module_list[idx_mod].is_patch_disabled[iFunc] = 1;
						module_list[idx_mod].old_func_addr_list[iFunc] = 0;	// disable duplicated patch
						break;
					}
				}
			}
			if(module_list[idx_mod].is_patch_disabled[iFunc])	{	// recheck
				continue;
			}
			
			WithJmp[nFunc_InBlk]=-1;
			
			pTrampoline[nFunc_InBlk].addr_org_func = (void *)(module_list[idx_mod].old_func_addr_list[iFunc]);
			pTrampoline[nFunc_InBlk].offset_rIP_var = NULL_RIP_VAR_OFFSET;
			pTrampoline[nFunc_InBlk].saved_code_len = 0;
			
			ud_obj.pc = 0;
			ud_set_input_hook(&ud_obj, input_hook_x);
			
			nInstruction = 0;
			ud_idx = 0;
			bin_code_buff = (char *)(pTrampoline[nFunc_InBlk].addr_org_func);
			
			while (ud_disassemble(&ud_obj)) {
				OffsetList[nInstruction] = ud_insn_off(&ud_obj);
				if(OffsetList[nInstruction] >= JMP_INSTRCTION_LEN)	{	// size of jmp instruction
					pTrampoline[nFunc_InBlk].saved_code_len = OffsetList[nInstruction];
					if( (nInstruction > 0) && (strncmp(szHexCode[nInstruction-1], "e9", 2)==0) )	{
						if(strlen(szHexCode[nInstruction-1]) == 10)	{	// found a jmp instruction here!!!
							if(nInstruction >= 2)	{
								WithJmp[nFunc_InBlk] = strlen(szHexCode[nInstruction-2])/2 + 1;
							}
						}
					}
					break;
				}
				
				strcpy(szHexCode[nInstruction], ud_insn_hex(&ud_obj));
				strcpy(szInstruction[nInstruction], ud_insn_asm(&ud_obj));
				pSubStr = strstr(szInstruction[nInstruction], "[rip+");
				if(pSubStr)	{
					ReadItem = sscanf(pSubStr+5, "%x]", &RIP_Offset);
					if(ReadItem == 1)	{
						pTrampoline[nFunc_InBlk].offset_rIP_var = RIP_Offset;
					}
				}
				nInstruction++;
				if(nInstruction >= MAX_INSTUMENTS)	{
					printf("WARNING> nInstruction >= MAX_INSTUMENTS\nYou might need to increase MAX_INSTUMENTS.\n");
					break;
				}
			}			
			
			memcpy(pTrampoline[nFunc_InBlk].bounce, instruction_bounce, BOUNCE_CODE_LEN);
			*((unsigned long int *)(pTrampoline[nFunc_InBlk].bounce + OFFSET_NEW_FUNC_ADDR)) = (unsigned long int)(module_list[idx_mod].new_func_addr_list[iFunc]);	// the address of new function
			
			
			memcpy(pTrampoline[nFunc_InBlk].trampoline, pTrampoline[nFunc_InBlk].addr_org_func, pTrampoline[nFunc_InBlk].saved_code_len);
			pTrampoline[nFunc_InBlk].trampoline[pTrampoline[nFunc_InBlk].saved_code_len] = 0xE9;	// jmp
			Jmp_Offset = (int) ( ( (long int)(pTrampoline[nFunc_InBlk].addr_org_func) - ( (long int)(pTrampoline[nFunc_InBlk].trampoline) + 5) )  & 0xFFFFFFFF);
			*((int*)(pTrampoline[nFunc_InBlk].trampoline + pTrampoline[nFunc_InBlk].saved_code_len + 1)) = Jmp_Offset;
			
			if(pTrampoline[nFunc_InBlk].offset_rIP_var != NULL_RIP_VAR_OFFSET)	{
				jMax = pTrampoline[nFunc_InBlk].saved_code_len - 4;
				for(j=jMax-2; j<=jMax; j++)	{
					p_int = (int *)(pTrampoline[nFunc_InBlk].trampoline + j);
					if( *p_int == pTrampoline[nFunc_InBlk].offset_rIP_var )	{
						*p_int += ( (int)( ( (long int)(pTrampoline[nFunc_InBlk].addr_org_func) - (long int)(pTrampoline[nFunc_InBlk].trampoline) )  ) );	// correct relative offset of PIC var
					}
				}
			}
			if(WithJmp[nFunc_InBlk] > 0)	{
				p_int = (int *)(pTrampoline[nFunc_InBlk].trampoline + WithJmp[nFunc_InBlk]);
				*p_int += ( (int)( ( (long int)(pTrampoline[nFunc_InBlk].addr_org_func) - (long int)(pTrampoline[nFunc_InBlk].trampoline) )  ) );
			}
			if(pTrampoline[nFunc_InBlk].trampoline[0] == 0xE9)	{	// First instruction is JMP xxxx. Needs address correction
				p_int = (int *)(pTrampoline[nFunc_InBlk].trampoline + 1);	// the next four bytes are supposed to be the relative offset
				*p_int += ( (int)( ( (long int)(pTrampoline[nFunc_InBlk].addr_org_func) - (long int)(pTrampoline[nFunc_InBlk].trampoline) )  ) );
			}
			
			// set up function pointers for original functions
			*(module_list[idx_mod].ptr_old_func_add_list[iFunc]) = (long int)(pTrampoline[nFunc_InBlk].trampoline);	// the entry address to call orginal function
			
			
			pbaseOrg = (void *)( (long int)(pTrampoline[nFunc_InBlk].addr_org_func) & mask );	// fast mod
			
			MemSize_Modify = determine_mem_block_size((void *)(pTrampoline[nFunc_InBlk].addr_org_func), page_size);
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_WRITE | PROT_EXEC) != 0)	{
				printf("Error in executing mprotect(). %s\n", module_list[idx_mod].func_name_list[iFunc]);
				exit(1);
			}
			
			memcpy(pTrampoline[nFunc_InBlk].org_code, pTrampoline[nFunc_InBlk].addr_org_func, 5);	// save orginal code for uninstall 
			
			pOpOrgEntry = (char *)(pTrampoline[nFunc_InBlk].addr_org_func);
			pOpOrgEntry[0] = 0xE9;
			*((int *)(pOpOrgEntry+1)) = (int)( (long int)(pTrampoline[nFunc_InBlk].bounce) - (long int)(pTrampoline[nFunc_InBlk].addr_org_func) - 5 );
			
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_EXEC) != 0)	{
				printf("Error in executing mprotect(). %s\n", module_list[idx_mod].func_name_list[iFunc]);
				exit(1);
			}
			
			nFunc_InBlk++;
			num_hook_installed++;
		}
		patch_blk_list[module_list[idx_mod].idx_patch_blk].num_trampoline += module_list[idx_mod].num_hook;
	}
	
	return num_hook_installed;
}
#undef MAX_INSTUMENTS

// Taken from udis86 project
static void init_udis86(void)
{
	ud_init(&ud_obj);
	ud_set_mode(&ud_obj, 64);     // 32 or 64
	ud_set_syntax(&ud_obj, UD_SYN_INTEL); // intel syntax
}

// Taken from udis86 project
static int input_hook_x(ud_t* u)
{
	if(ud_idx < MAX_LEN_TO_DISASSEMBLE)        {
		ud_idx++;
		return (int)(bin_code_buff[ud_idx-1] & 0xFF);
	}
	else    {
		return UD_EOI;
	}
}

/*
 * register_a_hook - Add one target function into the list of the functions to intercept.
 *   @module_name: The name of shared library. Both short name ("ld") and full name ("ld-2.17.so") are accepted. 
 *   @func_Name:   The function name. 
 *   @new_func_addr: The address of our new implementation. 
 *   @ptr_org_func: *ptr_org_func will hold the address of orginal function implemented in lib module_name. 
 * Returns:
 *   0: success; otherwise fail. 
 */
int register_a_hook(const char *module_name, const char *func_name, const void *new_func_addr, const long int *ptr_org_func)
{
	void *module;
	int idx, idx_mod;
	char module_name_local[MAX_LEN_PATH_NAME];

	// make sure module_name[] and func_name[] are not too long. 
	if(strlen(module_name) >= MAX_LEN_PATH_NAME)	{
		return REGISTER_MODULE_NAME_TOO_LONG;
	}
	if(strlen(func_name) >= MAX_LEN_FUNC_NAME)	{
		return REGISTER_FUNC_NAME_TOO_LONG;
	}

	// Do some initialization work at the first time. 
	if( !num_hook )	{
		memset(module_list, 0, sizeof(MODULE_PATCH_INFO)*MAX_MODULE);
		memset(patch_blk_list, 0, sizeof(PATCH_BLOCK)*MAX_MODULE);
		
		init_udis86();
		determine_lib_path();
	}
	
	if(found_libc==0)	{
		return REGISTER_NOT_FOUND_LIBC;
	}

	if(num_called_ModuleMap == 0)	{
		get_module_maps();
		num_called_ModuleMap++;
	}
	
	if( strcmp(module_name, "ld") == 0 )	{
		strcpy(module_name_local, path_ld);
	}
	else if( strcmp(module_name, "libc") == 0 )	{
		strcpy(module_name_local, path_libc);
	}
	else if( strcmp(module_name, "libpthread") == 0 )	{
		strcpy(module_name_local, path_libpthread);
	}
	else	{
		strcpy(module_name_local, module_name);
	}
	
	if(module_name_local[0] == '/')	{	// absolute path
		if( query_lib_name_in_list(module_name_local) == -1 )	{	// not loaded yet
			module = dlopen(module_name_local, RTLD_LAZY);	// load the library
			if(module == NULL)	{
				printf("Error> Fail to dlopen: %s.\n", module_name_local);
				return REGISTER_DLOPEN_FAILED;
			}
			get_module_maps();
		}
	}
	
	idx = query_lib_name_in_list(module_name_local);
	if( idx == -1 )	{
		printf("Failed to find %s in /proc/pid/maps\nQuit\n", module_name_local);
		exit(1);
	}
	
	idx_mod = query_registered_module(module_name_local);
	if(idx_mod == -1)	{	// not registered module name. Register it.
		strcpy(module_list[num_module].module_name, module_name_local);
		module_list[num_module].module_base_addr = lib_base_addr[idx];
		strcpy(module_list[num_module].func_name_list[module_list[num_module].num_hook], func_name);
		module_list[num_module].new_func_addr_list[module_list[num_module].num_hook] = (void*)new_func_addr;
		module_list[num_module].ptr_old_func_add_list[module_list[num_module].num_hook] = (long int *)ptr_org_func;
		module_list[num_module].num_hook = 1;
		num_module++;
	}
	else	{		
		if(module_list[idx_mod].module_base_addr != lib_base_addr[idx])	{
			printf("WARING> module_list[idx_mod].module_base_addr != lib_base_addr[idx]\n");
		}
		
		strcpy(module_list[idx_mod].func_name_list[module_list[idx_mod].num_hook], func_name);
		module_list[idx_mod].new_func_addr_list[module_list[idx_mod].num_hook] = (void *)new_func_addr;
		module_list[idx_mod].ptr_old_func_add_list[module_list[idx_mod].num_hook] = (long int *)ptr_org_func;
		module_list[idx_mod].num_hook ++;
	}
	
	num_hook++;
	
	if(num_hook > MAX_PATCH)	{
		printf("Error> num_hook > MAX_PATCH\nQuit\n");
		return REGISTER_TOO_MANY_HOOKS;
	}

	return REGISTER_SUCCESS;
}

/*
 * query_all_org_func_addr - Queries the addresses of all orginal functions to hook.
 * Returns:
 *   void
 */
static void query_all_org_func_addr(void)
{
	int idx, idx_mod, iFunc;
	
	get_module_maps();	// update module map
	
	for(idx_mod=0; idx_mod<num_module; idx_mod++)	{	// update module base address
		idx = query_lib_name_in_list(module_list[idx_mod].module_name);
		
		if(idx == -1)	{	// a new name not in list
			printf("Fail to find library %s in maps.\nQuit\n", module_list[idx_mod].module_name);
			exit(1);
		}
		else	{
			strcpy(module_list[idx_mod].module_name, lib_name_list[idx]);
			module_list[idx_mod].module_base_addr = lib_base_addr[idx];
		}
	}
	
	for(idx_mod=0; idx_mod<num_module; idx_mod++)	{
		query_func_addr(module_list[idx_mod].module_name, module_list[idx_mod].func_name_list, \
			module_list[idx_mod].old_func_addr_list, module_list[idx_mod].old_func_len_list, \
			module_list[idx_mod].module_base_addr, module_list[idx_mod].num_hook);
		
		for(iFunc=0; iFunc<module_list[idx_mod].num_hook; iFunc++)	{
			if(module_list[idx_mod].old_func_addr_list[iFunc])	{
				module_list[idx_mod].old_func_addr_min = module_list[idx_mod].old_func_addr_list[iFunc];
				module_list[idx_mod].old_func_addr_max = module_list[idx_mod].old_func_addr_list[iFunc];
				break;
			}
		}
		
		for(iFunc=0; iFunc<module_list[idx_mod].num_hook; iFunc++)	{
			if( module_list[idx_mod].old_func_addr_list[iFunc] == 0 )	{
				//printf("Fail to find the entry address for function %s in %s\nQuit\n", module_list[idx_mod].func_name_list[iFunc], module_list[idx_mod].module_name);
				//exit(1);
				module_list[idx_mod].is_patch_disabled[iFunc] = 1;
				continue;
			}
			if(module_list[idx_mod].old_func_addr_min > module_list[idx_mod].old_func_addr_list[iFunc])	{
				module_list[idx_mod].old_func_addr_min = module_list[idx_mod].old_func_addr_list[iFunc];
			}
			if(module_list[idx_mod].old_func_addr_max < module_list[idx_mod].old_func_addr_list[iFunc])	{
				module_list[idx_mod].old_func_addr_max = module_list[idx_mod].old_func_addr_list[iFunc];
			}
		}
		//printf("module_list[%d], %d functions,  %s, 0x%p, 0x%p\n", idx_mod, module_list[idx_mod].num_hook, module_list[idx_mod].module_name, module_list[idx_mod].OrgFuncMin, module_list[idx_mod].OrgFuncMax);
	}
}
