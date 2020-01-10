#include<stdio.h>
#include<stdlib.h>
#include "storage_mgr.h"
#include<string.h>

FILE *f;

//initializing page handler to NUll for the storage manager.
extern void initStorageManager(void)
{
	printf("Storage Manager, by group_27\n");
    printf("Group Members:\n");
    printf("1. Disha Sharma\n");
    printf("2. Vaishnavi Chaudhari\n");
    printf("3. Karthik Kini\n");
	f = NULL;
}

extern RC createPageFile(char *fileName)
{
	f=fopen(fileName,"w+");
	if(f==NULL)
		return RC_FILE_NOT_FOUND;
	else 
	{
		SM_PageHandle memoryBlock=(SM_PageHandle)calloc(PAGE_SIZE,sizeof(char));
		if(fwrite(memoryBlock,sizeof(char),PAGE_SIZE,f) >= PAGE_SIZE) 
		{
			//flushing all file buffers and freeing the memory to prevent memory leaks.
			fclose(f);
	    	free(memoryBlock);
        	return RC_OK;
		}
		return RC_ERROR;
	}
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
	f=fopen(fileName,"r+");
	if(f==NULL)
		return RC_FILE_NOT_FOUND;
	else
	{
		fHandle->fileName=fileName;
		//finding total number of pages by getting total file size and dividing by PAGE_SIZE
		fseek(f,0,SEEK_END);
		int endByte=ftell(f);
		fHandle->totalNumPages=endByte/PAGE_SIZE;
		fHandle->mgmtInfo=f;
		fHandle->curPagePos=0;
		rewind(f);
		fclose(f);
		return RC_OK;
	}
}
//closing all file handlers
extern RC closePageFile(SM_FileHandle *fHandle)
{
	RC closed=fclose(fHandle->mgmtInfo);
	if(closed==0)
		return RC_ERROR;
	return RC_OK;
}

//deleting the test case file from the disk
extern RC destroyPageFile(char *fileName)
{
	RC Remove=remove(fileName);
	if(Remove==0)
		return RC_OK;
	return RC_FILE_NOT_FOUND;
}

//This method reads blocks from the file specified by pageNum parameter and and stores the contents in the memory pointed to by memPage.
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle->mgmtInfo==NULL)
		return RC_FILE_NOT_FOUND;
	if(pageNum>fHandle->totalNumPages && pageNum<0)
		return RC_READ_NON_EXISTING_PAGE;
	f=fopen(fHandle->fileName,"r+");
	//moving cursor to the starting of the block by multiplying pageNum with PAGE_SIZE
	int seekCheck=fseek(f, pageNum*PAGE_SIZE,SEEK_SET);
	if(seekCheck!=0)
		return RC_ERROR;
  	//reading from the beginning of the page number pointed by the cursor.
	fread(memPage, sizeof(char), PAGE_SIZE, f);
	fHandle->curPagePos=pageNum;
	fclose(f);
	return RC_OK;
}
//returns the current cursor position.
extern int getBlockPos(SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}

extern RC readFirstBlock(SM_FileHandle *fHandle , SM_PageHandle memPage)
{
    return readBlock(0,fHandle,memPage);
}

extern RC readLastBlock(SM_FileHandle *fHandle , SM_PageHandle memPage)
{
  	if(fHandle->mgmtInfo==NULL)
		return RC_FILE_NOT_FOUND;
  	return readBlock(fHandle->totalNumPages,fHandle,memPage);
}

extern RC readPreviousBlock(SM_FileHandle *fHandle , SM_PageHandle memPage)
{
	if(fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;
  	int current=getBlockPos(fHandle);
  	int previous=current-1;
  	return readBlock(previous,fHandle,memPage);
}

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;
  	int current=getBlockPos(fHandle);
  	return readBlock(current,fHandle,memPage);
}

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
  	int current=getBlockPos(fHandle);
  	int next=current+1;
  	return readBlock(next,fHandle,memPage);
}

//This method writes a page to the disk. It takes contents from the memPage PageHandle and writes it to the Disk.
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle==NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	if(pageNum>fHandle->totalNumPages || pageNum<0)
		return RC_WRITE_FAILED;
 	f=fopen(fHandle->fileName,"r+");
  	if(f==NULL)
    	return RC_FILE_NOT_FOUND;
  	int seekCheck=fseek(f, pageNum*PAGE_SIZE,SEEK_SET);
	if(seekCheck!=0)
		return RC_ERROR;
	else
	{
    	fwrite(memPage,sizeof(char),strlen(memPage),f);
		fHandle->curPagePos=pageNum;
		fclose(f);
		return RC_OK;
  	}
}

extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	fHandle->totalNumPages+=1;
	return writeBlock(getBlockPos(fHandle),fHandle,memPage);
}
//adds empty block after the last page of the file
extern RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	f=fopen(fHandle->fileName,"r+");
	if(f==NULL)
		return RC_FILE_NOT_FOUND;
	char *newEmptyBlock=(char *)calloc(PAGE_SIZE,sizeof(char));
	int totalNoOfPages=fHandle->totalNumPages;
	fHandle->totalNumPages += 1;
		//moves cursor to the last page and writes 0 bytes until the entire page is filled.
	fseek(f,totalNoOfPages*PAGE_SIZE,SEEK_SET);
	RC size=fwrite(newEmptyBlock,1,PAGE_SIZE,f);
	free(newEmptyBlock);
	if(size!=PAGE_SIZE)
		return RC_WRITE_FAILED;
	fclose(f);
	return RC_OK;
}
//creates new pages to the file until the numberOfPages parameter are satisfied.
//calls appendEmptyBlock() until it is numberOfPages is satisfied.
extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
	if(f==NULL)
		return RC_FILE_NOT_FOUND;
	int extraPages=numberOfPages-fHandle->totalNumPages;
	int i;
	if(extraPages > 0)
	{
		for(i=0;i<extraPages;i++)
			appendEmptyBlock(fHandle);
	}
	if(fHandle->totalNumPages == numberOfPages)
		return RC_OK;
	else
		return RC_WRITE_FAILED;
}