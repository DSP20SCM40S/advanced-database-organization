// importing packages
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

FILE *f;                                              //file pointer
//function to initialize the storage manager
void initStorageManager(void)
{
    printf("Storage Manager, by group_27\n");
    printf("Group Members:\n");
    printf("1. Disha Sharma\n");
    printf("2. Vaishnavi Chaudhari\n");
    printf("3. Karthik Kini\n");
}

/*
 * It creates a new page file with "fileName". The initial file size is of one page.
 * This method fills this single page with '\0' bytes.
 */
RC createPageFile(char *fileName)
{
    f=fopen(fileName,"w+");      //Open the filePage in write mode
    char *memoryBlock=malloc(PAGE_SIZE*sizeof(char));
    if(f==NULL)                 //if file exists
        return RC_FILE_NOT_FOUND;
    else
    {
        memset(memoryBlock,'\0',PAGE_SIZE);
        fwrite(memoryBlock,sizeof(char),PAGE_SIZE,f);
        free(memoryBlock);
        fclose(f);
        return RC_OK;
    }

}

/*
 * This method Opens an existing page file & returns RC_FILE_NOT_FOUND if the file does not exist.
 * If file opening is a success, then the fields of file handle are initialized with the information about the opened file.
 */
RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    f=fopen(fileName,"r+");
    if(f==0)
        return RC_FILE_NOT_FOUND;
    else
    {
        fseek(f,0,SEEK_END);
        int endByte=ftell(f);
        int length=endByte+1;
        int NumOfPages=length/PAGE_SIZE;
        fHandle->fileName=fileName;
        fHandle->totalNumPages=NumOfPages;
        fHandle->curPagePos=0;
        rewind(f);
        return RC_OK;
    }
}

/*
 * This method is used to close the pageFile
 */
RC closePageFile(SM_FileHandle *fHandle)
{
    RC closed;
    closed=fclose(f);
    //if closing the file is success
    if(closed==0)
        return RC_OK;
    else
    {
        return RC_FILE_NOT_FOUND;
    }
}

/*
 * This method deletes the pageFile or destory's the pageFile
 */
RC destroyPageFile(char *fileName)
{
    //if remove pageFile is successful
    RC Remove;
    Remove=remove(fileName);
    if(Remove==0)
        return RC_OK;
    else
    {
        return RC_FILE_NOT_FOUND;
    }
}

/* reading blocks from disc */

/*
 * This method reads the pageNum(th) block from a file and stores its content in the memory pointed to by the memPage page handle.
 * If the file has less than pageNum pages, it return RC_READ_NON_EXISTING_PAGE.
 */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //check if PageNum is Valid
    RC readSize;
    if(fHandle->totalNumPages<pageNum)
        return RC_READ_NON_EXISTING_PAGE;
    else
    {
        fseek(f,pageNum*PAGE_SIZE,SEEK_SET);
        readSize=fread(memPage,sizeof(char),PAGE_SIZE,f);
        if(readSize!=PAGE_SIZE)
            return RC_READ_NON_EXISTING_PAGE;
        fHandle->curPagePos=pageNum;
        return RC_OK;
    }
}

/*
 * This method returns the current page position
 */
int getBlockPos(SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

/*
 * This method is used to read the First Block from the pageFile into memPage.
 * It returns an error if there are no pages in the pageFile
 */
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //call to readBlock with page No. 0 i.e first block
    if(fHandle==NULL)
        return RC_FILE_NOT_FOUND;
    else
    {
        if(fHandle->totalNumPages<=0)
            return RC_READ_NON_EXISTING_PAGE;
        else
        {
            RC firstBlock;
            fseek(f,0,SEEK_SET);
            firstBlock=fread(memPage,sizeof(char),PAGE_SIZE,f);
            fHandle->curPagePos=0;
            if(firstBlock<0 || firstBlock>PAGE_SIZE)
                return RC_READ_NON_EXISTING_PAGE;
            else
                return RC_OK;
        }
    }
}

/*
 * This method is used to read the previous Block from the pageFile into memPage.
 * It returns an error if there is no previous block in the pageFile
 */
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //call to readBlock with page No. (currentPage-1) i.e previous block
    if(fHandle==NULL)
        return RC_FILE_NOT_FOUND;
    else
    {
        RC previousBlock;
        RC previousBlockOffset;
        RC previousBlockRead;
        previousBlock=getBlockPos(fHandle)-1;
        if(previousBlock<0)
            return RC_READ_NON_EXISTING_PAGE;
        else
        {
            previousBlockOffset=previousBlock*PAGE_SIZE;
            fseek(f,previousBlockOffset,SEEK_SET);
            previousBlockRead=fread(memPage,sizeof(char),PAGE_SIZE,f);
            fHandle->curPagePos=fHandle->curPagePos-1;
            if(previousBlockRead<0 || previousBlockRead>PAGE_SIZE)
                return RC_READ_NON_EXISTING_PAGE;
            else
                return RC_OK;
        }
    }
}

/*
 * This method is used to read the Current Block from the pageFile into memPage.
 * It returns an error if there are no blocks in the pageFile
 */
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //call to readBlock with page No. (currentPage) i.e current block
    if(fHandle==NULL)
        return RC_FILE_NOT_FOUND;
    else
    {
        RC currentBlock;
        RC currentBlockOffset;
        RC currentBlockRead;
        currentBlock=getBlockPos(fHandle);
        currentBlockOffset=currentBlock*PAGE_SIZE;
        fseek(f,currentBlockOffset,SEEK_SET);
        currentBlockRead=fread(memPage,sizeof(char),PAGE_SIZE,f);
        if(currentBlockRead<0 || currentBlockRead>PAGE_SIZE)
            return RC_READ_NON_EXISTING_PAGE;
        else
            return RC_OK;
    }
}

/*
 * This method is used to read the First Block from the pageFile into memPage.
 * It returns an error if there are no pages in the pageFile
 */
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //call to readBlock with page No. (currentPage+1) i.e next block
    if(fHandle==NULL)
        return RC_FILE_NOT_FOUND;
    else
    {
        RC nextBlock;
        RC nextBlockOffset;
        RC nextBlockRead;
        nextBlock=getBlockPos(fHandle)+1;
        if(nextBlock<fHandle->totalNumPages)
        {
            nextBlockOffset=(nextBlock*PAGE_SIZE);
            fseek(f,nextBlockOffset,SEEK_SET);
            nextBlockRead=fread(memPage,sizeof(char),PAGE_SIZE,f);
            fHandle->curPagePos=nextBlock;
            if(nextBlockRead<0 || nextBlockRead>PAGE_SIZE)
                return RC_READ_NON_EXISTING_PAGE;
            else
                return RC_OK;
        }
        else
        {
            return RC_READ_NON_EXISTING_PAGE;
        }
    }
}

/*
 * This method is used to read the Last Block from the pageFile into memPage.
 * It returns an error if there are no pages in the pageFile
 */
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //call to readBlock with page as Last block
    if(fHandle==NULL)
        return RC_FILE_NOT_FOUND;
    else
    {
        RC lastBlock;
        RC lastBlockOffset;
        RC lastBlockRead;
        lastBlock=(fHandle->totalNumPages)-1;
        lastBlockOffset=lastBlock*PAGE_SIZE;
        fseek(f,lastBlockOffset,SEEK_SET);
        lastBlockRead=fread(memPage,sizeof(char),PAGE_SIZE,f);
        fHandle->curPagePos=lastBlock;
        if(lastBlockRead<0 || lastBlockRead>PAGE_SIZE)
            return RC_READ_NON_EXISTING_PAGE;
        else
            return RC_OK;
    }

}

/* writing blocks to a page file */

/*
 * This method is used to write onto the block specified in the pageNum field.
 * If the block is not present it gives an error RC_WRITE_FAILED
 */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
  //check if pageNum is valid
    if(pageNum<0 || pageNum>PAGE_SIZE)
        return RC_WRITE_FAILED;
    else if(fHandle==NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    else
    {
        if(f!=NULL)
        {
            RC writeBlockOffset=pageNum*PAGE_SIZE;
            if(fseek(f,writeBlockOffset,SEEK_SET)==0)
            {
                fwrite(memPage,sizeof(char),PAGE_SIZE,f);
                fHandle->curPagePos=pageNum;
                fseek(f,0,SEEK_END);
                fHandle->totalNumPages=ftell(f)/PAGE_SIZE;
                return RC_OK;
            }
            else
                return RC_WRITE_FAILED;
        }
        else
            return RC_FILE_NOT_FOUND;
    }
}

/*
 * This method is used to write onto the currently pointed block.
 * If the block is not present it gives and error RC_WRITE_FAILED
 */
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
  //write on the current block

    RC currentPosition=getBlockPos(fHandle);
    RC writeCurrentBlock=writeBlock(currentPosition,fHandle,memPage);
    if(writeCurrentBlock==RC_OK)
        return RC_OK;
    else
    {
            return RC_WRITE_FAILED;
    }
}

/*
 * This method is used to append a new Empty block into the pageFile.
 * The empty block will contain '\0' bytes
 */
RC appendEmptyBlock (SM_FileHandle *fHandle)
{
    if(f!=NULL)
    {
        RC size=0;
        char *newEmptyBlock=(char *)calloc(PAGE_SIZE,sizeof(char));
        fseek(f,0,SEEK_END);
        size=fwrite(newEmptyBlock,1,PAGE_SIZE,f);
        if(size==PAGE_SIZE)
        {
            fHandle->totalNumPages=ftell(f)/PAGE_SIZE;
            fHandle->curPagePos=fHandle->totalNumPages-1;
            free(newEmptyBlock);
            return RC_OK;
        }
        else
        {
            free(newEmptyBlock);               //free the memory allocated to avoid memory leaks
            return RC_WRITE_FAILED;
        }
    }
    else
    {
        return RC_FILE_NOT_FOUND;
    }
}

/*
 * This method is used to ensure the capacity of pageFile.
 * The pageFile must have numberOfPages that is specified,
 * if not then add those many number of pages to achieve that capacity
 */
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
    int No_OfPages=fHandle->totalNumPages;
    int i;
    if(No_OfPages<numberOfPages)
    {
        int extraPages=numberOfPages-No_OfPages;
        for(i=0;i<extraPages;i++)            //loop through and append empty blocks to attain the required capacity
        {
            appendEmptyBlock(fHandle);       //call to appendEmptyBlock()
        }
        return RC_OK;
    }
    else
    {
        return RC_WRITE_FAILED;
    }
}