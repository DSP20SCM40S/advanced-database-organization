#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
//The Page Frame structure which holds the actual page frame in the Buffer Manager.
typedef struct Frame 
{
	SM_PageHandle content;
	int isDirty;
	int pinStatus;
	int freeStat;
	int fixCount;
	PageNumber PageNumber;
	void *qPointer;
} Frame;
//The Queue Data structure which simulates a queue for the FIFO and LRU Page Repalcement Algorithm
typedef struct Queue 
{
	int count;
	int position;
	Frame *fPointer;
	int pNo;
} Queue;

int bufferSize=0;			//This variable indicates the current size of the buffer manager
int maxBufferSize;			//This variable indicates the maximum size of the buffer Manager.
int queueSize;				//This variable indicates the maximum size of the Queue.
int isBufferFull=0;			//This variable indicates if the buffer is full or not.
int currentQsize;			//This variable indicates if the current queue size.s
int writes=0;				//This varaible keeps track of the number of writes to the disk.
int reads=0;				//This variable keeps track of the number of reads from the disk.
//This function initiates the buffer manager with the necessary parameters.
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
{
	// Initializing all the variables.
	queueSize=numPages;
	maxBufferSize=numPages;
	currentQsize=0;
	writes=0;
	reads=0;
	//setting the values in the buffer manager variable.
	bm->pageFile=(char *)pageFileName;
	bm->numPages=numPages;
	bm->strategy=strategy;
	int i;
	//Allocating space for the PageFrame and the Queue.
	Frame *f=malloc(sizeof(Frame)*numPages);
	Queue *q=malloc(sizeof(Queue)*numPages);
	//Initializing all the values in the PageFrame and Queue.
	for(i=0;i<numPages;i++) 
	{
		f[i].content=(SM_PageHandle)malloc(PAGE_SIZE);
		f[i].isDirty=0;
		f[i].pinStatus=0;
		f[i].freeStat=0;
		f[i].PageNumber=-1;
		f[i].fixCount=0;
		q[i].count=0;
		q[i].position=0;
		q[i].fPointer=NULL;
		q[i].pNo=-1;
	}
	//The first PageFrame always contains the pointer to the Queue.
	f[0].qPointer=q;
	//The pageFrame is stored in the bufferpool variable.
	bm->mgmtData=f;
	return RC_OK;
}
//This function returns the PageFrame if the pagenumber and the bufferpool variable are given.
Frame * returnPagePointer(BM_BufferPool *const bm,BM_PageHandle *const page)
{
	Frame *ff=(Frame *)bm->mgmtData;
	for(int i=0; i<maxBufferSize; i++) 
	{
		if(page->pageNum==ff[i].PageNumber) 
		{
			return &ff[i];
			break;
		}
	}
	return ff;
}
//This function returns the maximum position value in the queue, used for LRU.
int maxQueue(Queue *q)
{
	int max=-1;
	for(int i=0; i<currentQsize; i++) 
	{
		if(q[i].position > max) 
			max=q[i].position;
	}
	return max;
}
//The LRU Function is a void function which the maintains the queue and performs LRU when the buffer size is full.
void LRU(BM_BufferPool *const bm, BM_PageHandle *const page,int pageNum)
{
	//Get the PageFrame pointer and the Queue pointer.
	Frame *ff=(Frame *)bm->mgmtData;
	Queue *q=(Queue *)ff[0].qPointer;
	//if the pageFrame is already in buffer make its position 1 so that we know it was the last accessed page.
	//increment the rest.
	for(int i=0; i<currentQsize; i++) 
	{
		if(ff[i].PageNumber==pageNum) 
		{
			q[i].position=1;
			ff[i].fixCount++;
			for(int j=0; j<currentQsize; j++)
				if(j!=i)
					q[j].position++;
			//update the page handler.
			page->data=ff[i].content;
			page->pageNum=ff[i].PageNumber;
			return;
		}
	}
	//if the buffer is not full insert pages normally.
	if(currentQsize<bm->numPages) 
	{
		//insert the first page in the q.
		if(currentQsize==0) 
		{
			q[0].position=1;
			q[0].fPointer=&ff[0];
			currentQsize++;
			return;
		}
		else
		{
			//find the place in the queue which is free and insert the page.
			for(int i=0; i<maxBufferSize; i++) 
			{
				//finding which pageFrame is null.
				if(q[i].fPointer==NULL) 
				{
					q[i].position=1;
					for(int j=0; j<maxBufferSize; j++) 
						if(j!=i)
							q[j].position+=1;
					q[i].fPointer=returnPagePointer(bm,page);
					currentQsize++;
					return;
				}
			}
		}
	}
	//If the queue is full we need to do LRU.
	else if(currentQsize==queueSize) 
	{
		for(int i =0; i<currentQsize; i++)
		{
			// The queue frame with the maximum position is the least recently used, so we must replace that.
			if(q[i].position==maxQueue(q)) 
			{
				//check if no other client is accessing the page.
				if(ff[i].fixCount==0) 
				{
					q[i].position=1;
					for(int j=0; j<currentQsize; j++)
						if(j != i)
							q[j].position++;
					SM_FileHandle fh;
					//if the page is dirty write it back to the disk.
					if(ff[i].isDirty==1) 
					{
						openPageFile(bm->pageFile, &fh);
						ensureCapacity(ff[i].PageNumber,&fh);
						writeBlock(ff[i].PageNumber, &fh, ff[i].content);
						writes++;
					}
					// pin the page and store the pagenumber.
					ff[i].pinStatus = 1;
					ff[i].PageNumber = pageNum;
					ff[i].freeStat = 1;
					ff[i].isDirty = 0;
					ff[i].fixCount = 0;
					//read the data from the disk and store it in the page handler.
					openPageFile(bm->pageFile,&fh);
					ensureCapacity(ff[i].PageNumber, &fh);
					readBlock(ff[i].PageNumber,&fh,ff[i].content);
					//increment the read count
					reads++;
					//update the page handler.
					page->data  = ff[i].content;
					page->pageNum = ff[i].PageNumber;
					return;
				}
			}
		}
		// if the page is in use by other client find the next largest page and check if we can replace that
		for(int i =0; i<currentQsize; i++) 
		{
			int temp = maxQueue(q)-1;
			if(q[i].position == temp && ff[i].fixCount == 0) 
			{
				q[i].position = 1;
				for(int j=0; j<currentQsize; j++)
					if(j != i)
						q[j].position++;
				SM_FileHandle fh;
				if(ff[i].isDirty ==1) 
				{
					openPageFile(bm->pageFile, &fh);
					ensureCapacity(ff[i].PageNumber,&fh);
					writeBlock(ff[i].PageNumber, &fh, ff[i].content);
					writes++;
				}
				// pin the page and store the pagenumber.
				ff[i].pinStatus = 1;
				ff[i].PageNumber = pageNum;
				ff[i].freeStat = 1;
				ff[i].isDirty = 0;
				ff[i].fixCount = 0;
				//read the data from the disk and store it in the page handler.
				openPageFile(bm->pageFile,&fh);
				ensureCapacity(ff[i].PageNumber, &fh);
				readBlock(ff[i].PageNumber,&fh,ff[i].content);
				//increment the read count
				reads++;
				//update the page handler.
				page->data  = ff[i].content;
				page->pageNum = ff[i].PageNumber;
				return;
			}
		}
	}
}

//The FIFO Function is a void function which the maintains the queue and performs FIFO when the buffer size is full.
void FIFO(BM_BufferPool *const bm, BM_PageHandle *const page,int pageNum)
{
	//Get the PageFrame pointer and the Queue pointer.
	Frame *ff = (Frame *)bm->mgmtData;
	Queue *q = (Queue *)ff[0].qPointer;
	//if the buffer is not full insert pages normally.
	if(currentQsize < bm->numPages) 
	{
		//insert the first page in the queue.
		if(currentQsize == 0)
		{
			q[0].position = 1;
			q[0].fPointer = &ff[0];
			q[0].pNo = pageNum;
			currentQsize++;
			return;
		}
		else
		{
			//find the place in the queue which is free and insert the page.
			for(int i =0; i<maxBufferSize; i++) 
			{
				//finding which pageFrame is null.
				if(q[i].fPointer == NULL) 
				{
					q[i].position = 1;
					q[i].pNo = pageNum;
					for(int j=0; j<maxBufferSize; j++)
						if(j != i)
							q[j].position +=1;
					q[i].fPointer = returnPagePointer(bm,page);
					currentQsize++;
					return;
				}
			}
		}
	}
	//If the queue is full we need to do FIFO.
	else if(currentQsize == queueSize ) 
	{
		for(int i =0; i<currentQsize; i++) 
		{
			//if the page already exists in buffer then just increment the fix count.
			if(ff[i].PageNumber == pageNum) 
			{
					ff[i].fixCount += 1;
					return;
			}
			//Remove the element with the maximum position value which will be first one which came in to the buffer
			if(q[i].position == currentQsize) 
			{
				if(ff[i].fixCount == 0)
				{
					//change its position to the first and insert the new frame.
					q[i].position = 1;
					for(int j=0; j<currentQsize; j++)
						if(j != i)
							q[j].position++;
					SM_FileHandle fh;
					if(ff[i].isDirty ==1) 
					{
						openPageFile(bm->pageFile, &fh);
						ensureCapacity(ff[i].PageNumber,&fh);
						writeBlock(ff[i].PageNumber, &fh, ff[i].content);
						writes++;
					}
					// pin the page and store the pagenumber.
					ff[i].pinStatus = 1;
					ff[i].PageNumber = pageNum;
					//printf("%d",pf->PageNumber);
					ff[i].freeStat = 1;
					ff[i].isDirty = 0;
					ff[i].fixCount = 0;
					//read the data from the disk and store it in the page handler.
					openPageFile(bm->pageFile,&fh);
					ensureCapacity(ff[i].PageNumber, &fh);
					readBlock(ff[i].PageNumber,&fh,ff[i].content);
					//increment the read count
					reads++;
					//update the page handler.
					page->data  = ff[i].content;
					page->pageNum = ff[i].PageNumber;
					return;
				}
			}
		}
		// if the page is in use by other client find the next largest page and check if we can replace that
		for(int i =0; i<currentQsize; i++) 
		{
			int temp = currentQsize-1;
			if(q[i].position == temp && ff[i].fixCount == 0)
			{
				q[i].position = 1;
				for(int j=0; j<currentQsize; j++) 
					if(j != i)
						q[j].position++;
				SM_FileHandle fh;
				if(ff[i].isDirty ==1)
				{
					openPageFile(bm->pageFile, &fh);
					ensureCapacity(ff[i].PageNumber,&fh);
					writeBlock(ff[i].PageNumber, &fh, ff[i].content);
					writes++;
				}
				// pin the page and store the pagenumber.
				ff[i].pinStatus = 1;
				ff[i].PageNumber = pageNum;
				ff[i].freeStat = 1;
				ff[i].isDirty = 0;
				ff[i].fixCount = 0;
				//read the data from the disk and store it in the page handler.
				openPageFile(bm->pageFile,&fh);
				ensureCapacity(ff[i].PageNumber, &fh);
				readBlock(ff[i].PageNumber,&fh,ff[i].content);
				//increment the read count
				reads++;
				//update the page handler.
				page->data  = ff[i].content;
				page->pageNum = ff[i].PageNumber;
				return;
			}
		}
	}
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	int size = bm->numPages;
	Frame *f = (Frame *)bm->mgmtData;
	//if the buffer is full call the replacement strategy.
	if(bufferSize == maxBufferSize) 
	{
		isBufferFull = 1;
		if(bm->strategy == RS_FIFO)
			FIFO(bm,page,pageNum);
		else
			LRU(bm,page,pageNum);
		return RC_OK;
	}
	else if(isBufferFull == 0) 
	{
		if(bufferSize == 0) 
		{
			if(bm->strategy == RS_FIFO)
				FIFO(bm,page,pageNum);
			else
				LRU(bm,page,pageNum);
			SM_FileHandle fh;
			// pin the page and store the pagenumber.
			f[0].pinStatus = 1;
			f[0].PageNumber = pageNum;
			f[0].freeStat = 1;
			f[0].fixCount++;
			//read the data from the disk and store it in the page handler.
			openPageFile(bm->pageFile,&fh);
			ensureCapacity(pageNum, &fh);
			readBlock(pageNum,&fh,f[0].content);
			//increment the read count
			reads++;
			//update the page handler.
			page->data  = f[0].content;
			page->pageNum = f[0].PageNumber;
			bufferSize++;
			return RC_OK;
		}
		else
		{
			for(int i=1; i<size; i++) 
			{
				if(f[i].freeStat == 0) 
				{
					SM_FileHandle fh;
					if(bm->strategy == RS_FIFO)
						FIFO(bm,page,pageNum);
					else
						LRU(bm,page,pageNum);
					// pin the page and store the pagenumber.
					f[i].freeStat = 1;
					f[i].pinStatus = 1;
					f[i].PageNumber = pageNum;
					f[i].fixCount++;
					openPageFile(bm->pageFile,&fh);
					ensureCapacity(pageNum, &fh);
					readBlock(pageNum,&fh,f[i].content);
					reads++;
					//update the page handler.
					page->data  = f[i].content;
					page->pageNum = pageNum;
					bufferSize++;
					return RC_OK;
				}
			}
			return RC_OK;
		}
	}
	return RC_ERROR;
}
//unpinning the page.
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	Frame *f = (Frame *)bm->mgmtData;
	int i;
	for(i=0; i<bufferSize; i++) 
	{
		if(f[i].PageNumber == page->pageNum) 
		{
			//make the pin status as 0.
			f[i].pinStatus = 0;
			//decrement the fix count since client is unpinning.
			if(f[i].fixCount> 0)
				f[i].fixCount--;
			else
				f[i].fixCount=0;
			f[1].pinStatus = 0;
			return RC_OK;
		}
	}
	return RC_ERROR;
}
//marking the page as dirty.
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	Frame *f = (Frame *)bm->mgmtData;
	int i;
	for(i=0; i<bufferSize; i++) 
	{
		if(f[i].PageNumber == page->pageNum) 
		{
			f[i].isDirty = 1;
			return RC_OK;
		}
	}
	return RC_ERROR;
}
//shut down the buffer pool and free all the memory allocated. write to disk all the pages before shutting down.
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	Frame *f = (Frame *)bm->mgmtData;
	Queue *q = f[0].qPointer;
	//writing all the data back to the disk.
	forceFlushPool(bm);
	int i;
	for(i = 0; i < bufferSize; i++)
		if(f[i].pinStatus != 0)
			return RC_ERROR;
	//freeing the memory allocated to the content variable in frame pointer.
	for(i=0;i< maxBufferSize;i++)
		free(f[i].content);
	free(q);
	free(f);
	//reinitialize all the variables back to the normal state.
	bufferSize = 0;
	isBufferFull = 0;
	currentQsize=0;
	bm->mgmtData = NULL;
	return RC_OK;
}

extern RC forceFlushPool(BM_BufferPool *const bm)
{
	Frame *f = (Frame *)bm->mgmtData;
	int i;
	for(i = 0; i < bufferSize; i++)
	{   //check if a page frame is drity, if its dirty write it back to the disk.
		if(f[i].isDirty == 1)
		{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			ensureCapacity(f[i].PageNumber,&fh);
			writeBlock(f[i].PageNumber, &fh, f[i].content);
			//make the dirty bit as 0.
			f[i].isDirty = 0;
			//increment the write count.
			writes++;
		}
	}
	return RC_OK;
}
//force write a page to the disk.
extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	Frame *f = (Frame *)bm->mgmtData;
	for(int i = 0; i < bufferSize; i++)
	{
		//force a page to the disk using the page number.
		if(f[i].PageNumber == page->pageNum)
		{
			//write the contents to the disk.
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(f[i].PageNumber, &fh, f[i].content);
			//increment the write count.
			writes++;
			f[i].isDirty= 0;
		}
	}
	return RC_OK;
}

//returns the contents of the frame.
extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber *pageNumbers = malloc(sizeof(PageNumber) * bufferSize);
	Frame *f = (Frame *) bm->mgmtData;
	int i = 0;
	//returns the number if its -1 it returns the constant NO_PAGE.
	while(i < maxBufferSize) 
	{
		pageNumbers[i] = (f[i].PageNumber != -1) ? f[i].PageNumber : NO_PAGE;
		i++;
	}
	return pageNumbers;
	free(pageNumbers);
}
//returns the boolean value(true,false) for the dirty bits in the page Frame.
extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool *dirtyBits = malloc(sizeof(bool) * bufferSize);
	Frame *f = (Frame *)bm->mgmtData;
	int i;
	for(i = 0; i < maxBufferSize; i++)
		dirtyBits[i] = (f[i].isDirty == 1) ? true : false;
	return dirtyBits;
	free(dirtyBits);
}
//returns the fix counts of the page frames.
extern int *getFixCounts (BM_BufferPool *const bm)
{
	int *fixCounts = malloc(sizeof(int) * bufferSize);
	Frame *f= (Frame *)bm->mgmtData;
	int i = 0;
	while(i < maxBufferSize)
	{
		fixCounts[i] = (f[i].fixCount != -1) ? f[i].fixCount : 0;
		i++;
	}
	return fixCounts;
	free(fixCounts);
}
//returns the number of reads done by the buffer manager.
extern int getNumReadIO (BM_BufferPool *const bm)
{
	return reads;
}
//returns the number of writes done by the buffer manager,
extern int getNumWriteIO (BM_BufferPool *const bm)
{
	return writes;
}