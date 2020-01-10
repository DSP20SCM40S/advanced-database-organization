
#include "buffer_mgr.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "bufferMgrDataStructures.h"

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) 
{
	pageAndDirtyBitIndicator *temp;
	ramPageMap *st = firstRamPageMapPtr;
	while(st->discPageFrameNumber != page->pageNum)
		st = st->nextRamPageMap;
	temp = firstPageAndDirtyBitMap;
	while(st->ramPageFrameNumber != temp->ramPageFrameNumber)
		temp = temp->nextPageDirtyBit;
	if(temp != NULL)
	{
		temp->isPageDirty = 1;
		return RC_OK;
	}
	else
		return RC_DIRTY_UPDATE_FAILED;
}

frameList *createBufferPool(int numPages)
{
	int i = 1;
	frameList *curr = NULL, *prev = NULL;
	while(i <= numPages) //Ex : create 3 pages and link them to form a list.
	{
		curr = (frameList *)malloc(sizeof(frameList));
		if(i == 1)
			firstFramePtr = curr; // firstFramePtr. deallocate Pool using this.
		else
			prev->nextFramePtr = curr;
		prev = curr;
		i++;
	}
	curr->nextFramePtr = NULL;
	return firstFramePtr;
}

ramPageMap *createRamPageMapList(int numPages)
{
	int i = 0;
	ramPageMap *cur = NULL;
	ramPageMap *pre = NULL;
	ramPageMap *st = NULL;
	while(i < numPages) //Ex : create 3 Maps and link them.
	{
		cur = (ramPageMap *)malloc(sizeof(ramPageMap));
		if(i == 0)
		{
			st = cur; // firstFramePtr. deallocate Pool using this.
			clockPtr = st;
		}
		else
			pre->nextRamPageMap = cur;
		cur->ramPageFrameNumber = i;
		cur->discPageFrameNumber = -1;
		cur->clockReferenceBit = 0;
		pre = cur;
		i++;
	}
	cur->nextRamPageMap = NULL;
	return st;
}

pageAndDirtyBitIndicator *createPageAndDirtyBitMap(int numPages)
{
	int i = 0;
	pageAndDirtyBitIndicator *current = NULL;
	pageAndDirtyBitIndicator *previous = NULL;
	pageAndDirtyBitIndicator *st = NULL;
	while(i < numPages) //Ex : create 3 Maps and link them.
	{
		current = (pageAndDirtyBitIndicator *)malloc(sizeof(pageAndDirtyBitIndicator));
		if(i == 0)
			st = current; // firstFramePtr. deallocate Pool using this.
		else
			previous->nextPageDirtyBit = current;
		current->ramPageFrameNumber = i;
		current->isPageDirty = 0;
		previous = current;
		i++;
	}
	current->nextPageDirtyBit = NULL;
	return st;
}

pageAndFixCount *createPageAndFixCountMap(int numPages)
{
	int i = 0;
	pageAndFixCount *c = NULL;
	pageAndFixCount *p = NULL;
	pageAndFixCount *st = NULL;
	while(i < numPages) //Ex : create 3 Maps and link them.
	{
		c = (pageAndFixCount *)malloc(sizeof(pageAndFixCount));
		if(i == 0)
			st = c; // firstFramePtr. deallocate Pool using this.
		else
			p->nextPageFixCount = c;
		c->ramPageFrameNumber = i;
		c->fixCount = 0;
		p = c;
		i++;
	}
	c->nextPageFixCount = NULL;
	return st;
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		  const int numPages, ReplacementStrategy strategy,
		  void *stratData)
{
	numberOfWrites = 0;
	numberOfReads  = 0;
	frameContentPtr = (PageNumber *)malloc(sizeof(PageNumber) * numPages);
	dirtyBitPtr = (bool *)malloc(sizeof(bool) * numPages);
	fixCountPtr = (int *)malloc(sizeof(int) * numPages);
	numberOfFrames = numPages;
	bm->mgmtData = createBufferPool(numPages); //gives address of the first Frame.
	firstRamPageMapPtr = createRamPageMapList(numPages);//give address of the first RamPage Map.
	firstPageAndDirtyBitMap = createPageAndDirtyBitMap(numPages);
	firstPageAndFixCountPtr = createPageAndFixCountMap(numPages);
	openPageFile((char *)pageFileName, &fHandle);
	bm->numPages = numPages;
	bm->pageFile = (char *)pageFileName;
	bm->strategy = strategy;
	if(bm->mgmtData != NULL && firstRamPageMapPtr !=NULL && firstPageAndDirtyBitMap != NULL && firstPageAndFixCountPtr != NULL)
		return RC_OK;
	else
		return RC_BUFFER_POOL_INIT_ERROR;
}

RC checkIfPagePresentInFramePageMaps(const PageNumber pageNum)
{
	ramPageMap *i = firstRamPageMapPtr;
	while(i != NULL)
	{
		if(i->discPageFrameNumber == pageNum)
			return i->ramPageFrameNumber;
		i = i->nextRamPageMap;
	}
	return RC_NO_FRAME;
}

void getFrameData(int frameNumber,BM_PageHandle * page)
{
	frameList *i = firstFramePtr;
	int i = 0;
	while(i < frameNumber)
	{
		i = i->nextFramePtr;
		i++;
	}
	page->data = i->frameData;
}

void getFirstFreeFrameNumber(int *firstfreeFrameNumber,PageNumber PageNum)
{
	ramPageMap *i = firstRamPageMapPtr;
	while(i != NULL && i->discPageFrameNumber != -1)
		i = i->nextRamPageMap;
	if(i != NULL)
	{
		*firstfreeFrameNumber = i->ramPageFrameNumber;
		i->discPageFrameNumber = PageNum;
	}
	else
		*firstfreeFrameNumber = -99;
}

RC changeFixCount(int flag,int page)
{
	ramPageMap *startFramePtr = firstRamPageMapPtr;
	while(startFramePtr != NULL && startFramePtr->discPageFrameNumber != page)
		startFramePtr = startFramePtr->nextRamPageMap;
	pageAndFixCount *startFixCountPtr = firstPageAndFixCountPtr;
	while((startFixCountPtr != NULL) && (startFixCountPtr->ramPageFrameNumber != startFramePtr->ramPageFrameNumber))
		startFixCountPtr = startFixCountPtr->nextPageFixCount;
	if(flag == 1)
		startFixCountPtr->fixCount++;
	else
		startFixCountPtr->fixCount--;
	return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	if(writeBlock (page->pageNum, &fHandle, page->data) == RC_OK)
	{
		numberOfWrites++;
		return RC_OK;
	}
	else
		return RC_WRITE_FAILED;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	ramPageMap *startFramePtr = firstRamPageMapPtr;
	while(startFramePtr != NULL && startFramePtr->discPageFrameNumber != page->pageNum)
		startFramePtr = startFramePtr->nextRamPageMap;
	RC status = changeFixCount(2,startFramePtr->discPageFrameNumber); //
	int frameNumber = startFramePtr->ramPageFrameNumber;//here
	//If dirty, call forcepage.
	pageAndDirtyBitIndicator *startDirtyPointer = firstPageAndDirtyBitMap;
	while(startDirtyPointer != NULL && startDirtyPointer->ramPageFrameNumber != frameNumber)
		startDirtyPointer = startDirtyPointer->nextPageDirtyBit;
	if(startDirtyPointer->isPageDirty == 1)
		forcePage(bm,page);
	if(status == RC_OK)
		return RC_OK;
	else
		return RC_UNPIN_FAILED;
}

RC FIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum,SM_PageHandle ph)
{
	pageAndFixCount *fixCountStart = firstPageAndFixCountPtr;
	//getFrameNumber of firstNode in ramPagePtr;
	//go to that fixCountPtr whose frameNumber = above frameNumber,
	//and check its fix count for > 0.
	ramPageMap *begin =firstRamPageMapPtr;
	while(fixCountStart != NULL && fixCountStart->ramPageFrameNumber != begin->ramPageFrameNumber)
		fixCountStart = fixCountStart->nextPageFixCount;
	if(fixCountStart != NULL && fixCountStart->fixCount > 0) //means node should'nt be deleted...
	{
		begin = firstRamPageMapPtr;
		int FrameNumberOfNewPage = begin->nextRamPageMap->ramPageFrameNumber;
		//make a new node ,add discPageDetails to it and make it last node
		//try writing this in a function...
		ramPageMap *cur = (ramPageMap *)malloc(sizeof(ramPageMap));
		cur->discPageFrameNumber = pageNum;
		cur->ramPageFrameNumber = FrameNumberOfNewPage;
		cur->nextRamPageMap = begin->nextRamPageMap->nextRamPageMap;
		free(begin->nextRamPageMap);
		begin->nextRamPageMap = cur;
		//write to buffer..
		int i = 0;
		frameList *beginFrame = firstFramePtr;
		while(i < FrameNumberOfNewPage)
		{
			beginFrame = beginFrame->nextFramePtr;
			i++;
		}
		memset(beginFrame->frameData,'\0',PAGE_SIZE + 1);
		if(ph != NULL)
			strcpy(beginFrame->frameData,ph);
		//strcpy(page->data,beginFrame->frameData);
		page->data = beginFrame->frameData;
		//here [START]
		//page->pageNum = FrameNumberOfNewPage;
		page->pageNum = pageNum;
		//here [END]
	}
	//get first node's frame Number;
	else
	{
		int frameNumberOfNewPage = firstRamPageMapPtr->ramPageFrameNumber;
		//remove the first node[START];
		ramPageMap *temp;
		temp = firstRamPageMapPtr;
		firstRamPageMapPtr = firstRamPageMapPtr->nextRamPageMap;
		free(temp);
		temp = NULL;
		//make a new node ,add discPageDetails to it and make it last node
		ramPageMap *cur = (ramPageMap *)malloc(sizeof(ramPageMap));
		cur->discPageFrameNumber = pageNum;
		cur->ramPageFrameNumber = frameNumberOfNewPage;
		cur->nextRamPageMap = NULL;
		temp = firstRamPageMapPtr;
		while(temp->nextRamPageMap != NULL)
			temp = temp->nextRamPageMap;
		temp->nextRamPageMap = cur;
		//writing data to buffer
		int i = 0;
		frameList *beginFrame = firstFramePtr;
		while(i < frameNumberOfNewPage)
		{
			beginFrame = beginFrame->nextFramePtr;
			i++;
		}
		memset(beginFrame->frameData,'\0',PAGE_SIZE + 1);
		if(ph != NULL)
			strcpy(beginFrame->frameData,ph);
		//strcpy(page->data,beginFrame->frameData);
		page->data = beginFrame->frameData;
		page->pageNum = pageNum;
	}
	return RC_OK;
}
//All these are dummy methods. Required functionality has to be added later[START]

void attachAtEndOfList(ramPageMap *temp)
{
	ramPageMap *i = firstRamPageMapPtr;
	while(i->nextRamPageMap != NULL)
		i = i->nextRamPageMap;
	i->nextRamPageMap = temp;
}
void sortFixCounts(int *intArray, int size)
{
      char flag = 'Y';
      int j = 0;
      int temp;
      while (flag == 'Y')
      {
            flag = 'N';j++;int i;
            for (i = 0; i <size-j; i++)
            {
				if (intArray[i] > intArray[i+1])
				{
					temp = intArray[i];
					intArray[i] = intArray[i+1];
					intArray[i+1] = temp;
					flag = 'Y';
				}
            }
      }
}
void moveClockPtr()
{
	if(clockPtr->nextRamPageMap == NULL)
		clockPtr = firstRamPageMapPtr;
	else
		clockPtr = clockPtr->nextRamPageMap;
}
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	SM_PageHandle ph = (SM_PageHandle)malloc(PAGE_SIZE);
	//call ensure capacity.
	ensureCapacity((pageNum + 1),&fHandle);
	PageNumber frameNumber = checkIfPagePresentInFramePageMaps(pageNum);
	if(frameNumber != RC_NO_FRAME)
	{
		page->pageNum = frameNumber;
		getFrameData(frameNumber,page);
		page->pageNum = pageNum;
		if(bm->strategy == RS_LRU)
		{
			ramPageMap *temp = firstRamPageMapPtr;
			ramPageMap *prev = NULL;
			int i = 0;
			while(temp !=NULL && temp->discPageFrameNumber != pageNum)
			{
				prev = temp;
				temp = temp->nextRamPageMap;
				i++;
			}
			if(temp != NULL)
			{
				if(i == 0)
				{
					prev = firstRamPageMapPtr;
					firstRamPageMapPtr = firstRamPageMapPtr->nextRamPageMap;
					prev->nextRamPageMap = NULL;
					attachAtEndOfList(temp);
				}
				else
				{
					prev->nextRamPageMap = temp->nextRamPageMap;
					temp->nextRamPageMap = NULL;
					attachAtEndOfList(temp);
				}
			}
		}
		else if(bm->strategy == RS_CLOCK)
		{
			ramPageMap *i = firstRamPageMapPtr;
			while(frameNumber != i->ramPageFrameNumber)
			{
				i = i->nextRamPageMap;
			}
			i->clockReferenceBit = 1;
		}
	}
	else
	{
		int freeframeNumber = - 99;
		getFirstFreeFrameNumber(&freeframeNumber,pageNum);
		readBlock(pageNum, &fHandle,ph);
		numberOfReads++;
		if(freeframeNumber != -99)
		{
			int i = 0;
			frameList *st = firstFramePtr;
			while(i < freeframeNumber)
			{
				st = st->nextFramePtr;
				i++;
			}
			memset(st->frameData,'\0',PAGE_SIZE+1);
			if(ph != NULL)
				strcpy(st->frameData,ph);
			page->data = st->frameData;
			page->pageNum = pageNum;
			ramPageMap *begin = firstRamPageMapPtr;
			while(begin->ramPageFrameNumber != freeframeNumber)
				begin = begin->nextRamPageMap;
			begin->discPageFrameNumber = pageNum;
			//go to freeframeNumberTh [END]
			if(bm->strategy == RS_CLOCK)
			{
				clockPtr->clockReferenceBit = 0;
				moveClockPtr();
			}
		}
		else
		{
			if(bm->strategy == RS_FIFO || bm->strategy == RS_LRU)
				FIFO(bm,page,pageNum,ph);
			else if(bm->strategy == RS_LFU)
			{
				pageAndFixCount *st = firstPageAndFixCountPtr;
				int sortedFixCountArray[bm->numPages];
				int index = 0;
				while(st != NULL)
				{
					sortedFixCountArray[index++] = st->fixCount;
					st = st->nextPageFixCount;
				}
				sortFixCounts(sortedFixCountArray,bm->numPages);
				st = firstPageAndFixCountPtr;
				while(st->fixCount != sortedFixCountArray[0])
					st = st->nextPageFixCount;
				st->fixCount = 0;
				ramPageMap *tempRPM = firstRamPageMapPtr;
				while(tempRPM->ramPageFrameNumber != st->ramPageFrameNumber)
					tempRPM = tempRPM->nextRamPageMap;
				tempRPM->discPageFrameNumber = pageNum;
				page->pageNum = pageNum;
			}
			else if(bm->strategy == RS_CLOCK)
			{
				while(clockPtr->clockReferenceBit == 1)
				{
					clockPtr->clockReferenceBit = 0;
					moveClockPtr();
				}
				clockPtr->discPageFrameNumber = pageNum;
				moveClockPtr();
				page->pageNum = pageNum;
			}
		}
	}
	changeFixCount(1,pageNum);
	free(ph);
	ph = NULL;
	return RC_OK;
}

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	ramPageMap *i = firstRamPageMapPtr;
	while(i != NULL)
	{
		frameContentPtr[i->ramPageFrameNumber] = i->discPageFrameNumber;
		i = i->nextRamPageMap;
	}
	return frameContentPtr;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
	pageAndDirtyBitIndicator *st = firstPageAndDirtyBitMap;
	int i = 0;
	while(st != NULL)
	{
		if(st->isPageDirty == 1)
			dirtyBitPtr[i++] = true;
		else
			dirtyBitPtr[i++] = false;
		st = st->nextPageDirtyBit;
	}
	return dirtyBitPtr;
}
int *getFixCounts (BM_BufferPool *const bm)
{
	pageAndFixCount *st = firstPageAndFixCountPtr;
	int i = 0;
	while(st != NULL)
	{
		fixCountPtr[i++] = st->fixCount;
		st = st->nextPageFixCount;
	}
	return fixCountPtr;
}
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	bool NonZeroFixIndicator = false;
	pageAndFixCount *st = firstPageAndFixCountPtr;
	while(st != NULL)
	{
		if(st->fixCount >0)
		{
			NonZeroFixIndicator = true;
			break;
		}
		st = st->nextPageFixCount;
	}
	st = NULL;
	if(NonZeroFixIndicator == false)
	{
		clockPtr = NULL;
		forceFlushPool(bm);
		free(frameContentPtr);
		free(dirtyBitPtr);
		free(fixCountPtr);
		int i = 0;
		ramPageMap *RPMtemp = NULL;
		pageAndDirtyBitIndicator *PADBItemp = NULL;
		pageAndFixCount *PAFCtemp = NULL;
		frameList *FLtemp = NULL;
		frameList *firstFramePtr = NULL;
		firstFramePtr = (frameList*) bm->mgmtData;
		bm->mgmtData = NULL;
		while(i < bm->numPages)
		{
			PAFCtemp = firstPageAndFixCountPtr;
			firstPageAndFixCountPtr = firstPageAndFixCountPtr->nextPageFixCount;
			free(PAFCtemp);
			RPMtemp = firstRamPageMapPtr;
			firstRamPageMapPtr = firstRamPageMapPtr->nextRamPageMap;
			free(RPMtemp);
			PADBItemp = firstPageAndDirtyBitMap;
			firstPageAndDirtyBitMap = firstPageAndDirtyBitMap->nextPageDirtyBit;
			free(PADBItemp);
			FLtemp = firstFramePtr;
			firstFramePtr = firstFramePtr->nextFramePtr;
			free(FLtemp);
			i++;
		}
		firstPageAndFixCountPtr = NULL;
		firstRamPageMapPtr = NULL;
		firstPageAndDirtyBitMap = NULL;
		closePageFile(&fHandle);
	}
	if(firstPageAndFixCountPtr == NULL && firstRamPageMapPtr == NULL && firstPageAndDirtyBitMap == NULL && bm->mgmtData ==NULL && NonZeroFixIndicator == false)
		return RC_OK;
	else
		return RC_SHUT_DOWN_ERROR;
}

int getNumWriteIO(BM_BufferPool *const bm)
{
	return numberOfWrites;
}

int getNumReadIO(BM_BufferPool *const bm)
{
	return numberOfReads;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
	//If a frame is Dirty in DirtyBitIndicator,from BufferPool, write that frame onto disc.
	pageAndDirtyBitIndicator *dirtyStart = firstPageAndDirtyBitMap;
	frameList *frameStart = firstFramePtr;
	while (dirtyStart != NULL)
	{
		if(dirtyStart->isPageDirty == 1)
		{
			ramPageMap *ramPageStart = firstRamPageMapPtr;
			while(ramPageStart->ramPageFrameNumber != dirtyStart->ramPageFrameNumber)
				ramPageStart = ramPageStart->nextRamPageMap;
			writeBlock (ramPageStart->discPageFrameNumber, &fHandle,frameStart->frameData);
			dirtyStart->isPageDirty = 0;
		}
		dirtyStart = dirtyStart->nextPageDirtyBit;
		frameStart = frameStart->nextFramePtr;
	}
	//If a page is dirty, write it back to the disc.
	return RC_OK;
}
