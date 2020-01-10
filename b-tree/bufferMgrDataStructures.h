#include "storage_mgr.h"
//A Frame's structure which is present on the RAM.
typedef struct frame
{
		char frameData[PAGE_SIZE + 1]; //+1 to store /0 character
		struct frame *nextFramePtr;
}frameList;


//Map to store PageFrameNumber and the its corresponding discPageNumber;
typedef struct ramPageMap
{
	int ramPageFrameNumber;
	int discPageFrameNumber;
	bool clockReferenceBit;
	struct ramPageMap *nextRamPageMap;

}ramPageMap;

typedef struct pageAndItsFixCount
{
	int ramPageFrameNumber;
	int fixCount;
	struct pageAndItsFixCount *nextPageFixCount;

}pageAndFixCount;

typedef struct pageAndDirtyBitIndicator
{
	int ramPageFrameNumber;
	char  isPageDirty;
	struct pageAndDirtyBitIndicator *nextPageDirtyBit;

}pageAndDirtyBitIndicator;

//Global variables [START]
frameList *firstFramePtr = NULL;
ramPageMap *firstRamPageMapPtr = NULL;
pageAndDirtyBitIndicator *firstPageAndDirtyBitMap = NULL;
pageAndFixCount *firstPageAndFixCountPtr = NULL;
ramPageMap *clockPtr = NULL;
SM_FileHandle fHandle;
int numberOfFrames = 0;
PageNumber *frameContentPtr;
bool *dirtyBitPtr;
int *fixCountPtr;
int numberOfWrites = 0;
int numberOfReads = 0;
//Global variables [END]
