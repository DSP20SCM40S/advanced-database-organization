#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */

#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */

static void extraTestCases1(void);
static void extraTestCases2(void);

/* main function running all tests */

int main (void)
{
  testName = "";  
  extraTestCases1();
  extraTestCases2();
  return 0;
}

/* check a return code. If it is not RC_OK then output a message, error description, and exit */

void extraTestCases1(void)
{ 
  SM_FileHandle fh;
  SM_PageHandle pg;
  int i;

  testName = "Test for checking single page's content";
  pg=(SM_PageHandle)malloc(PAGE_SIZE);
  TEST_CHECK(createPageFile(TESTPF));
  TEST_CHECK(openPageFile(TESTPF,&fh));
  printf("Created and opened file\n");

  //test to read first page into handle
  TEST_CHECK(readFirstBlock(&fh,pg));
  
  //check for empty block
  for(i=0;i<PAGE_SIZE;i++)
    ASSERT_TRUE((pg[i]==0),"expected zero byte in first page of freshly initialized page");
  printf("First block checked successfully\n");

  //to change pg to be a string type and write it to disk
  for(i=0;i<PAGE_SIZE;i++)
    pg[i]='A'+(i%26);
  TEST_CHECK(writeBlock(0,&fh,pg));
  printf("First block written successfully\n");

  //to read the page again to check for the written string
  TEST_CHECK(readBlock(0,&fh,pg));
  for(i=0;i<PAGE_SIZE;i++)
    ASSERT_TRUE((pg[i]=='A'+(i%26)),"Page read from the disk matches with the string written to it");
  printf("First block checked successfully\n");

  //edit string pg and write it to the disk as a second block
  for(i=0;i<PAGE_SIZE;i++)
    pg[i]=(i%10);
  TEST_CHECK(writeBlock(1,&fh,pg));
  printf("Second block written successfully\n");

  //read back the page containing the string and check that it is correct for second block with readCurrentBlock
  TEST_CHECK(readCurrentBlock(&fh,pg));
  for(i=0;i<PAGE_SIZE;i++)
    ASSERT_TRUE((pg[i]==(i%10)),"Page read from the disk matches with the string written to it");
  printf("Second block checked successfully\n");

  //increment the number of pages in the file by 1.
  TEST_CHECK(appendEmptyBlock(&fh));
  ASSERT_TRUE((fh.totalNumPages==3),"The total number of pages should be 3");

  //read the previous page
  TEST_CHECK(readPreviousBlock(&fh,pg));
  for(i=0;i<PAGE_SIZE;i++)
    ASSERT_TRUE((pg[i]==(i%10)),"Page read from the disk matches with the string written to it");
  printf("Previous block (page 2) read successfully\n");

  //read the next page as pointer has moved to the second page
  TEST_CHECK(readNextBlock(&fh,pg));
  for(i=0;i<PAGE_SIZE;i++)
    ASSERT_TRUE((pg[i]==0),"expected 0B in the last page");
  printf("Third block checked successfully\n");

  //edit the string pg and write it to the third page on disk
  for(i=0;i<PAGE_SIZE;i++)
    pg[i]=(i%26)+'a';
  TEST_CHECK(writeCurrentBlock(&fh, pg));
  printf("Third block written successfully\n");

  //increase the capacity to seven so that there is some empty space to store on disk 
  TEST_CHECK(ensureCapacity (7, &fh));
  printf("%d\n",fh.totalNumPages);
  ASSERT_TRUE((fh.totalNumPages==7),"adding four more pages to ensure some space for the file");

  //to verify if the pages are appended to the file
  ASSERT_TRUE((fh.curPagePos==6),"the pointer should point to the last page of the file");

  //read the last page
  TEST_CHECK(readLastBlock (&fh, pg));

  //the last page should be empty
  for(i=0;i<PAGE_SIZE;i++)
    ASSERT_TRUE((pg[i]==0),"expected 0B in the last page");
  printf("Last block checked successfully\n");
  
  //destroy the file
  TEST_CHECK(destroyPageFile(TESTPF));
  free(pg);
  TEST_DONE();
}


void extraTestCases2(void)
{
  SM_FileHandle fh;
  SM_PageHandle pg;
  int i;
  testName="Test for second page content";
  pg=(SM_PageHandle)malloc(PAGE_SIZE);

  //creating a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file succesfully\n");

  //fh.totalNumPages=2;
  for(i=0;i<PAGE_SIZE;i++)
    pg[i]=(i%10)+'0';
  TEST_CHECK(writeBlock(0,&fh,pg));
  printf("First block written successfully\n");
  for(i=0;i<PAGE_SIZE;i++)
    pg[i]=(i%26)+'A';
  TEST_CHECK(writeBlock(1,&fh,pg));
  printf("Second block written succesfully\n");

  //read the first page to verify its contents
  TEST_CHECK(readPreviousBlock(&fh,pg));
  for(i=0;i<PAGE_SIZE;i++)
    ASSERT_TRUE((pg[i]==(i%10)+'0'),"the page contains the string written to the disc");
  printf("Previous(first) block checked successfully\n");

  //read back the page containing the string and check that it is correct
  TEST_CHECK(readNextBlock(&fh,pg));
  for(i=0;i<PAGE_SIZE;i++)
    ASSERT_TRUE((pg[i]==(i%26)+'A'),"the page contains the string written to the disc");
  printf("Next(Second) block checked successfully\n");

  //read back the page containing the string and check that it is correct
  TEST_CHECK(readLastBlock(&fh,pg));
  for(i=0;i<PAGE_SIZE;i++)
    ASSERT_TRUE((pg[i]==(i%26)+'A'),"the page contains the string written to the disc");
  printf("Previous(first) block checked successfully\n");

  //writing block at current position
  for(i=0;i<PAGE_SIZE;i++)
    pg[i]=(i%26)+'a';
  TEST_CHECK(writeCurrentBlock(&fh,pg));
  printf("First block rewritten successfully\n");

  //Appending empty block to the file
  TEST_CHECK(appendEmptyBlock(&fh));
  printf("Empty block appended to the file\n");

  printf("\nTotal number of pages: %d\n", fh.totalNumPages);
    
  //Increasing the capacity of the file is 5 pages
  TEST_CHECK(ensureCapacity(5,&fh));
  ASSERT_TRUE((fh.totalNumPages==5),"checking total number of pages is 5");
  printf("The number of pages in file is increased to 5\n");

  TEST_CHECK(destroyPageFile(TESTPF));
  TEST_DONE();
}