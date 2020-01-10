# README #

### Contributors ###
Team Members: - Group 27
	
1. Karthik Kini

2. Disha Sharma
	
3. Vaishnavi Chaudhari


### Description ###
Implementing the interface of a storage manager that allows read/writing of blocks to/from a file on disk into memory.

### How to run ###
1. Open terminal and Clone from BitBucket repository to the required location.
2. Navigate to assign1 folder.
3. Use make command to execute the Makefile, type make

### Running the Script ###
1. Go to Project directory(assign1).
2. Type ls to list the files and check that we are in the correct directory.
3. Type "make clean" to delete old compiled .o files.
4. Type "make" to compile all project files including "test_assign1_1.c" file 
5. Type "make runTest1" to run "test_assign1_1.c" file.
6. Type "make test2" to compile Custom test file "test_assign1_2.c".
7. Type "make runTest2" to run "test_assign1_2.c" file.
### Solution Description ###
createPageFile() -->
In this method Page File is created and allocation of '\0' bytes to the firstPage page is done. In order to store additional file information like total number of pages in it the header page is reserved at the start of the page file 

openPageFile() -->
This method opens an existing page file specified by fileName. It is used to store page file information/ attributes like curPagePos, totalNumPages and mgmtInfo. The totalNumPage information is set from the headerPage of the page file. Throws RC_FILE_NOT_FOUND error if page file not found to be opened. 
closePageFile & destroyPageFile -->
 This method destroys or loses an open page file. Throws RC_FILE_NOT_FOUND error if page file not found is opened.

readBlock() -->
 This method reads the pageNumth block from a file and stores its content in the memory pointed to the memPage page handle. Saves/ Updates the curPagePos information of the page file. If the file has less than pageNum pages, the method returns RC_READ_NON_EXISTING_PAGE.

getBlockPos() -->
 This method is used to return the current page position of the file, that is available in fileHandle attribute 'curPagePos'.

readFirstBlock(), readLastBlock() -->
 Reads the first and last page of a file by sending the pageNumth information while calling the readBlock method.

readPreviousBlock(), readCurrentBlock(), readNextBlock() --> 
This method reads the current, previous, or next page relative to the curPagePos of the file by sending the pageNumth information while calling the readBlock method. The curPagePos is moved to the page that was read. If the user tries to read a block before the first page or after the last page of the file, the method returns RC_READ_NON_EXISTING_PAGE error.

writeBlock(), writeCurrentBlock() -->
 This method writes a page to disk using either the current position or an absolute position. For writeBlock() method if absolute position is out of range then it returns RC_WRITE_FAILED error and for failure of write operations on any page of file it returns RC_WRITE_FAILED error.

appendEmptyBlock() --> 
This method appends the pagefile by one page (Empty Block). The value of the totalNumPage along with the headerPage page information is also incremented by one. The new last page is filled with zero bytes. The curPagePos is set to the position of the appended page in the file. Returns RC_WRITE_FAILED error if write operation fails on the page of the file.

ensureCapacity() --> 
In this method the size of the file is increased to numberOfPages by calling appendEmptyBlock method to add the remaining empty pages to the existing pages in the file.

### Test Cases ###
The program verifies all the test cases that are mentioned in the test file i.e test_assign1_1 and ensures that there are no errors. Along with the default test case given, there is an additional test case prepared i.e test_assign1_2 which tests all the methods that have been implemented, and runs successfully. Furthermore, in the implementation design we have taken steps to avoid memory leaks.
To Run the additional Test Case
In terminal,

1. To clean, type make clean

2. Type make test2

3. Type make runTest2