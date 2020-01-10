________________________________________________________________________________
Assignment 4: B+ - Tree Index

Team: 
	1. Karthik Kini
	2. Disha Sharma
	3. Vaishnavi Chaudhari

Contents:

---------

1)Instructions to run the code

2)Description of functions used

3)Changes in test_assign4_1.c
1)Instructions to run the code

-------------------------------



For Executing Test Cases:

1. Change the directory to the assignemnt directory.(i.e. assignment4)

2. Issue "make" in the terminal to make the Object files.

3. For test cases issue:

	./test_assign4
	./test_expr

4. Issue "make clean" in the terminal to clean all the object files.


2)Description of functions used

-------------------------------


		----------------------

		//B+-Tree Functions//
		---------------------	

	createBtree Function
	--------------------              	
		This Function creates a B tree index.
		
	deleteBtree Function
	--------------------
		This Function deletes a B tree index and removes the corresponding page file.

	openBtree Function
	--------------------
		This Functions opens the B tree index created.

	closeBtree Function
	------------------
		This Function closes the B tree index which is opened and the index manager 
ensures that all new or modified pages of the index are flushed back to disk. 


		------------------
		//Key Functions//
		-----------------

	findKey Function
	----------------
		This Function returns the RID for the entry with the search key in the b-tree 
and if the key does not exist it returns RC_IM_KEY_NOT_FOUND.

	insertKey Function
	------------------
		This Function inserts a new key and a record pointer pair into the index and 
if that key is already stored in the b-tree it returns the error code RC_IM_KEY_ALREADY_EXISTS .

	deleteKey Function
	------------------
		This Function removes a key and the corresponding record pointer from the index 
and if the key is not in the index then returns RC_IM_KEY_NOT_FOUND. (For deletion it is up to 
the client whether this is handled as an error.)

	openTreeScan Function
	---------------------
		This Function opens the tree for a scan through all entries of a Btree . 

	nextEntry Function
	------------------
		This Function reads the next entry in the Btree.

	closeTreeScan Function
	---------------------
		This Function closes the tree after scanning through all the elements of the B tree.


		--------------------------------
		//Access Information Functions//
		--------------------------------

	getNumNodes Function
	---------------------
		This Function calculates the total number of nodes in the Btree.


	getNumEntries Function
	----------------------
		This Function calculates the total number of entries in the Btree.

	getKeyType Function
	--------------------
		This Function returns the keytype.


		------------------
		//Debug Functions//
		------------------

	printTree Function
	-------------------
		This Function is used to create a string representation of a b-tree. 




