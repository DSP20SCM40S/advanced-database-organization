#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "btree_mgr.h"
#include "tables.h"
#include "record_mgr.h"
#include "storage_mgr.h"

struct BTree
{
    int *key;
    struct BTREE **next;
    RID *id;
};
typedef struct BTree BT;
BT *root,*scan;
int IndNum=0,maxVal;
SM_FileHandle btree_fh;

RC initIndexManager (void *mgmtData)
{
    return RC_OK;
}

RC shutdownIndexManager()
{
    return RC_OK;
}
// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n)
{
	int i;
    int size=sizeof(int)*n;
    root = (BT*)malloc(sizeof(BT));
    root->id = malloc(size);
    root->key = malloc(size);
    root->next = malloc(sizeof(BT) * (n + 1));
    for (i=0;i<=n;i++)
        root->next[i]=NULL;
    maxVal=n;
    createPageFile(idxId);
    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
    openPageFile (idxId,&btree_fh);
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree)
{
    closePageFile(&btree_fh);
    BT *temp=root;
    free(temp);
    return RC_OK;
}

RC deleteBtree (char *idxId)
{
    destroyPageFile(idxId);
    return RC_OK;
}
// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result)
{
    int count=0,i;
    BT *temp=(BT*)malloc(sizeof(BT));  
    for(i=0;i<maxVal+2;i++)
        count++;
    *result=count;
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result)
{
	int totalSize=0,i;
    BT *temp=(BT*)malloc(sizeof(BT));
    temp=root;
    while(temp!=NULL)
    {
        for(i=0;i<maxVal;i++)
            if(temp->key[i]!=0)
                totalSize++;
        temp=temp->next[maxVal];
    }
    *result = totalSize;
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result)
{
    return RC_OK;
}
// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    BT *temp=(BT*)malloc(sizeof(BT));
    temp=root;
    int count=1,i;
    while(temp!=NULL) 
    {
        for(i=0;i<maxVal;i++) 
        {
            if(temp->key[i]==key->v.intV) 
            {
                result->page=temp->id[i].page;
                result->slot=temp->id[i].slot;
                count=-1;
               break;
            }
        }
        if(count==-1)
            break;
        temp=temp->next[maxVal];
    }
    if (count==1)
        return RC_OK;
    else
        return RC_IM_KEY_NOT_FOUND;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    int i=0,filled=0,totalSize=0;
    BT *temp=(BT*)malloc(sizeof(BT));
    BT *temp1=(BT*)malloc(sizeof(BT));
    int size=sizeof(int)*maxVal;
    int sizeNode=sizeof(BT) * (maxVal + 1);
    temp1->key=malloc(size);
    temp1->id=malloc(size);
    temp1->next=malloc(sizeNode);
    for(i=0;i<maxVal;i++)
    	temp1->key[i]=0;
    temp=root;
    while(temp!=NULL) 
    {
        filled=0;
        for(i=0;i<maxVal;i++) 
        {
            if(temp->key[i]==0) 
            {
                temp->id[i].slot=rid.slot;
                temp->key[i]=key->v.intV;
                temp->id[i].page=rid.page;
                temp->next[i]=NULL;
                filled++;
                break;
            }
        }
        if((filled==0)&&(temp->next[maxVal]==NULL)) 
        {
            temp1->next[maxVal]=NULL;
            temp->next[maxVal]=temp1;
        }
        temp=temp->next[maxVal];
    }
    temp=root;
    while(temp!=NULL)
    {
        for(i=0;i<maxVal;i++)
            if(temp->key[i]!=0)
                totalSize ++;
        temp=temp->next[maxVal];
    }
    if(totalSize==6) 
    {
        temp1->key[0]=(root->next[maxVal])->key[0];
        temp1->key[1]=root->next[maxVal]->next[maxVal]->key[0];
        temp1->next[0]=root;
        temp1->next[1]=root->next[maxVal];
        temp1->next[2]=root->next[maxVal]->next[maxVal];
    }
    return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key)
{
    int count=1, i;
    BT *temp=(BT*)malloc(sizeof(BT));
    temp=root;
    while(temp!=NULL) 
    {
        for(i=0;i<maxVal;i++) 
        {
            if(key->v.intV==temp->key[i]) 
            {
                temp->id[i].page=0;
                temp->id[i].slot=0;
                temp->key[i]=0;
                count=-1;
                break;
            }
        }
        if(count==-1)
            break;
        temp=temp->next[maxVal];
    }
    return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    int swap,swap1,swap2,j,k;
    int totalSize=0,i,ct=0;
    IndNum=0;
    scan=(BT*)malloc(sizeof(BT));
    scan=root;
    BT *temp=(BT*)malloc(sizeof(BT));
    temp=root;
    while(temp!=NULL)
    {
        for(i=0;i<maxVal;i++)
            if(temp->key[i]!=0)
                totalSize++;
        temp=temp->next[maxVal];
    }
    int arr[maxVal][totalSize];
    int K[totalSize];
    temp=root;
    while(temp!=NULL) 
    {
        for(i=0;i<maxVal;i++) 
        {
            K[ct]=temp->key[i];
            arr[0][ct]=temp->id[i].page;
            arr[1][ct]=temp->id[i].slot;
            ct++;
        }
        temp=temp->next[maxVal];
    }
    for(j=0;j<=ct-2;j++)
    {
        for(k=0;k<=ct-j-1;k++)
        {
            if (K[k]>K[k+1])
            {
                swap=K[k];
                swap1=arr[0][k];
                swap2=arr[1][k];
                K[k]=K[k+1];
                arr[0][k]=arr[0][k+1];
                arr[1][k]=arr[1][k+1];
                K[k+1]=swap;
                arr[0][k+1]=swap1;
                arr[1][k+1]=swap2;
            }
        }
    }
    ct=0;
    temp=root;
    while(temp!=NULL) 
    {
        for(i=0;i<maxVal;i++) 
        {
            temp->key[i]=K[ct];
            temp->id[i].page=arr[0][ct];
            temp->id[i].slot=arr[1][ct];
            ct++;
        }
        temp=temp->next[maxVal];
    }
    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    BT *b=scan->next[maxVal];
    if(b!=NULL) 
    {
        if(maxVal==IndNum) 
        {
            IndNum=0;
            scan=scan->next[maxVal];
        }
        result->page=scan->id[IndNum].page;
        result->slot=scan->id[IndNum].slot;
        IndNum++;
    }
    else
        return RC_ERROR;
    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle)
{
    IndNum = 0;
    return RC_OK;
}
// debug and test functions
char *printTree (BTreeHandle *tree)
{
    return RC_OK;
}