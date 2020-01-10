#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"

struct DBManager 
{
    BM_PageHandle pg;
    BM_BufferPool bf;
    int rowCount;
    int freeCount;
    char attrName[5];
    int attrLength[5];
    int attrType[5];
    int attrCount;
    int recordCount;
};
typedef struct DBManager DB;
int attrSize=10;
int snCount=0;
int pageNumber=0;
DB *d;
// ======= TABLE AND MANAGER FUNCTIONS ======== //
//Initializes the record manager by initializing the storage manager.
extern RC initRecordManager(void *mgmtData)
{
    initStorageManager();
    return RC_OK;
}

//It shuts down the record manager and also the Buffer Manager.
extern RC shutdownRecordManager ()
{
    shutdownBufferPool(&d->bf);
    free(d);
    return RC_OK;
}
//Creates the Table using the storage manager functions and A FIFO Queue with size 40.
extern RC createTable (char *name, Schema *schema)
{
    int i,j,k=sizeof(int),pos=0,check;
    //allocate memory for our data structure.
    d =(DB *) malloc(sizeof(DB));
    //create a buffer manager with 40 pages and a FIFO queue.
    if(initBufferPool(&d->bf,name,40,RS_FIFO,NULL)==RC_OK)
    {
        //allocate a string equal to the size of a page.
        char *Page;
        char pageData[PAGE_SIZE];
        //make all the characters in the page as '-' which is a keyword for this program.
        for(i=0;i<PAGE_SIZE;i++)
            pageData[i]='-';
        //store the first free page of the table. We use 0 page for schema.
        pageData[pos]='1';
        pos+=k;
        d->freeCount = 1;
        //store the number of rows in the table. when creating new table its always 0.
        pageData[pos]='0';
        pos+=k;
        d->rowCount=0;
        char samp[30];
        //store the number of attribues in the schema
        sprintf(samp,"%d",schema->numAttr);
        pageData[pos]=*samp;
        pos+=k;
        //copy it to our data structure.
        d->attrCount=schema->numAttr;
        //store all the attributes of the schema to the table.
        for(i=0;i<schema->numAttr;i++) 
        {
            Page=&pageData[pos];
            //copy the attribute names.
            for(j=0; j<strlen(schema->attrNames[i]); j++) 
                pageData[pos] = schema->attrNames[i][j];
            char *samp = &d->attrName[i];
            strncpy(samp,schema->attrNames[i],1);
            //copy the attribute types.
            pos+=attrSize;
            sprintf(samp,"%d",schema->dataTypes[i]);
            pageData[pos]=*samp;
            pos+=k;
            d->attrType[i]=schema->dataTypes[i];
            //copy the attribute typelength.
            char str[30];
            sprintf(str,"%d",schema->typeLength[i]);
            pageData[pos]=*str;
            pos+=k;
            d->attrLength[i]=schema->typeLength[i];
        }
        //mark the end of string
        pageData[pos]='\0';
        //the file handler.
        SM_FileHandle fh;
        //Create the pageFile using the file handler and insert the data string into the page.
        check=createPageFile(name);
        if(check==RC_OK)
            return RC_OK;
        else
            return check;
        check=openPageFile(name,&fh);
        if(check==RC_OK)
            return RC_OK;
        else
            return check;
        check=writeBlock(0,&fh,pageData);
        if(check==RC_OK)
            return RC_OK;
        else
            return check;
        check=closePageFile(&fh);
        if(check==RC_OK)
            return RC_OK;
        else
            return check;
    }
    else
        return RC_ERROR;
    
}
//Open the table file from the disk and perform operations and set data in the RM_TableData structure;
extern RC openTable (RM_TableData *rel, char *name)
{
    SM_PageHandle ph;
    Schema *s=(Schema *)malloc(sizeof(Schema));
    rel->name=name;
    rel->mgmtData=d;
    s->numAttr=d->attrCount;
    int count=s->numAttr;
    s->attrNames=(char **)malloc(count * sizeof(char *));
    s->dataTypes=malloc(count * sizeof(DataType));
    s->typeLength=malloc(count * sizeof(int));
    int i,len,c=3;
    for(i=0; i<3; i++)
    {
        s->attrNames[i] = (char *)malloc(attrSize);
        char *temp = &d->attrName[i];
        strncpy(s->attrNames[i],temp,1);
        s->dataTypes[i] = d->attrType[i];
        s->typeLength[i] = d->attrLength[i];
    }
    rel->schema=s;
    return RC_OK;
}
//Close the table by flushing all the changes to the Disk.
extern RC closeTable (RM_TableData *rel)
{
    DB *db=rel->mgmtData;
    forceFlushPool(&db->bf);
    return RC_OK;
}
//Delete the table using the record manager functions.
extern RC deleteTable (char *name)
{
    destroyPageFile(name);
    return RC_OK;
}
//Returns the number of tuples in the table.
extern int getNumTuples (RM_TableData *rel)
{
    DB *db=(DB *)rel->mgmtData;
    return d->rowCount;
}
// ==== HANDLING RECORDS IN THE TABLE FUNCTIONS ==== //
//Insert record, inserts a record into the table taking data from the record structure.
extern RC insertRecord (RM_TableData *rel, Record *record)
{
    int free,pgno,Row,recrdSize,i;
    int loc=0,flag,rows;
    char *data;
    RID *recordID;
    char d[PAGE_SIZE];
    DB *dbm=rel->mgmtData;
    pgno=dbm->freeCount;
    Row=getNumTuples(rel);
    recrdSize=getRecordSize(rel->schema);
    rows=PAGE_SIZE/recrdSize;
    pinPage(&dbm->bf,&dbm->pg,pgno);
    recordID=&record->id;
    data=dbm->pg.data;
    recordID->page=pgno;
    free=dbm->rowCount;
    recordID->slot=free;
    flag=0;
    if(Row>rows) 
    {
        pgno++;
        dbm->freeCount++;
        dbm->rowCount=0;
        flag=1;
        unpinPage(&dbm->bf,&dbm->pg);
        recordID->slot=dbm->rowCount;
        recordID->page=pgno;
        pinPage(&dbm->bf,&dbm->pg,recordID->page);
        data=dbm->pg.data;
    }
    loc=recordID->slot*recrdSize;
    markDirty(&dbm->bf,&dbm->pg);
    for(i=0;i<recrdSize;i++) 
        data[loc++]=record->data[i];
    unpinPage(&dbm->bf,&dbm->pg);
    forcePage(&dbm->bf,&dbm->pg);
    dbm->rowCount++;
    return RC_OK;
}
//Update the record having ID in the RID and the corresponding table.
extern RC updateRecord (RM_TableData *rel, Record *record)
{
    char *p;
    DB *db=rel->mgmtData;
    RID rID=record->id;
    int recSize=getRecordSize(rel->schema);
    int totalSize=rID.slot*recSize;
    pinPage(&db->bf,&db->pg,record->id.page);
    db->pg.data+=totalSize;
    p=db->pg.data;
    memcpy(p,record->data,recSize);
    markDirty(&db->bf,&db->pg);
    unpinPage(&db->bf,&db->pg);
    forcePage(&db->bf,&db->pg);
    return RC_OK;
}
//Delete the record.
extern RC deleteRecord(RM_TableData *rel, RID id)
{
    int i,j;
    int recdSize;
    DB *db=rel->mgmtData;
    char *pageData=db->pg.data;
    recdSize=getRecordSize(rel->schema);
    pinPage(&db->bf,&db->pg,id.page);
    recdSize*=id.slot;
    for(i=0;i<recdSize;i++)
    {
        j=recdSize+i;
        pageData[j]='-';
    }
    markDirty(&db->bf,&db->pg);
    unpinPage(&db->bf,&db->pg);
    forcePage(&db->bf,&db->pg);
    return RC_OK;
}
//Retreives the record from the memory.
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    int i,j;
    int recdSize=getRecordSize(rel->schema);
    DB *db=(DB*)rel->mgmtData;
    pinPage(&db->bf,&db->pg,id.page);
    char *pageData=db->pg.data;
    for(i=0;i<recdSize;i++)
    {
        j=(id.slot*recdSize)+i;
        record->data[i]=pageData[j];
    }
    unpinPage(&db->bf,&db->pg);
    return RC_OK;
}
// ===== SCAN FUNCTIONS ====//
//initializes the scan and sets the condition into the mgmtData void Pointer and sets the table handler
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    scan->mgmtData = cond;
    scan->rel = rel;
    return RC_OK;
}
//Get the next record that matches the scan condition.
extern RC next (RM_ScanHandle *scan, Record *record)
{
    Expr *condition=(Expr *)scan->mgmtData;
    Schema *sch=scan->rel->schema;
    if(condition!=NULL)
    {
        int recSize,rows;
        char *pageData,*recordData;
        recSize=getRecordSize(sch);
        rows=d->rowCount;
        Value *recValue=(Value *) malloc(sizeof(Value));
        while(snCount<=rows) 
        {
            record->id.slot=snCount;
            record->id.page=1;
            recordData=record->data;
            pageData=d->pg.data;
            pageData+=recSize*snCount;
            snCount++;
            pinPage(&d->bf,&d->pg,1);
            memcpy(recordData,pageData,recSize);
            evalExpr(record,sch,condition,&recValue);
            if(recValue->v.boolV == TRUE) 
            {
                unpinPage(&d->bf,&d->pg);
                return RC_OK;
            }
        }
        snCount=0;
        return RC_RM_NO_MORE_TUPLES;
    }
    else
        return RC_ERROR;
}

//Close the scan.
extern RC closeScan (RM_ScanHandle *scan)
{
    return RC_OK;
}
// ====== SCHEMA FUNCTIONS =====//
//get the size of the record.
extern int getRecordSize (Schema *schema)
{
    Schema *sch=schema;
    int attrCount=sch->numAttr,i=0,recSize=0;
    while(i<attrCount) 
    {
        if(sch->dataTypes[i]==DT_INT)
            recSize+=sizeof(int);
        else if(sch->dataTypes[i]==DT_STRING)
            recSize+=sch->typeLength[i];
        else if(sch->dataTypes[i]==DT_FLOAT)
            recSize+=sizeof(float);
        else if(sch->dataTypes[i]==DT_BOOL)
            recSize+=sizeof(bool);
        i++;    
    }
    return recSize;
}
//create the schema from the parameters
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *sch=(Schema *)malloc(sizeof(Schema));
    sch->numAttr=numAttr;
    sch->attrNames=attrNames;
    sch->dataTypes=dataTypes;
    sch->keySize=keySize;
    sch->keyAttrs=keys;
    sch->typeLength=typeLength;
    return sch;
}
//Delete the schema and all the iterative mallocs.
extern RC freeSchema (Schema *schema)
{
    int i=0;
    Schema *s = schema;
    while(i<s->numAttr)
    {
        free(s->attrNames[i]);
        i++;
    }
    free(s->dataTypes);
    free(s->typeLength);
    free(s->keyAttrs);
    free(s);
    return RC_OK;
}
//==== DEALING WITH THE RECORD AND ATTRIBUTE VALUE FUNCTIONS ====//
//create the Record. The reocord is of the style (---------) for the size of the record.
extern RC createRecord (Record **record, Schema *schema)
{
    int i,recSize;
    Record *rec=(Record *)malloc(sizeof(Record));
    rec->id.page=-1;
    rec->id.slot=-1;
    recSize=getRecordSize(schema);
    rec->data=(char*)malloc(recSize);
    for(i=0;i<getRecordSize(schema);i++) 
        rec->data[i]='-';
    *record=rec;
    return RC_OK;
}
//Get the current offset for the attribute to get/set the attribute.
extern int getAttributeOffset(int attrNum, Schema *schema)
{
    int i=0,attrOffset=0;
    while(i<attrNum)
    {
        if(schema->dataTypes[i]==DT_INT)
            attrOffset+=sizeof(int);
        else if(schema->dataTypes[i]==DT_STRING)
            attrOffset+=schema->typeLength[i];
        else if(schema->dataTypes[i]==DT_FLOAT)
            attrOffset+=sizeof(float);
        else if(schema->dataTypes[i]==DT_BOOL)
            attrOffset+=sizeof(bool);
        i++;
    }
    return attrOffset;
}
//deallocate the memory allocated for the record.
extern RC freeRecord (Record *record)
{
    Record *r=record;
    free(r->data);
    free(r);
    return RC_OK;
}

//set the attribute for the record.
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    //get total number of attributes.
    int i,j,k,off=0,dataP=0;
    int ct=0;
    char str[30];
    off=getAttributeOffset(attrNum,schema);
    dataP+=off;
    if(schema->dataTypes[attrNum]==0)
    {
        sprintf(str,"%d", value->v.intV);
        while(value->v.intV!=0)
        {
            value->v.intV/=10;
            ++ct;
        }
        for(i=0;i<ct;i++)
            record->data[dataP+i] = str[i];
    }
    else if(schema->dataTypes[attrNum]==1)
    {
        i=strlen(value->v.stringV);
        j=dataP;
        for(k=0;k<i;k++)
            record->data[j+k]=value->v.stringV[k];
    }
    else if(schema->dataTypes[attrNum]==2)
    {
        sprintf(str,"%f", value->v.floatV);
        record->data[dataP] =*str;
    }
    else if(schema->dataTypes[attrNum]==3)
    {
        sprintf(str,"%d", value->v.boolV);
        record->data[dataP] =*str;
    }
    return RC_OK;
}
//get the attribute from the record.
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    Value *newVal=(Value *)malloc(sizeof(Value));
    int off=0,p=0,i,j,k,ct=0;
    char str[30];
    off=getAttributeOffset(attrNum,schema);
    p+=off;
    char *recData=record->data;
    recData+=off;
    if(schema->dataTypes[attrNum]==0)
    {
        for(i=0;i<4;i++) 
        {
            if((recData[i]-'0')>0) 
            {
                str[ct] = recData[i];
                ct++;
            }
            else
                break;
        }
        j=0;
        for(k=0;k<ct;k++)
        {
            j*=10;
            j+=(str[k] - '0');
        }
        newVal->v.intV=j;
        newVal->dt=0;
    }
    else if(schema->dataTypes[attrNum]==1) 
    {
        //copy the string from the record to the value data structure.
        newVal->v.stringV=(char *)malloc(4);
        newVal->dt = 1;
        strncpy(newVal->v.stringV,recData,4);
        newVal->v.stringV[4]='\0';
    }
    else if(schema->dataTypes[attrNum]==2) 
    {
        newVal->v.floatV=recData[p]-'0';
        newVal->dt=2;
    }
    else if(schema->dataTypes[attrNum]==3) 
    {
        newVal->v.boolV=recData[p]-'0';
        newVal->dt=3;
    }
    *value = newVal;
    return RC_OK;
}