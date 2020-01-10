#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>

SM_FileHandle recMgr;
int curRec = 0;
int tomb[50000];
// table and manager
RC initRecordManager (void *mgmtData)
{
	int i=0;
	while(i<10000)
	{
		tomb[i]=tomb[i]-99;
		i++;
	}
    return RC_OK;
}

RC shutdownRecordManager ()
{
    return RC_OK;
}

RC createTable (char *name, Schema *schema)
{
    SM_PageHandle ph;    
    ph=(SM_PageHandle)malloc(PAGE_SIZE);
    createPageFile(name);                      //Create a new file with file name as table name.
    openPageFile(name,&recMgr);           //Open the file.
    memset(ph,'\0',PAGE_SIZE);
    strcpy(ph,serializeSchema(schema)); // changed
    writeBlock(0,&recMgr,ph);
    memset(ph,'\0',PAGE_SIZE);
    writeBlock(1,&recMgr,ph);
    free(ph);
    return RC_OK;
}

RC openTable (RM_TableData *rel, char *name)
{
	SM_PageHandle ph;
	rel->name=(char *)malloc(sizeof(char)*100);
    rel->schema=(Schema *)malloc(sizeof(Schema));//this will be deserialized schema.    
    ph=(SM_PageHandle)malloc(PAGE_SIZE);
    if(recMgr.fileName==NULL)
    	openPageFile(rel->name,&recMgr);
    strcpy(rel->name,name);
    readBlock(0,&recMgr,ph);
    deSerializeSchema(ph,rel->schema);//make changes
    rel->mgmtData=NULL; //not sure what's required here.
    free(ph);
    if(rel->schema!=NULL)
	{
		if(rel!=NULL)
        	return RC_OK;
	}
    else
    	return RC_OPEN_TABLE_FAILED;
    closePageFile(&recMgr);
    return RC_OK;
}
RC closeTable (RM_TableData *rel)
{
	int i=0,j;
	char freepg[PAGE_SIZE];
	memset(freepg,'\0',PAGE_SIZE);
	SM_PageHandle ph = (SM_PageHandle)malloc(PAGE_SIZE);
	readBlock(1,&recMgr,ph);
	strcpy(freepg,ph);
	char FreePageNumber[10];
	memset(FreePageNumber,'\0',10);
	char nullString[PAGE_SIZE];
	memset(nullString,'\0',PAGE_SIZE);
	while(tomb[i]!=-99)
	{
		sprintf(FreePageNumber,"%d",(tomb[i]));
		strcat(freepg,FreePageNumber);
		strcat(freepg,";");
		writeBlock(1,&recMgr,freepg);// update FSM block
		j=tomb[i]+2;
		writeBlock(j,&recMgr,nullString);//write deleted block
		memset(FreePageNumber,'\0',10);
	}
    closePageFile(&recMgr);
    free(rel->schema);
    free(rel->name);
    return RC_OK;
}

RC deleteTable (char *name)
{
    destroyPageFile(name);
    return RC_OK;
}

int getNumTuples (RM_TableData *rel)
{
	SM_PageHandle ph;
    int count=0,j=0;
    ph=(SM_PageHandle)malloc(PAGE_SIZE);
    readBlock(1,&recMgr,ph);
    while(ph[j]!=NULL)
	{
        if(ph[j]==';')
            count++;
		j++;
    }
    closePageFile(&recMgr);
    openPageFile(rel->name,&recMgr);
	count=recMgr.totalNumPages-count;
    return count;
}
RC insertRecord (RM_TableData *rel, Record *record)
{
	char frstno[10];
	int rID,strSize,i,j=0;
	char freepg[PAGE_SIZE];
	SM_PageHandle ph;
	char *freeList,*str;
	bool freepgcheck=false;
	if(recMgr.fileName==NULL)
		openPageFile(rel->name,&recMgr);
	strSize=sizeof(char)*PAGE_SIZE;
    str=(char *)malloc(strSize);
	memset(str, '\0',PAGE_SIZE);
    strcpy(str,record->data);
	memset(freepg,'\0',PAGE_SIZE);
	ph=(SM_PageHandle)malloc(PAGE_SIZE);
	readBlock(1,&recMgr,ph);
	strcpy(freepg,ph); //read the 1st block content to fpl here
	free(ph);
	if(freepg != NULL)
	{
		for(i=0;i<strlen(freepg);i++)
		{
			if(freepg[i]==';')
			{
				freepgcheck=true;
				break;
			}
		}
	}
	if(freepgcheck==false)
	{
		writeBlock(recMgr.totalNumPages,&recMgr,str);
		rID = recMgr.totalNumPages - 3;
	}
	else
	{
		memset(frstno,'\0',10);
		i=0;
		while(freepg[i]!=';')
		{
			frstno[i]=freepg[i];
			i++;
		}
		writeBlock(atoi(frstno) + 2,&recMgr,str);
		recMgr.totalNumPages--; //because we replacing and not adding new blk
		freeList=(SM_PageHandle)malloc(PAGE_SIZE);;
		memset(freeList,'\0',PAGE_SIZE);
		i=strlen(frstno)+1;
		while(i<strlen(freepg))
		{
			freeList[j++]=freepg[i];
			i++;
		}
		writeBlock(1,&recMgr,freeList);
		free(freeList);
		rID=atoi(frstno);
	}
	record->id.page=rID;
	record->id.slot=-99; //useless because we are storing 1 rec / page.
    free(str);
	return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id)
{
	int i=0;
	while(tomb[i]!=-99)
		i++;
	tomb[i]=id.page;
    return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record)
{
	int write=record->id.page + 2;
	openPageFile(rel->name, &recMgr);
	writeBlock(write,&recMgr,record->data);
	closePageFile(&recMgr);
    return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	int write=id.page + 2;
    SM_PageHandle ph;
    SM_FileHandle fh;
    openPageFile(rel->name, &fh);
    ph=(SM_PageHandle) malloc(PAGE_SIZE);
    memset(ph,'\0',PAGE_SIZE);
    readBlock(write, &fh, ph);
    record->id.page=id.page;
    strcpy(record->data, ph);
    closePageFile(&fh);
    free(ph);
    return RC_OK;
}

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	scan->mgmtData=cond;
	scan->rel=rel;
    return RC_OK;
}

void getColumnData(int columnNum,char *record,int *pos,char *c)
{
	int ct;
	int i,j=0;
	if(columnNum!=0)
		ct=pos[columnNum-1]+1;
	else
		ct=0;
	for(i=ct;i<pos[columnNum];i++)
		c[j++]=record[i];
}

void storeSemiColonPostion(char * record,int *p)
{
	int i=0,j=0;
	while(i<strlen(record))
	{
		if(record[i]==';')
			p[j++]=i;
		i++;
	}
}

RC next (RM_ScanHandle *scan, Record *record)
{
	char cellValue[PAGE_SIZE];
	int sc_pos[3];
	if(recMgr.fileName == NULL)
		openPageFile(scan->rel->name,&recMgr);
	Expr *e = (Expr *) scan->mgmtData;
	while(curRec < recMgr.totalNumPages - 2)
	{
		SM_PageHandle ph = (SM_PageHandle) malloc(PAGE_SIZE);
		memset(ph,'\0',PAGE_SIZE);
		readBlock(curRec+2,&recMgr,ph);
		memset(cellValue,'\0',PAGE_SIZE);
		storeSemiColonPostion(ph,sc_pos);
		if(e->expr.op->type == OP_COMP_EQUAL)
		{
			getColumnData(e->expr.op->args[1]->expr.attrRef,ph,sc_pos,cellValue);
			if(e->expr.op->args[0]->expr.cons->dt == DT_INT)
			{
				if(atoi(cellValue) == e->expr.op->args[0]->expr.cons->v.intV)
				{
					strcpy(record->data,ph);
					curRec++;
					free(ph);
					return RC_OK;
				}
			}
			else if(e->expr.op->args[0]->expr.cons->dt == DT_STRING)
			{
				if(strcmp(cellValue,e->expr.op->args[0]->expr.cons->v.stringV) == 0)
				{
					strcpy(record->data,ph);
					curRec++;
					free(ph);
					return RC_OK;
				}
			}
		}
		else if(e->expr.op->type == OP_BOOL_NOT)
		{
			getColumnData(e->expr.op->args[0]->expr.op->args[0]->expr.attrRef,ph,sc_pos,cellValue);
			if(e->expr.op->args[0]->expr.op[0].args[1]->expr.cons->v.intV <= atoi(cellValue))
			{
				strcpy(record->data,ph);
				free(ph);
				curRec++;
				return RC_OK;
			}
		}
		curRec++;
		free(ph);
	}
	curRec=0; 
	return RC_RM_NO_MORE_TUPLES;
}

RC closeScan (RM_ScanHandle *scan)
{
    return RC_OK;
}

int getRecordSize (Schema *schema)
{
    int size=0,i=0;
    while(i<schema->numAttr) 
	{
        if(schema->dataTypes[i]==DT_INT)
            size+=sizeof(int);
        if(schema->dataTypes[i]==DT_FLOAT)
            size+=sizeof(float);
        if(schema->dataTypes[i]==DT_BOOL)
            size += sizeof(bool);
		if(schema->dataTypes[i]==DT_STRING)
			size += schema->typeLength[i];
		i++;
    }
    size=2*2*2;
    return size;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	Schema *schema = (Schema *)malloc(sizeof(Schema));
	schema->numAttr = numAttr;
	schema->typeLength = typeLength;
	schema->keyAttrs = keys;
	schema->keySize = keySize;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	return schema;
}

RC freeSchema (Schema *schema)
{
    free(schema);    
    return RC_OK;
}

RC createRecord (Record **record, Schema *schema)
{
	*record=(Record *)malloc(sizeof(Record));
	 (*record)->data=(char *)malloc(PAGE_SIZE);
	 memset((*record)->data,'\0',PAGE_SIZE);
	 if(record!=NULL)
		 return RC_OK;
	 else
		 return RC_CREATE_FAILED;
}

RC freeRecord (Record *record)
{
    free(record);
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    char temp[PAGE_SIZE + 1];
    int tcount = 1,dtcount = 0,sizeVal,i;
	sizeVal=sizeof(Value)*schema->numAttr;
    memset(temp,'\0',PAGE_SIZE + 1);
    *value=(Value *)malloc(sizeVal);
    for(i=0;i<PAGE_SIZE;i++)
    {
        if((record->data[i]==';')||(record->data[i]=='\0'))
        {
            if (attrNum == dtcount) 
			{
                if(schema->dataTypes[dtcount]==DT_INT) 
                    temp[0]='i';
                else if(schema->dataTypes[dtcount]==DT_FLOAT)    
                    temp[0]='f';
                else if(schema->dataTypes[dtcount]==DT_BOOL)
					temp[0]='b';
                else if(schema->dataTypes[dtcount]==DT_STRING)
                    temp[0]='s';
            }
		    *value=stringToValue(temp);
			break;
            }
            dtcount++;
            tcount=1;
            memset(temp,'\0',PAGE_SIZE + 1);
        }
        else
            temp[tcount++] = record->data[i];
    }
    return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	 char *colVal=serializeValue(value);
	 int sc_count=0,i,j=0,k;
	 int sc_pos[schema->numAttr];
	 char str[PAGE_SIZE],fstr[PAGE_SIZE],lstr[PAGE_SIZE];
	 for(i=0;i<strlen(record->data);i++)
		 if(record->data[i]==';')
			 sc_count++;
	 if(sc_count==schema->numAttr)
	 {
		 for(i=0;i<strlen(record->data);i++)
			 if(record->data[i]==';')
				 sc_pos[j++] = i;
		 if(attrNum == 0)
		 {
			 memset(str,'\0',PAGE_SIZE);
			 i=0;
			 for(k=sc_pos[attrNum];k<strlen(record->data);k++)
				 str[k++] = record->data[k];
			 str[i]='\0';//here
			 memset(record->data,'\0',PAGE_SIZE);
			 strcpy(record->data,colVal);
			 strcpy(record->data,str);
		 }
		 else
		 {
			 j=0;
			 memset(fstr,'\0',PAGE_SIZE);
			 memset(lstr,'\0',PAGE_SIZE);
			 for(i=0;i<=sc_pos[attrNum-1];i++)
				 fstr[i]=record->data[i];
			 fstr[i]='\0';
			 for(i=sc_pos[attrNum];i<strlen(record->data);i++)
				 lstr[j++]=record->data[i];
			 lstr[j]='\0';
			 strcat(fstr,colVal);
			 strcat(fstr,lstr);
			 memset(record->data,'\0',PAGE_SIZE);
			 strcpy(record->data,fstr);
		 }
	 }
	 else
	 {
		 char sc=";";
		 strcat(record->data,colVal);
		 strcat(record->data,sc);
	 }
	 if(colVal!=NULL)
		 return RC_OK;
	 else
		 return RC_SET_ATTR_FAILED;
}

