#include "catalog.h"


RelCatalog::RelCatalog(Status &status) :
	 HeapFile(RELCATNAME, status)
{
// nothing should be needed here
}


const Status RelCatalog::getInfo(const string & relation, RelDesc &record)
{
  if (relation.empty())
    return BADCATPARM;

  Status status;
  Record rec;
  RID rid;




}


const Status RelCatalog::addInfo(RelDesc & record)
{
  RID rid;
  InsertFileScan*  ifs;
  Status status;




}

const Status RelCatalog::removeInfo(const string & relation)
{
  Status status;
  RID rid;
  HeapFileScan*  hfs;

  if (relation.empty()) return BADCATPARM;



}


RelCatalog::~RelCatalog()
{
// nothing should be needed here
}


AttrCatalog::AttrCatalog(Status &status) :
	 HeapFile(ATTRCATNAME, status)
{
// nothing should be needed here
}


const Status AttrCatalog::getInfo(const string & relation, 
				  const string & attrName,
				  AttrDesc &record)
{  
  Status status;
  RID rid = NULLRID;
  Record rec;
  HeapFileScan*  hfs = NULL;
  AttrDesc *tmp;
  const char *crelation = relation.c_str();
  const char *cattriname = attrName.c_str();
  
  if (relation.empty() || attrName.empty()) return BADCATPARM;
  hfs = new HeapFileScan(ATTRCATNAME, status); //new heapfile scan obj
  if (status != OK) return status;
  status = hfs->startScan(0, strlen(crelation), STRING, crelation, EQ);
  if (status != OK) return status;
  while((status = hfs->scanNext(rid)) != FILEEOF){
    status = hfs->getRecord(rec);
    if(status != OK) return status;
    tmp = (AttrDesc*)rec.data;
    if(strncmp(tmp->attrName, cattriname, strlen(cattriname)) == 0){
      //matching relation and matching attrName
      record = *tmp;
      hfs->endScan();
      delete hfs;
      hfs = NULL;
      return OK;
    }
  }
  //if out of the while loop; 
  //2 cases: 1. we have found the right relation but no right attrname
  //         2. we have not found the right relation
  if(rid.pageNo == -1 || rid.slotNo == -1) return RELNOTFOUND;
  hfs->endScan();
  delete hfs;
  hfs = NULL;
  return ATTRNOTFOUND;
}


const Status AttrCatalog::addInfo(AttrDesc & record)
{
  RID rid;
  InsertFileScan*  ifs;
  Status status;
  AttrDesc tmp;
  const string relation(record.relName);
  const string attrName(record.attrName);
   //check for duplicates
  status = getInfo(relation, attrName, tmp);
  if(status == OK) return DUPLATTR;
  Record toAdd;
  toAdd.data = &record;
  toAdd.length = sizeof(record);
  ifs = new InsertFileScan(ATTRCATNAME, status);
  if(status != OK) return status;
  status = ifs->insertRecord(toAdd, rid);
  delete ifs;
  ifs = NULL;
  return status;
}


const Status AttrCatalog::removeInfo(const string & relation, 
			       const string & attrName)
{
  Status status;
  Record rec;
  RID rid;
  AttrDesc record;
  HeapFileScan*  hfs;
  AttrDesc *tmp;
  const char *crelation = relation.c_str();
  const char *cattriname = attrName.c_str();
  if (relation.empty() || attrName.empty()) return BADCATPARM;
  hfs = new HeapFileScan(ATTRCATNAME, status); //new heapfile scan obj
  if (status != OK) return status;
  status = hfs->startScan(0, strlen(crelation), STRING, crelation, EQ);
  if (status != OK) return status;
  while((status = hfs->scanNext(rid)) != FILEEOF){
    status = hfs->getRecord(rec);
    if(status != OK) return status;
    tmp = (AttrDesc*)rec.data;
    if(strncmp(tmp->attrName, cattriname, strlen(cattriname)) == 0){
      //matching relation and matching attrName
      hfs->deleteRecord();
      hfs->endScan();
      delete hfs;
      hfs = NULL;
      return OK;
    }
  }
  hfs->endScan();
  delete hfs;
  hfs = NULL;
  return ATTRNOTFOUND;
}


const Status AttrCatalog::getRelInfo(const string & relation, 
				     int &attrCnt,
				     AttrDesc *&attrs)
{
  Status status;
  RID rid = NULLRID; 
  Record rec;
  HeapFileScan*  hfs;
  int i = 0;
  int maxCnt = -1;
  const char *crelation = relation.c_str();
  if (relation.empty()) return BADCATPARM;
  hfs = new HeapFileScan(ATTRCATNAME, status); //new heapfile scan obj
  if(status != OK) return status;
  maxCnt = hfs->getRecCnt();
  attrs = new AttrDesc[maxCnt];
  status = hfs->startScan(0, strlen(crelation), STRING, crelation, EQ);
  if (status != OK) return status;
  while((status = hfs->scanNext(rid)) != FILEEOF){
    status = hfs->getRecord(rec);
    if(status != OK) return status;
    attrs[i] = *((AttrDesc*)(rec.data));
    i++;
  }
  if(rid.pageNo == -1 || rid.slotNo == -1) return RELNOTFOUND;
  attrCnt = i;
  hfs->endScan();
  delete hfs;
  hfs = NULL;
  return OK;
}


AttrCatalog::~AttrCatalog()
{
// nothing should be needed here
}

