/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"

/*file_deleted : 0 -> no data page, no dir page deleted
               1 -> data page deleted but dir page not deleted
               2 -> last directory page deleted
               3 -> first directory page deleted
               4 -> All directory pages deleted */


// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
Scan::Scan (HeapFile *hf, Status& status)
{
  status = init(hf);   
  if(status != OK)
     cout << "Scan class initializing failed" << endl;

}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{
    Status reset_s = reset();
    if(reset_s != OK)
       cout << "Scan class deletion failed" << endl;
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{
  Status check;
  RID nextRid;
  
  if(nxtUserStatus == OK){
     
     dataPage->getRecord(userRid,recPtr,recLen);
     //cout << " User Rid is " << userRid.slotNo << "    " << userRid.pageNo << endl;
     nxtUserStatus =  dataPage->nextRecord(userRid, nextRid);
     rid = userRid;
     if(nxtUserStatus == OK){
        userRid = nextRid;
     }   
  }
  else{
      Status ndps = nextDataPage();
      if(ndps == OK){
        
	dataPage->getRecord(userRid,recPtr,recLen);
	nxtUserStatus =  dataPage->nextRecord(userRid, nextRid);
        rid = userRid;         
        //cout << "Rid being passed out " << rid.slotNo << "   " << rid.pageNo << endl;
         if(nxtUserStatus == OK){
            
            userRid = nextRid;
         }  
      }
      else{
         cout << " Kitni baar ghusa " << endl;
         scanIsDone = 1;
         if(_hf->file_deleted < 2 ) {
            check = MINIBASE_BM->unpinPage(dirPageId);
            if(check != OK)
               cout << " Error Scan 1" << endl; 
         }
         //cout << "data pa ge rid in get next of scan " << dataPageId << endl;
         if(_hf->file_deleted == 0 ) {
            check = MINIBASE_BM->unpinPage(dataPageId); 
            if(check != OK)
               cout << " Error Scan 2" << endl; 
         }
         //reset();
         //minibase_errors.clear_errors();
         return DONE;
      }
  }
  
  return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
  _hf = hf;
  //cout << " Code in initialization " << endl;
  Page * pagedir;
  dirPageId = _hf->firstDirPageId;
  Status check = MINIBASE_BM->pinPage(dirPageId, pagedir);//,true,fileName);// dir page

  if(check == OK){ 
     HFPage * curDirP = (HFPage*)pagedir;
     dirPage = curDirP;
     //cout << " current directory page ***********   " << dirPageId << endl;
     firstDataPage();
     scanIsDone = 0;
  }
  else {
     cout << " Error in Init " << endl;
  }

  return OK;
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
  Status s;
  if(scanIsDone == 0){
     MINIBASE_BM->unpinPage(dirPageId); 
     if(s != OK)
        cout << " Error in Scan : : reset 1 " << endl;
     MINIBASE_BM->unpinPage(dataPageId); 
     if(s != OK)
        cout << " Error in Scan : : reset 2 " << endl;
  }

  dataPage = NULL;
  dirPage = NULL;   
  dataPageId = 0;
  dirPageId = 0;
  nxtUserStatus = 0;
  scanIsDone = 0;
  userRid.slotNo = 0;
  userRid.pageNo = 0;
  dataPageRid.slotNo = 0;
  dataPageRid.pageNo = 0;
  
  return OK;
}

// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()
{
  Status s;
  RID rid, tempRid;
  RID nextRid;
  int dpinfolen;
  HFPage *curDP;
  DataPageInfo *dpinfo = new DataPageInfo;
  char * dpinfobuf = new char;
  Status recordIDs = dirPage->firstRecord(rid);
  cout<<"value of rid inside scan::first data page  :  "<<rid.pageNo<<" and slot no : "<<rid.slotNo<<endl;
  dataPageRid = rid;
  //cout << "Data Page Info Rid **********************  " << dataPageRid.slotNo << "      " <<dataPageRid.pageNo << endl;
  if(recordIDs == OK){
     recordIDs = dirPage->returnRecord(rid, dpinfobuf, dpinfolen);
     //cout << " Succes open data page *********** " << recordIDs << endl;
     dpinfo = (DataPageInfo *)dpinfobuf;
     s = MINIBASE_BM->pinPage(dpinfo->pageId, (Page *&)curDP);
     if(s != OK)
        cout << " Error in Scan : : firstData Page 1 " << endl;
     dataPage = curDP;
     dataPageId = dpinfo->pageId;
     //cout << "Data Page ID **********************  " << dataPageId << endl;
     recordIDs = dataPage->firstRecord(tempRid);
     userRid = tempRid;
     nxtUserStatus = dataPage->nextRecord(userRid, nextRid);
  }
  return OK;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){
  
  Status s;
  int dpinfolen ;
  char * dpinfobuf = new char;
  HFPage *curDP;
  RID nextRid, rid;
  DataPageInfo *dpinfo = new DataPageInfo;
  Status nextDPexist = dirPage->nextRecord (dataPageRid, nextRid);
  if( nextDPexist == OK){
       
        if(_hf->file_deleted == 0 ) {
           s = MINIBASE_BM->unpinPage(dataPageId); 
           if(s != OK){
              cout << " Error in Scan : : nextData Page 1 " << endl;
              return DONE;
           }
        }
  	dataPageRid = nextRid;
        dirPage->getRecord(dataPageRid, dpinfobuf, dpinfolen);
        dpinfo = (DataPageInfo *)dpinfobuf;
        dataPageId = dpinfo->pageId;
        
  	s = MINIBASE_BM->pinPage(dataPageId, (Page *&)curDP);
        if(s != OK)
           cout << " Error in Scan : : nextData Page 2 " << endl;
  	dataPage = curDP;
        dataPage->firstRecord(userRid);
     	nxtUserStatus = dataPage->nextRecord(userRid, nextRid);
  	return OK;
  }
  else {
        Status nxtDirPageExist = nextDirPage();
        if(nxtDirPageExist == DONE)
           return DONE;
        else{
           s = MINIBASE_BM->unpinPage(dataPageId); 
           if(s != OK)
              cout << " Error in Scan : : nextData Page 3 " << endl;
           dirPage->firstRecord(rid);
           dataPageRid = rid;
           dirPage->returnRecord(rid, dpinfobuf, dpinfolen);
     	   
     	   dpinfo = (DataPageInfo *)dpinfobuf;
           s = MINIBASE_BM->pinPage(dpinfo->pageId, (Page *&)curDP);
           if(s != OK)
               cout << " Error in Scan : : nextData Page 4 " << endl;
     	   dataPage = curDP;
     	   dataPageId = dpinfo->pageId;
     	   dataPage->firstRecord(userRid);
     	   nxtUserStatus = dataPage->nextRecord(userRid, nextRid);
           return OK;
        }
  }
  
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {
  
   Status s;
   PageId nxtdirPage;
   HFPage * curDirP;
   nxtdirPage = dirPage->getNextPage();
   if(nxtdirPage == INVALID_PAGE){
      scanIsDone = 1;
      return DONE;
   }
   else{
      cout << " Directory shift happened " << endl;
      s = MINIBASE_BM->unpinPage(dirPageId); 
      if(s != OK)
         cout << " Error in Scan : : nextDir Page1 " << endl;
      dirPageId = nxtdirPage;
      s = MINIBASE_BM->pinPage(dirPageId, (Page *&)curDirP);
      if(s != OK)
         cout << " Error in Scan : : nextDir Page2 " << endl;
      dirPage = curDirP;
   }

   return OK;
}
