#include <iostream>
#include <stdlib.h>
#include <memory.h>
#include <cmath>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

PageId gcurPage;
void HFPage::init(PageId pageNo)
{
    //cout << "A new Page is Initialized" << endl;
    slotCnt = 0;
    usedPtr = MAX_SPACE - DPFIXED;
    freeSpace = MAX_SPACE - DPFIXED + 4;
    prevPage = nextPage = INVALID_PAGE;
    curPage = pageNo;
    gcurPage = pageNo;
    //slot = new vector<slot_t>;
    //for(int erase = 0;;erase++)
        //data[erase] = '\n';
    //slot.push_back(slot_t());
    //slot[0].length = EMPTY_SLOT;
    //slot[0].offset = EMPTY_SLOT;
    
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;
   
    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
    if(prevPage == -1)
       return -1;
    else
       return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{

    prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
    
    if(nextPage == -1)
       return -1;
    else
       return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
     nextPage = pageNo;
}

// Current Page
/*IPageId page_no(){ 
     
     PageId c = gcurPage;
     return c;
}*/

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
    int sloti = 0;
    int emp_slot_fnd = 0;
    if(recLen > freeSpace){
       return DONE;
    }
    if(usedPtr != sizeof(slot)){
       for( ;sloti < slotCnt ; sloti++){
           if(slot[sloti].length == EMPTY_SLOT){
              emp_slot_fnd = 1;
              break;
           }
       }      
       if(emp_slot_fnd == 1) {
           char *datatemp = &data[usedPtr];
           memcpy(datatemp - recLen, recPtr, recLen);
           //slot.push_back(slot_t());
       	   slot[sloti].length = recLen;
       	   slot[sloti].offset = usedPtr-recLen;
           usedPtr -= recLen;
           freeSpace -= recLen;// + sizeof(slot_t);
           rid.pageNo = curPage;
           rid.slotNo = sloti;
           return OK;
       }
       else {  
           int fptemp = freeSpace; 
           char *datatemp = &data[usedPtr];
           memcpy(datatemp - recLen, recPtr, recLen);
           //memcpy(data + usedPtr - recLen, recPtr, recLen);
           /*if(slotCnt != 0){
              cout << "##### SZ #####" << sizeof(data)<< endl;
              memcpy(data + (slotCnt-1)*4, slot, 4);
              cout << "2" << endl;
           }*/
           memcpy(data + slotCnt*4, slot, 4);
           //slot.push_back(slot_t());
       	   slot[slotCnt].length = recLen;
       	   slot[slotCnt].offset = usedPtr-recLen;
           usedPtr -= recLen;
           fptemp = fptemp - (recLen + sizeof(slot_t));
           /*double *dp = (double*) recPtr;
           char * r = &data[usedPtr];
           double *pp = (double*) r;
           cout << " ****** RecLen **** " << slotCnt << " **** RecLen *** " << slot[slotCnt].length << " **** RecVal **** "<< *dp << " *** stor Val *** " << *pp;*/
           
           slotCnt++; 
           rid.pageNo = curPage;
           rid.slotNo = slotCnt - 1;
           freeSpace = fptemp;
           return OK;
       }
    }else{
       
       return DONE;
    }
    
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
    int temp;
    if(rid.slotNo < 0 || rid.slotNo >= slotCnt)
       return DONE;
    memmove( data+ usedPtr + slot[rid.slotNo].length, data + usedPtr, slot[rid.slotNo].offset - usedPtr); 
    temp = usedPtr;
    usedPtr += slot[rid.slotNo].length;
    if(temp - slot[rid.slotNo].offset != 0){
       for(int i = slotCnt -1; i > rid.slotNo ; i--){
           slot[i].offset = slot[i].offset + slot[rid.slotNo].length;
           //cout << i << " Offset " << slot[i].offset << " length " << slot[i].length << endl;
       }
    }
    //cout<<"  page $$$$$ "<< page_no() << endl;
    freeSpace += slot[rid.slotNo].length;
    slot[rid.slotNo].offset = slot[rid.slotNo].length = EMPTY_SLOT;
    
    for (int i = slotCnt -1; i > -1; i--){
         if(slot[i].length == EMPTY_SLOT){
            slotCnt--;
            freeSpace += sizeof(slot_t);
	 }
         else
            break;
    }
    return OK;


}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    
    if (slotCnt == 0) 
        return DONE;
	

    for (int i = 0; i < slotCnt; i++) {
	if (slot[i].offset != EMPTY_SLOT){// && slot[i].length != EMPTY_SLOT && slot[i].length > 0 && slot[i].length < 1001) {
            //cout << " Slot Valid ????  " << slot[i].length << " offset  " << slot[i].offset << endl; 
	    firstRid.pageNo = curPage;
	    firstRid.slotNo = i;
            //cout << " RID being passed out " << firstRid.pageNo << "   " << firstRid.slotNo << endl;
            return OK;
	}
    }
    return DONE;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
    
    if(slotCnt == 0 || curRid.slotNo < 0)
       return FAIL;
    int i = curRid.slotNo + 1;
    
    //cout << " In HFPAGE " << i << " " << slotCnt << endl; 
    for (; i < slotCnt; i++) { 
	if (slot[i].length != EMPTY_SLOT) {
	    nextRid.pageNo = curPage;
	    nextRid.slotNo = i;
            return OK;
	}
    }
    
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    //cout<<"slot rid.slotno.offset  "<<slot[rid.slotNo].offset<<"   length  "<<slot[rid.slotNo].length<<endl;
    memcpy(recPtr, data + slot[rid.slotNo].offset,slot[rid.slotNo].length);
    //cout << " Error here sz 5 " << endl;
    recLen = slot[rid.slotNo].length;
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
    //char * datatemp = &data[0];
    recPtr = &data[slot[rid.slotNo].offset];
    recLen = slot[rid.slotNo].length;
    /*char * r = &data[slot[rid.slotNo].offset];
    double *pp = (double*) r;
    cout << " #### val here ### " << *pp << endl;*/
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    if(slotCnt == 0)
       return freeSpace - 4;
    else
       return freeSpace;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    for(int i = slotCnt-1; i > -1; i--){
        if (slot[i].length != EMPTY_SLOT)
            return false;
    }
    return true;
}



