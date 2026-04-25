#include "heapfile.h"
#include "error.h"

/**
 * Initializes a new heap file on disk.
 * Sets up the physical file structure by allocating a header page
 * and initial data page.
 * 
 * @param fileName The name of the file to create.
 * @return OK on success, FILEEXISTS if the file already exists, or
 * relevant error codes from the Buffer Manager or Database.
 */
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // Try to open the file. This should return an error.
    status = db.openFile(fileName, file);
    if (status != OK) // File does not exists
    {
        // Create and open the file on disk
		status = db.createFile(fileName);
        if (status != OK) return status;
        status = db.openFile(fileName, file);
        if (status != OK) return status;

        // Initialize the header page
		status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK) return status;
        hdrPage = (FileHdrPage*) newPage;
        memset(hdrPage->fileName, 0, MAXNAMESIZE); // Ensure hdrPage->fileName is blank
        strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE-1); // Fill in fileName
        hdrPage->pageCnt = 0;
        hdrPage->recCnt = 0;

        // Allocate an initial data page
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) return status;
        newPage->init(newPageNo);

        // Set header page to recognize initial data page
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1; // Now has one data page

        // Unpin opened pages
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK) return status;
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK) return status;

        db.closeFile(file);
		return (OK);
    }

    db.closeFile(file);
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

/**
 * Opens an existing heap file and initializes the in-memory object.
 * Maps the physical file to the HeapFile object.
 * 
 * @param fileName     The name of the file to open.
 * @param returnStatus [o] Returns OK on success, 
 * relevant error codes from the Buffer Manager or Database.
 */
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status  status;
    Page* pagePtr;

    headerPageNo = 1; // Header page is always at page number 1

    // Open the header page
    if ((status = db.openFile(fileName, filePtr)) != OK) {
        returnStatus = status;
        return;
    }
    status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
    if (status != OK) {
        returnStatus = status;
        return;
    }
    headerPage = (FileHdrPage*) pagePtr;
    hdrDirtyFlag = false;

    // Get the first data page
    curPageNo = headerPage->firstPage;
    curPage = NULL; //  Default state
    if (curPageNo != -1) {
        // If a first page exists, set curPage to that
        status = bufMgr->readPage(filePtr, curPageNo, pagePtr);
        if (status != OK) {
            returnStatus = status;
            return;
        }
        curPage = pagePtr;
    }

    // Bookkeeping
    curDirtyFlag = false;
    curRec = NULLRID;

    returnStatus = OK;
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

/**
 * Get the record with the requested rid from the database.
 * 
 * @param rid The rid of the record to retrieve
 * @param rec [o] Returns the retrieved record
 * @return OK if successful or relevant error 
 * codes from the Buffer Manager.
 */
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // Get the correct page if the correct page isn't already open
    if (curPage == NULL || rid.pageNo != curPageNo) {
        // Unset the open page if there is one
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
            
            curPage = NULL;
            curPageNo = -1;
        }

        // Read in the correct page
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) {
            curPage = NULL;
            curPageNo = -1;
            return status;
        }
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
    }

    // Get the record from the page
    status = curPage->getRecord(rid, rec);
    if (status != OK) return status;
    curRec = rid;

    return status;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        ((type_ == INTEGER && length_ != sizeof(int))
         || (type_ == FLOAT && length_ != sizeof(float))) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}

/**
 * Scan through the pages until the next record that matches
 * the filter is found.
 * 
 * @param outRid [o] Returns the rid of the next record 
 *                   that passes the filter.
 * @return OK if successful, or relevant error codes 
 * from the Buffer Manager.
 */
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status status = OK;
    RID nextRid;
    Record rec;

    // If no page is pinned, start at the first page
    if (curPage == NULL) {
        curPageNo = headerPage->firstPage;
        if (curPageNo == -1) return FILEEOF;

        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;

        curDirtyFlag = false;
        curRec = NULLRID;
    }

    // Loop through pages until a record is found or there are no more pages
    while (true)
    {
        if (curRec.pageNo == -1) { // No current record
            // Start at the page's first record
            status = curPage->firstRecord(nextRid);
        } else { // Yes current record
            // Go to the next record
            status = curPage->nextRecord(curRec, nextRid);
        }

        // There is a current record loaded in
        if (status == OK) {
            curRec = nextRid; 
            status = curPage->getRecord(curRec, rec);
            if (status != OK) return status;

            // Return record if it is a match
            if (matchRec(rec))
            {
                outRid = curRec;
                return OK;
            }
        }
        // No more records, end of current page
        else {
            // Get the next page if there is one
            int nextPageNo;
            status = curPage->getNextPage(nextPageNo);
            if (status != OK) return status;
            if (nextPageNo == -1) {
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                curPage = NULL;
                curPageNo = -1;
                return FILEEOF;
            }

            // Unpin the old page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;

            // Go to the next page
            curPageNo = nextPageNo;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK) {
                curPage = NULL;
                return status;
            }
            
            // Bookkeeping
            curDirtyFlag = false;
            curRec = NULLRID;
        }
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

/**
 * Inserts a new record into the heap file.
 * 
 * @param rec The record to insert.
 * @param outRid [o] The RID of the new record.
 * @return OK on success, INVALIDRECLEN if the record is
 * too large, or relevant error codes from the Buffer
 * Manager.
 */
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // Go to the last page if it isn't already there
    if (curPage == NULL || curPageNo != headerPage->lastPage)
    {
        if (curPage != NULL) {
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        }
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;
    }

    status = curPage->insertRecord(rec, outRid);

    if (status == NOSPACE)
    {
        // Make a new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) return status;

        // Initialize the new page as a HeapPage
        newPage->init(newPageNo);
        
        // Link the old last page to this new page
        curPage->setNextPage(newPageNo);
        
        // Mark the old page as dirty
        curDirtyFlag = true; 
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;

        // Update Bookkeeping to point to the new page
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = true;
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        // Insert the record into the brand new page
        status = curPage->insertRecord(rec, outRid);
    }

    if (status == OK)
    {
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
        curRec = outRid;
    }

    return status;
}


