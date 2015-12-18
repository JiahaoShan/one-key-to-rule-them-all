/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	this->bufMgr = bufMgrIn;
	this->lowValString = (char*)malloc(sizeof(char) * 10);
	this->highValString = (char*)malloc(sizeof(char) * 10);

	// set scan parameters
	this->scanExecuting = false;

	// var declaration
	std::string indexName;						// index file name
	std::ostringstream idxStr;					// string stream that is used to form index file name

	// get the index file name
	idxStr<<relationName<<'.'<<attrByteOffset;
	indexName = idxStr.str();

	// first create the BlobFile object, if the actual file doesn't exist, create one
	try
	{
		this->file = new BlobFile(indexName, true);
		constructNew(relationName, attrByteOffset, attrType);
	}
	catch (FileExistsException e)
	{
		this->file = new BlobFile(indexName, false);
		constructFromExist(relationName, attrByteOffset, attrType);
		
	}
	
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	try
	{
		this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
	}catch(PageNotPinnedException e)
	{
	}

	try
	{
		this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
	}catch(PageNotPinnedException e)
	{
	}
	this->bufMgr->flushFile(this->file);
	this->scanExecuting = false;
	delete this->file;

}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// read the root page first
	Page* root_page;
	this->bufMgr->readPage(this->file, this->rootPageNum, root_page);

	PageKeyPair<int>*		newIntEntry = new PageKeyPair<int>();				
	PageKeyPair<double>*	newDoubleEntry = new PageKeyPair<double>();
	PageKeyPair<std::string>*		newStringEntry = new PageKeyPair<std::string>();
	int						intKey = 0;
	double					doubleKey = 0;
	bool					isLeaf;					// pass through the insert method to see if the return newChildEntry is a leaf node or not

	// set the key rid pair
	switch(this->attributeType){
		case(INTEGER):
			RIDKeyPair<int> intEntry;
			intKey = *((int*)key);
			intEntry.set(rid, intKey);
			if (this->isRootLeaf)
			{
				this->insert<RIDKeyPair<int>, PageKeyPair<int>, struct LeafNodeInt, struct NonLeafNodeInt>((LeafNodeInt*) root_page, NULL, intEntry, newIntEntry, isLeaf, this->rootPageNum);
			}else
			{
				this->insert<RIDKeyPair<int>, PageKeyPair<int>, struct LeafNodeInt, struct NonLeafNodeInt>(NULL, (NonLeafNodeInt* ) root_page, intEntry, newIntEntry, isLeaf, this->rootPageNum);
			}
			break;
		case(DOUBLE):
			RIDKeyPair<double> doubleEntry;
			doubleKey = *((double*)key);
			doubleEntry.set(rid, doubleKey);
			if (this->isRootLeaf)
			{
				this->insert<RIDKeyPair<double>, PageKeyPair<double>, struct LeafNodeDouble, struct NonLeafNodeDouble>((LeafNodeDouble*) root_page, NULL, doubleEntry, newDoubleEntry, isLeaf, this->rootPageNum);
			}else
			{
				this->insert<RIDKeyPair<double>, PageKeyPair<double>, struct LeafNodeDouble, struct NonLeafNodeDouble>(NULL, (NonLeafNodeDouble* ) root_page, doubleEntry, newDoubleEntry, isLeaf, this->rootPageNum);
			}
			break;
		case(STRING):
			RIDKeyPair<std::string> stringEntry;
			stringEntry.set(rid, std::string((char*)key));
			//stringEntry.rid = rid;
			//stringEntry.key = std::string((char*) key);
			//strcpy(stringEntry.key, (char*)key);
			if (this->isRootLeaf)
			{
				this->insert<RIDKeyPair<std::string>, PageKeyPair<std::string>, struct LeafNodeString, struct NonLeafNodeString>((LeafNodeString*) root_page, NULL, stringEntry, newStringEntry, isLeaf, this->rootPageNum);
			}else
			{
				this->insert<RIDKeyPair<std::string>, PageKeyPair<std::string>, struct LeafNodeString, struct NonLeafNodeString>(NULL, (NonLeafNodeString* ) root_page, stringEntry, newStringEntry, isLeaf, this->rootPageNum);
			}
			break;
	}

	// unpin the rootpage and flush the file
	this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
	this->bufMgr->flushFile(file);

	delete newIntEntry;
	delete newDoubleEntry;
	delete newStringEntry;

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	Page*	root_page;

	// if any scanning is running now, end it
	if (this->scanExecuting)
	{
		this->endScan();
	}

	// if the opcode is invalid
	if (((lowOpParm != GT) && (lowOpParm != GTE)) || ((highOpParm != LT) && (highOpParm != LTE)))
	{
		throw BadOpcodesException();
	}

	// first check keys
	if (compareConstKeys(lowValParm, highValParm) == 1)
	{
		throw BadScanrangeException();
	}

	// set up scan parameters
	this->lowOp = lowOpParm;
	this->highOp = highOpParm;
	this->scanExecuting = true;
	this->currentPageNum = this->rootPageNum;

	// read the root page
	this->bufMgr->readPage(this->file, this->rootPageNum, root_page);
	this->currentPageData = root_page;

	switch(this->attributeType)
	{
		case(INTEGER):
			this->lowValInt = *((int*)lowValParm);
			this->highValInt = *((int*)highValParm);
			(void*)(this->scan<int, LeafNodeInt, NonLeafNodeInt>(this->lowValInt, (NonLeafNodeInt*)root_page, NULL));
			break;
		case(DOUBLE):
			this->lowValDouble = *((double*)lowValParm);
			this->highValDouble = *((double*)highValParm);
			(void*)(this->scan<double, LeafNodeDouble, NonLeafNodeDouble>(this->lowValDouble, (NonLeafNodeDouble*)root_page, NULL));
			break;
		case(STRING):
			for (int i = 0; i < 10; i ++)
			{
				this->lowValString[i] = ((char*)lowValParm)[i];
				this->highValString[i] = ((char*)highValParm)[i];
			}
			(void*)(this->scan<const char*, LeafNodeString, NonLeafNodeString>(this->lowValString, (NonLeafNodeString*)root_page, NULL));
			break;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
	if (this->scanExecuting)
	{
		switch(this->attributeType)
		{
			case(INTEGER):
				getNextRId<LeafNodeInt, int>(outRid, this->highValInt);
				break;
			case(DOUBLE):
				getNextRId<LeafNodeDouble, double>(outRid, this->highValDouble);
				break;
			case(STRING):
				getNextRId<LeafNodeString, char*>(outRid, this->highValString);
				break;
		}
	}else
	{
		throw ScanNotInitializedException();
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	if (this->scanExecuting)
	{
		this->scanExecuting = false;
		try
		{
			this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
		}catch(PageNotPinnedException e)
		{
			// silently fail
		}
	}
	else
	{
		throw ScanNotInitializedException();
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: constructNew
// ------------------------------------------------------------------------------

void BTreeIndex::constructNew(const std::string & relationName, const int attrByteOffset, const Datatype attrType)
{
	Page* header_page;								// header page
	Page* root_page;								// root page
	struct LeafNodeInt* rootLeafNodeInt;			// root leaf node for int
	struct LeafNodeDouble* rootLeafNodeDouble;		// root leaf node for double
	struct LeafNodeString* rootLeafNodeString;		// root leaf node for string


	// set parts of memeber variables
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;

	// allocate header page and root page
	this->bufMgr->allocPage(this->file, this->headerPageNum, header_page);
	this->bufMgr->allocPage(this->file, this->rootPageNum, root_page);

	IndexMetaInfo* metaInfo = (IndexMetaInfo*) header_page;

	// when constrcting a new tree, the root node is a leaf node at the very beginning, so set corresponding var
	this->isRootLeaf = true;

	// set metaInfo
	metaInfo->attrByteOffset = attrByteOffset;
	metaInfo->attrType = attrType;
	strcpy(metaInfo->relationName,relationName.c_str());
	metaInfo->rootPageNo = this->rootPageNum;

	// unpin header page since we are done with it so far
	this->bufMgr->unPinPage(file, this->headerPageNum, true);

	// setup the root page depends on key type and also the node occupancy
	// since the tree is empty now, root node is the non leave node that right above leaf nodes
	switch(attrType){
		case (INTEGER):
			this->leafOccupancy = INTARRAYLEAFSIZE;
			this->nodeOccupancy = INTARRAYNONLEAFSIZE;
			this->allocateLeafNode<struct LeafNodeInt>(root_page, rootLeafNodeInt);
			rootLeafNodeInt->rightSibPageNo = 0;
			break;
		case (DOUBLE):
			this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
			this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
			this->allocateLeafNode<struct LeafNodeDouble>(root_page, rootLeafNodeDouble);
			rootLeafNodeDouble->rightSibPageNo = 0;
			break;
		case (STRING):
			this->leafOccupancy = STRINGARRAYLEAFSIZE;
			this->nodeOccupancy = STRINGARRAYNONLEAFSIZE;
			this->allocateLeafNode<struct LeafNodeString>(root_page, rootLeafNodeString);
			rootLeafNodeString->rightSibPageNo = 0;
			break;
	}

	// unpin root page since we are done with it so far
	this->bufMgr->unPinPage(file, this->rootPageNum, true);
	this->bufMgr->flushFile(file);


	{
		FileScan fscan(relationName, this->bufMgr);		// File Scan object
		try
		{
			RecordId scanRid;
			while(1)
			{
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				void *key;
				if (attrType == INTEGER)
				{
					key = ((int*)(record + attrByteOffset));
					this->insertEntry((char*)key, scanRid);
				}
				if (attrType == DOUBLE)
				{
					key = ((double*)(record + attrByteOffset));
					this->insertEntry((char*)key, scanRid);
				}
				if (attrType == STRING)
				{
					char* temp = ((char*)(record + attrByteOffset));
					/*char* keyArr = (char*)malloc(sizeof(char) * 10);
					for (int j = 0; j < 10; j++)
					{
						keyArr[j] = 0;
					}
					int i = 0;
					while (i < 10 || (((char*)temp)[i] == 0))
					{
						keyArr[i] = ((char*)temp)[i];
						i++;
					}*/
					std::string wholeKey = std::string(temp);
					std::string keyVal;
					keyVal = std::string(wholeKey.substr(0, 9).c_str());
					
					this->insertEntry(keyVal.c_str(), scanRid);
				}
			}
		}
		catch(EndOfFileException e)
		{
			//this->bufMgr->flushFile(this->file);
			std::cout<<"Read all records" <<std::endl;
		}
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: constructFromExist
// ------------------------------------------------------------------------------
void BTreeIndex::constructFromExist(const std::string & relationName, const int attrByteOffset, const Datatype attrType)
{
	Page*			header_page;
	IndexMetaInfo*	metaInfo;

	// set header page number and read it
	this->headerPageNum = 1;
	this->bufMgr->readPage(this->file, this->headerPageNum, header_page);
	// cast the header page to metaInfo struct
	metaInfo = (IndexMetaInfo*) header_page;

	// set member variable	
	if ((metaInfo->attrByteOffset == attrByteOffset) && (metaInfo->attrType == attrType))
	{
		this->attrByteOffset = metaInfo->attrByteOffset;
		this->attributeType = metaInfo->attrType;
		this->rootPageNum = metaInfo->rootPageNo;
	}

	if (metaInfo->rootPageNo != 2)
	{
		this->isRootLeaf = false;
	}else
	{
		this->isRootLeaf = true;
	}

	// set scanning state
	this->scanExecuting = false;
	
	// unpin header page
	this->bufMgr->unPinPage(this->file, this->headerPageNum, false);
	switch(attrType){
		case (INTEGER):
			this->leafOccupancy = INTARRAYLEAFSIZE;
			this->nodeOccupancy = INTARRAYNONLEAFSIZE;
			break;
		case (DOUBLE):
			this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
			this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
			break;
		case (STRING):
			this->leafOccupancy = STRINGARRAYLEAFSIZE;
			this->nodeOccupancy = STRINGARRAYNONLEAFSIZE;
			break;
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: insert
// ------------------------------------------------------------------------------
template<class RIDPairFormat, class PagePairFormat, class T_leafFormat, class T_nonleafFormat> void BTreeIndex::insert(T_leafFormat* leafNodePtr, T_nonleafFormat* nonleafNodePtr, RIDPairFormat entry, PagePairFormat* &newChildEntry, bool& isLeaf, int pageNum)
{
	// first check wheter the root node is a leaf node
	// if it is, jump to method that deals with this situation
	if(this->isRootLeaf)
	{
		insert_leafRoot<RIDPairFormat, PagePairFormat, T_leafFormat, T_nonleafFormat>(leafNodePtr, entry, newChildEntry, isLeaf, pageNum);
	}
	else
	{
		// if nonLeafNode is null, which means if we are currently looking at a leaf node
		if (nonleafNodePtr == NULL)
		{
			insert_leaf<RIDPairFormat, PagePairFormat, T_leafFormat, T_nonleafFormat>(leafNodePtr, entry, newChildEntry, isLeaf, pageNum);
		}
		// else
		else
		{
			insert_nonleaf<RIDPairFormat, PagePairFormat, T_leafFormat, T_nonleafFormat>(nonleafNodePtr, entry, newChildEntry, isLeaf, pageNum);
		}
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: insert_leafRoot
// ------------------------------------------------------------------------------
template<class RIDPairFormat, class PagePairFormat, class T_leafFormat, class T_nonleafFormat> void BTreeIndex::insert_leafRoot(T_leafFormat* nodePtr, RIDPairFormat entry, PagePairFormat* &newChildEntry, bool& isLeaf, int pageNum)
{
	// first check if the root node has space, if it has, simply put entry on it, since the root node is a leaf node now
	if (leafNodeHasSpace<T_leafFormat>(nodePtr))
	{
		putLeafNodeEntry<RIDPairFormat, PagePairFormat, T_leafFormat>(nodePtr, entry, newChildEntry);
	}
	else
	{
		PageId newPageNum = splitLeafNode<RIDPairFormat, PagePairFormat, T_leafFormat>(nodePtr, entry, newChildEntry);
		isLeaf = true;
		// create new root
		createNewRoot<T_leafFormat, T_nonleafFormat>(nodePtr, pageNum, newPageNum, isLeaf);
		// since we just split the leaf node, we will create a new root node in upper level, so the root node is no longer a leaf node now
		this->isRootLeaf = false;
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: insert_leaf
// ------------------------------------------------------------------------------
template<class RIDPairFormat, class PagePairFormat, class T_leafFormat, class T_nonleafFormat> void BTreeIndex::insert_leaf(T_leafFormat* nodePtr, RIDPairFormat entry, PagePairFormat* &newChildEntry, bool& isLeaf, int pageNum)
{
	// first check if the root node has space, if it has, simply put entry on it, since the root node is a leaf node now
	if (leafNodeHasSpace<T_leafFormat>(nodePtr))
	{
		putLeafNodeEntry<RIDPairFormat, PagePairFormat, T_leafFormat>(nodePtr, entry, newChildEntry);
	}
	else
	{
		PageId newPageNum = splitLeafNode<RIDPairFormat, PagePairFormat, T_leafFormat>(nodePtr, entry, newChildEntry);
		// set isLeaf
		isLeaf = true;
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: putLeafNodeEntry
// ------------------------------------------------------------------------------
template<class RIDPairFormat, class PagePairFormat, class T_leafFormat> void BTreeIndex::putLeafNodeEntry(T_leafFormat* &leafNode, RIDPairFormat entry, PagePairFormat* &newChildEntry)
{
	int index = 0;			// the next available index to put the entry
	T_leafFormat* node = (T_leafFormat*) leafNode;
	// first find the index
	while((node->ridArray[index].page_number != 0) && (compareKeys((void*)(&(node->keyArray[index])), &(entry.key)) == 0))
	{
		index ++;
	}
	// reside space for the new entry
	for (int i = leafOccupancy - 1; i > index; i --)
	{
		node->keyArray[i] = node->keyArray[i-1];
		node->ridArray[i] = node->ridArray[i-1];
	}
	// put the entry on that index position
	//node->keyArray[index] = entry.key;
	putLeafString((void*)node, (void*)&(entry.key), index);
	node->ridArray[index] = entry.rid;
	newChildEntry = NULL;
}

// ------------------------------------------------------------------------------
// BTreeIndex:: putLeafString
// ------------------------------------------------------------------------------
void BTreeIndex::putLeafString(void* leafNode, void* key, int index)
{
	switch(this->attributeType)
	{
		case(INTEGER):
			((LeafNodeInt*)leafNode)->keyArray[index] = *((int*)key);
			break;
		case(DOUBLE):
			((LeafNodeDouble*)leafNode)->keyArray[index] = *((double*)key);
			break;
		case(STRING):
			((LeafNodeString*)leafNode)->keyArray[index] = *((std::string*)key);
			break;
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: splitLeafNode
// ------------------------------------------------------------------------------
template<class RIDPairFormat, class PagePairFormat, class T_leafFormat> PageId BTreeIndex::splitLeafNode(T_leafFormat* &leafNode, RIDPairFormat entry, PagePairFormat* &newChildEntry)
{
	// split the current leafNode, first half entries stay, copy others to a newly created leafNode
	Page* newPage;						// the new page we are going to allocate
	PageId newPageNum;					// the page number for new page
	T_leafFormat* newLeafNode;			// the newly created leaf node
	int halfSize;						// the half size of leaf node

	// initialize some variables
	halfSize = this->leafOccupancy/2 + 1;
	
	// allocate a new page and allocate it as a leaf node
	this->bufMgr->allocPage(this->file, newPageNum, newPage);
	this->allocateLeafNode<T_leafFormat> (newPage, newLeafNode);

	// copy the latter half entries to the new node, and clear the original entries
	for (int i = halfSize; i < (this->leafOccupancy); i ++)
	{
		newLeafNode->keyArray[i-halfSize] = leafNode->keyArray[i];
		newLeafNode->ridArray[i-halfSize] = leafNode->ridArray[i];
		leafNode->ridArray[i].page_number = 0;
	}

	if (compareKeys((void*)(&(entry.key)), (void*)(&(leafNode->keyArray[halfSize - 1]))) == 0)
	{
		// put the entry
		putLeafNodeEntry<RIDPairFormat, PagePairFormat, T_leafFormat>(leafNode, entry, newChildEntry);
	}else
	{
		// put the entry
		putLeafNodeEntry<RIDPairFormat, PagePairFormat, T_leafFormat>(newLeafNode, entry, newChildEntry);
	}
	newChildEntry = new PagePairFormat();
	// initialize the newChildEntry
	newChildEntry->set(newPageNum, leafNode->keyArray[halfSize-1]);

	// set sibling number point to the new page
	newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
	leafNode->rightSibPageNo = newPageNum;

	// done with the newly created page, unpin and flush
	this->bufMgr->unPinPage(this->file, newPageNum, true);

	return newPageNum;
}

// ------------------------------------------------------------------------------
// BTreeIndex:: insert_nonleaf
// ------------------------------------------------------------------------------
template<class RIDPairFormat, class PagePairFormat, class T_leafFormat, class T_nonleafFormat> void BTreeIndex::insert_nonleaf(T_nonleafFormat* nonleafNodePtr, RIDPairFormat entry, PagePairFormat* &newChildEntry, bool& isLeaf, int pageNum)
{
	PageId				childPageNum;
	Page*				childPage;
	T_nonleafFormat*	childNonleafNode = NULL;
	T_leafFormat*		childLeafNode = NULL;
	// choose subtree
	childPageNum = chooseSubtree<RIDPairFormat, T_nonleafFormat>(nonleafNodePtr, entry);

	// read the page
	this->bufMgr->readPage(this->file, childPageNum, childPage);

	// if the child node is not a leaf node, cast the child page to a nonleaf node
	if (nonleafNodePtr->level == 0)
	{
		allocateNonleafNode(childPage, childNonleafNode);
	}
	// if the child node is a leaf node, cast the child page to a leaf node
	else
	{
		allocateLeafNode(childPage, childLeafNode);

	}
	// recursively call insert here
	insert<RIDPairFormat, PagePairFormat, T_leafFormat, T_nonleafFormat>(childLeafNode, childNonleafNode, entry, newChildEntry, isLeaf, childPageNum);
	
	// after the recursion return, check if newChildEntry is empty
	if (newChildEntry == NULL)
	{
		this->bufMgr->unPinPage(this->file, childPageNum, true);
		return;
	}
	else
	{
		if (nonLeafNodeHasSpace(nonleafNodePtr))
		{
			putNonleafNodeEntry(nonleafNodePtr, *newChildEntry, newChildEntry);
		}else
		{
			PageId newPageNum= splitNonleafNode<PagePairFormat, T_nonleafFormat>(nonleafNodePtr, *newChildEntry, newChildEntry);
			isLeaf = false;
			if (((Page*)nonleafNodePtr)->page_number() == this->rootPageNum)
			{
				createNewRoot<T_nonleafFormat, T_nonleafFormat>(nonleafNodePtr, pageNum, newPageNum, isLeaf);
			}
		}
	}

	this->bufMgr->unPinPage(this->file, childPageNum, true);
}

// ------------------------------------------------------------------------------
// BTreeIndex:: putNonleafNodeEntry
// ------------------------------------------------------------------------------
template<class PagePairFormat, class T_nonleafFormat> void BTreeIndex::putNonleafNodeEntry(T_nonleafFormat* &nonleafNode, PagePairFormat entry, PagePairFormat* &newChildEntry)
{
	int index = 0;
	// first find the index
	while((nonleafNode->pageNoArray[index+1] != 0) && (compareKeys((void*)(&(nonleafNode->keyArray[index])), &(entry.key)) == 0))
	{
		index ++;
	}
	// reside space for the new entry
	for (int i = nodeOccupancy - 1; i > index; i --)
	{
		nonleafNode->keyArray[i] = nonleafNode->keyArray[i-1];
	}
	for (int i = nodeOccupancy; i > (index + 1); i --)
	{
		nonleafNode->pageNoArray[i] = nonleafNode->pageNoArray[i-1];
	}
	// put entry in
	nonleafNode->keyArray[index] = entry.key;
	nonleafNode->pageNoArray[index+1] = entry.pageNo;
	newChildEntry = NULL;
}

// ------------------------------------------------------------------------------
// BTreeIndex:: splitNonleafNode
// ------------------------------------------------------------------------------
template<class PagePairFormat, class T_nonleafFormat> PageId BTreeIndex::splitNonleafNode(T_nonleafFormat* &nonleafNode, PagePairFormat entry, PagePairFormat* &newChildEntry)
{
	Page*					newPage;
	PageId					newPageNum;
	T_nonleafFormat*		newNonleafNode;
	int						halfSize;

	// initialize some variables
	halfSize = this->nodeOccupancy/2 + 1;
	
	// allocate a new page and allocate it as a leaf node
	this->bufMgr->allocPage(this->file, newPageNum, newPage);
	this->allocateLeafNode<T_nonleafFormat> (newPage, newNonleafNode);

	newNonleafNode->level = nonleafNode->level;

	// copy the latter half entries to the new node, and clear the original entries
	for (int i = halfSize; i < (this->nodeOccupancy); i ++)
	{
		newNonleafNode->keyArray[i-halfSize] = nonleafNode->keyArray[i];
		newNonleafNode->pageNoArray[i-halfSize + 1] = nonleafNode->pageNoArray[i + 1];
		nonleafNode->pageNoArray[i + 1] = 0;
	}

	if (compareKeys((void*)(&(entry.key)), (void*)(&(nonleafNode->keyArray[halfSize - 1]))) == 0)
	{
		// put the entry
		putNonleafNodeEntry<PagePairFormat, T_nonleafFormat>(nonleafNode, entry, newChildEntry);
	}else
	{
		// put the entry
		putNonleafNodeEntry<PagePairFormat, T_nonleafFormat>(newNonleafNode, entry, newChildEntry);
	}

	// put the entry
	// putNonleafNodeEntry<PagePairFormat, T_nonleafFormat>(newNonleafNode, entry, newChildEntry);
	// initialize the newChildEntry
	newChildEntry->set(newPageNum, newNonleafNode->keyArray[0]);

	// set sibling number point to the new page
	// nonleafNode->rightSibPageNo = newPageNum;

	// done with the newly created page, unpin and flush
	this->bufMgr->unPinPage(this->file, newPageNum, true);
	// this->bufMgr->flushFile(this->file);

	return newPageNum;
}

// ------------------------------------------------------------------------------
// BTreeIndex:: chooseSubtree
// ------------------------------------------------------------------------------
template <class RIDPairFormat, class T_nonleafFormat> PageId BTreeIndex::chooseSubtree(T_nonleafFormat* nonleafNodePtr, RIDPairFormat entry)
{
	PageId		pageIndex = -1;
	int			i = 0;
	int			lastAvailableIndex = 0;

	// find the last available slot in thei nonleaf node
	while (nonleafNodePtr->pageNoArray[lastAvailableIndex] != 0)
	{
		lastAvailableIndex ++;
	}
	lastAvailableIndex = lastAvailableIndex - 2;

	// start comparing
	if (compareKeys((void*)(&(nonleafNodePtr->keyArray[i])), (void*)((&entry.key))) == 1)
	{
		pageIndex = 0;
	}
	else if (compareKeys((void*)((&entry.key)), (void*)&((nonleafNodePtr->keyArray[lastAvailableIndex]))) == 1)
	{
		pageIndex = lastAvailableIndex + 1;
	}
	else
	{
		for (i = 0; i < this->nodeOccupancy - 1; i ++)
		{
			int compareResult1 = compareKeys((void*)&((nonleafNodePtr->keyArray[i])), (void*)&((entry.key)));
			int compareResult2 = compareKeys((void*)&((nonleafNodePtr->keyArray[i+1])), (void*)&((entry.key)));
			if (compareResult1 == 0 && compareResult2 == 1)
			{
				pageIndex = i + 1;
				break;
			}
		}
	}

	// check error
	if (pageIndex == -1)
	{
		std::cout<<"not found pageIndex."<<std::endl;
	}
	return nonleafNodePtr->pageNoArray[pageIndex];

}

// ------------------------------------------------------------------------------
// BTreeIndex:: createNewRoot
// ------------------------------------------------------------------------------
template<class T_nodeFormat, class T_nonleafFormat> void BTreeIndex::createNewRoot(T_nodeFormat* nodePtr1, int pageNumA, int pageNumB, bool& isLeaf)
{
	Page*				newRootPage;
	T_nonleafFormat*	newRootNode;
	int					keyIndex;
	Page*				header_page;
	IndexMetaInfo*		metaInfo;
	
	// first unpin the old root page
	this->bufMgr->unPinPage(this->file, this->rootPageNum, true);

	// allocate a new page for the new root node
	this->bufMgr->allocPage(this->file, this->rootPageNum, newRootPage);
	this->allocateNonleafNode<T_nonleafFormat>(newRootPage, newRootNode);
	if (isLeaf)
	{
		newRootNode->level = 1;
		keyIndex = this->leafOccupancy/2;
	}else
	{
		newRootNode->level = 0;
		keyIndex = this->nodeOccupancy/2;
	}
	
	newRootNode->keyArray[0] = nodePtr1->keyArray[keyIndex];
	newRootNode->pageNoArray[0] = pageNumA;
	newRootNode->pageNoArray[1] = pageNumB;

	// update header page
	this->bufMgr->readPage(this->file, this->headerPageNum, header_page);
	metaInfo = (IndexMetaInfo*) header_page;
	metaInfo->rootPageNo = this->rootPageNum;
	
	// done with the new page, unpin and flush
	this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
	// this->bufMgr->flushFile(this->file);
}

// ------------------------------------------------------------------------------
// BTreeIndex:: nonLeafNodeHasSpace
// ------------------------------------------------------------------------------
template<class T_nonLeafFormat> bool BTreeIndex::nonLeafNodeHasSpace(T_nonLeafFormat* nonLeafNode)
{
	if (nonLeafNode->pageNoArray[nodeOccupancy] == 0)
	{
		return true;
	}
	else
	{
		return false;
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: leafNodeHasSpace
// ------------------------------------------------------------------------------
template<class T_leafFormat> bool BTreeIndex::leafNodeHasSpace(T_leafFormat* leafNode)
{
	if (leafNode->ridArray[leafOccupancy - 1].page_number == 0)
	{
		return true;
	}
	else
	{
		return false;
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: allocateLeafNode
// ------------------------------------------------------------------------------
template <class T_leafFormat> void BTreeIndex::allocateLeafNode(Page* &page, T_leafFormat* &node)
{
		node = (T_leafFormat*) page;
}

// ------------------------------------------------------------------------------
// BTreeIndex:: allocateNonleafNode
// ------------------------------------------------------------------------------
template <class T_nonleafFormat> void BTreeIndex::allocateNonleafNode(Page* &page, T_nonleafFormat* &node)
{
		node = (T_nonleafFormat*) page;
}

// ------------------------------------------------------------------------------
// BTreeIndex:: compareKeys
// ------------------------------------------------------------------------------
template <class T_type, class T_leafFormat, class T_nonleafFormat> T_leafFormat* BTreeIndex::scan(T_type lowVal, T_nonleafFormat* nonleafNode, T_leafFormat* leafNode)
{
	Page*				page;
	T_leafFormat*		pageLeafNode = NULL;
	T_nonleafFormat*	pageNonleafNode = NULL;
	int					lastAvailableIndex = 0;
	if (leafNode != NULL)
	{
		// found the leaf node, set next entry and return the leaf node
		int i = 0;
		while (compareKeysWithOperator((void*)(&(leafNode->keyArray[i])), (void*)(&lowVal), this->lowOp) == 0)
		{
			i ++;
		}
		this->nextEntry = i;
		return leafNode;
	}else
	{
		// find the last available slot in thei nonleaf node
		while (nonleafNode->pageNoArray[lastAvailableIndex] != 0)
		{
			lastAvailableIndex ++;
		}
		lastAvailableIndex = lastAvailableIndex - 2;
		if (compareKeys((void*)(&lowVal), (void*)(&(nonleafNode->keyArray[0]))) == 0)
		{
			this->bufMgr->readPage(this->file, nonleafNode->pageNoArray[0], page);
			this->currentPageNum = nonleafNode->pageNoArray[0];
			this->currentPageData = page;
			this->bufMgr->unPinPage(this->file, nonleafNode->pageNoArray[0], false);
			if (nonleafNode->level == 1)
			{
				allocateLeafNode<T_leafFormat>(page, pageLeafNode);
			}else
			{
				allocateNonleafNode<T_nonleafFormat>(page, pageNonleafNode);
			}
			return this->scan<T_type, T_leafFormat, T_nonleafFormat>(lowVal, pageNonleafNode, pageLeafNode);
			
		}
		else if (compareKeys((void*)(&lowVal), (void*)(&(nonleafNode->keyArray[lastAvailableIndex]))) == 1)
		{
			this->bufMgr->readPage(this->file, nonleafNode->pageNoArray[lastAvailableIndex+1], page);
			this->currentPageNum = nonleafNode->pageNoArray[lastAvailableIndex+1];
			this->currentPageData = page;
			this->bufMgr->unPinPage(this->file, nonleafNode->pageNoArray[lastAvailableIndex+1], false);
			if (nonleafNode->level == 1)
			{
				allocateLeafNode<T_leafFormat>(page, pageLeafNode);
			}else
			{
				allocateNonleafNode<T_nonleafFormat>(page, pageNonleafNode);
			}
			return this->scan<T_type, T_leafFormat, T_nonleafFormat>(lowVal, pageNonleafNode, pageLeafNode);
		}
		else
		{
			for (int i = 0; i < this->nodeOccupancy - 1; i ++)
			{
				int compareResult1 = compareKeys((void*)&((nonleafNode->keyArray[i])), (void*)(&(lowVal)));
				int compareResult2 = compareKeys((void*)&((nonleafNode->keyArray[i+1])), (void*)(&(lowVal)));
				if (compareResult1 == 0 && compareResult2 == 1)
				{
					this->bufMgr->readPage(this->file, nonleafNode->pageNoArray[i+1], page);
					this->currentPageNum = nonleafNode->pageNoArray[i+1];
					this->currentPageData = page;
					this->bufMgr->unPinPage(this->file, nonleafNode->pageNoArray[i+1], false);
					if (nonleafNode->level == 1)
					{
						allocateLeafNode<T_leafFormat>(page, pageLeafNode);
					}else
					{
						allocateNonleafNode<T_nonleafFormat>(page, pageNonleafNode);
					}
					return this->scan<T_type, T_leafFormat, T_nonleafFormat>(lowVal, pageNonleafNode, pageLeafNode);
				}
			}
		}
	}
}

template <class T_leafFormat, class T_type> void BTreeIndex::getNextRId(RecordId& outRid, T_type highVal)
{
	T_leafFormat*	leafNode = (T_leafFormat*)(this->currentPageData);

	outRid = leafNode->ridArray[this->nextEntry];

	// if we have scanned through the entire leaf page, move to the next leaf page
	if (((this->nextEntry) == (this->leafOccupancy - 1)) || (leafNode->ridArray[this->nextEntry+1].page_number == 0))
	{
		this->currentPageNum = leafNode->rightSibPageNo;
		this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
		this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
		this->nextEntry = 0;
	}else
	{
		this->nextEntry ++;
	}

	leafNode = (T_leafFormat*)(this->currentPageData);
	if (this->currentPageNum != 0)
	{
		// if the nextEntry does not satisfy the highOp, end scanning. Otherwise proceed
		if (compareKeysWithOperator((void*)(&(leafNode->keyArray[this->nextEntry])), (void*)(&(highVal)), this->highOp) == 0)
		{
			throw IndexScanCompletedException();
		}
	}
	else
	{
			throw IndexScanCompletedException();
	}

}

// ------------------------------------------------------------------------------
// BTreeIndex:: compareKeys
// ------------------------------------------------------------------------------
int BTreeIndex::compareKeys(void* key1, void* key2)
{
	switch(this->attributeType)
	{
		case(INTEGER):
			if ((*(int*)key1) <= (*(int*)key2))
			{
				return 0;
			}
			else
			{
				return 1;
			}
			break;
		case(DOUBLE):
			if ((*(double*)key1) <= (*(double*)key2))
			{
				return 0;
			}
			else
			{
				return 1;
			}
			break;
		case(STRING):
			if (strncmp((*(std::string*)key1).c_str(), (*(std::string*)key2).c_str(), 10) <= 0)
			{
				return 0;
			}
			else
			{
				return 1;
			}
			break;
	}
}

// ------------------------------------------------------------------------------
// BTreeIndex:: compareConstKeys
// ------------------------------------------------------------------------------
int BTreeIndex::compareConstKeys(const void* key1, const void* key2)
{
	switch(this->attributeType)
	{
		case(INTEGER):
			if ((*(int*)key1) <= (*(int*)key2))
			{
				return 0;
			}
			else
			{
				return 1;
			}
			break;
		case(DOUBLE):
			if ((*(double*)key1) <= (*(double*)key2))
			{
				return 0;
			}
			else
			{
				return 1;
			}
			break;
		case(STRING):
			if (strncmp((*(std::string*)&key1).c_str(), (*(std::string*)&key2).c_str(), 10) <= 0)
			{
				return 0;
			}
			else
			{
				return 1;
			}
			break;
	}
}

int BTreeIndex::compareKeysWithOperator(void* key1, void* key2, Operator op)
{
	if (op == LT)
	{
		switch(this->attributeType)
		{
			case(INTEGER):
				if ((*(int*)key1) < (*(int*)key2))
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
			case(DOUBLE):
				if ((*(double*)key1) < (*(double*)key2))
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
			case(STRING):
				if (strncmp((*(std::string*)key1).c_str(), (*(std::string*)key2).c_str(), 10) < 0)
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
		}
	}

	if (op == LTE)
	{
		switch(this->attributeType)
		{
			case(INTEGER):
				if ((*(int*)key1) <= (*(int*)key2))
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
			case(DOUBLE):
				if ((*(double*)key1) <= (*(double*)key2))
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
			case(STRING):
				if (strncmp((*(std::string*)key1).c_str(), (*(std::string*)key2).c_str(), 10) <= 0)
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
		}
	}

	if (op == GT)
	{
		switch(this->attributeType)
		{
			case(INTEGER):
				if ((*(int*)key1) > (*(int*)key2))
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
			case(DOUBLE):
				if ((*(double*)key1) > (*(double*)key2))
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
			case(STRING):
				if (strncmp((*(std::string*)key1).c_str(), (*(std::string*)key2).c_str(), 10) > 0)
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
		}
	}

	if (op == GTE)
	{
		switch(this->attributeType)
		{
			case(INTEGER):
				if ((*(int*)key1) >= (*(int*)key2))
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
			case(DOUBLE):
				if ((*(double*)key1) >= (*(double*)key2))
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
			case(STRING):
				if (strncmp((*(std::string*)key1).c_str(), (*(std::string*)key2).c_str(), 10) >= 0)
				{
					return 1;
				}
				else
				{
					return 0;
				}
				break;
		}
	}
}
}

