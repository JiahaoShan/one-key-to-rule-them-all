/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"

namespace badgerdb
{

/**
 * @brief Datatype enumeration type.
 */
enum Datatype
{
	INTEGER = 0,
	DOUBLE = 1,
	STRING = 2
};

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator
{ 
	LT, 	/* Less Than */
	LTE,	/* Less Than or Equal to */
	GTE,	/* Greater Than or Equal to */
	GT		/* Greater Than */
};

/**
 * @brief Size of String key.
 */
const  int STRINGSIZE = 10;

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key               rid
const  int INTARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                     sibling ptr               key               rid
const  int DOUBLEARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( double ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                    sibling ptr           key                      rid
const  int STRINGARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof(std::string) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo                  key       pageNo
const  int INTARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( PageId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                        level        extra pageNo                 key            pageNo   -1 due to structure padding
const  int DOUBLEARRAYNONLEAFSIZE = (( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( double ) + sizeof( PageId ) )) - 1;

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                        level        extra pageNo             key                   pageNo
const  int STRINGARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof(std::string) + sizeof( PageId ) );

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to functions that 
 * add to or make changes to the leaf node pages of the tree. Is templated for the key member.
 */
template <class T>
class RIDKeyPair{
public:
	RecordId rid;
	T key;
	void set( RecordId r, T k)
	{
		rid = r;
		key = k;
	}
};

/**
 * @brief Structure to store a key page pair which is used to pass the key and page to functions that make 
 * any modifications to the non leaf pages of the tree.
*/
template <class T>
class PageKeyPair{
public:
	PageId pageNo;
	T key;
	void set( int p, T k)
	{
		pageNo = p;
		key = k;
	}
};

/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
template <class T>
bool operator<( const RIDKeyPair<T>& r1, const RIDKeyPair<T>& r2 )
{
	if( r1.key != r2.key )
		return r1.key < r2.key;
	else
		return r1.rid.page_number < r2.rid.page_number;
}

/**
 * @brief The meta page, which holds metadata for Index file, is always first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
struct IndexMetaInfo{
  /**
   * Name of base relation.
   */
	char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in pages.
   */
	int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
	Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
	PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in the pages for the index file depending on what kind of 
node they are. The level memeber of each non leaf structure seen below is set to 1 if the nodes 
at this level are just above the leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
*/
struct NonLeafNodeInt{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ INTARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all non-leaf nodes when the key is of DOUBLE type.
*/
struct NonLeafNodeDouble{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	double keyArray[ DOUBLEARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ DOUBLEARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all non-leaf nodes when the key is of STRING type.
*/
struct NonLeafNodeString{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	std::string keyArray[ STRINGARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ STRINGARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
*/
struct LeafNodeInt{
  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ INTARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief Structure for all leaf nodes when the key is of DOUBLE type.
*/
struct LeafNodeDouble{
  /**
   * Stores keys.
   */
	double keyArray[ DOUBLEARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ DOUBLEARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief Structure for all leaf nodes when the key is of STRING type.
*/
struct LeafNodeString{
  /**
   * Stores keys.
   */
	std::string keyArray[ STRINGARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ STRINGARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute of a
 * relation. This index supports only one scan at a time.
*/
class BTreeIndex {

 private:

  /**
   * File object for the index file.
   */
	File		*file;

  /**
   * Buffer Manager Instance.
   */
	BufMgr	*bufMgr;

  /**
   * Page number of meta page.
   */
	PageId	headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
	PageId	rootPageNum;

  /**
   * Datatype of attribute over which index is built.
   */
	Datatype	attributeType;

  /**
   * Offset of attribute, over which index is built, inside records. 
   */
	int 		attrByteOffset;

  /**
   * Number of keys in leaf node, depending upon the type of key.
   */
	int			leafOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int			nodeOccupancy;

	// CONSTUMIZE MEMBERS

  /**
   * True if the root is a leaf node
   */
	bool		isRootLeaf;

	// MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
	bool		scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
	int			nextEntry;

  /**
   * Page number of current page being scanned.
   */
	PageId	currentPageNum;

  /**
   * Current Page being scanned.
   */
	Page		*currentPageData;

  /**
   * Low INTEGER value for scan.
   */
	int			lowValInt;

  /**
   * Low DOUBLE value for scan.
   */
	double	lowValDouble;

  /**
   * Low STRING value for scan.
   */
	char*	lowValString;

  /**
   * High INTEGER value for scan.
   */
	int			highValInt;

  /**
   * High DOUBLE value for scan.
   */
	double	highValDouble;

  /**
   * High STRING value for scan.
   */
	char* highValString;
	
  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
	Operator	lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
	Operator	highOp;


	// Costumize Methods

  /*
   * Construct a new B+ tree
   * This method will build a new B+ tree on a newly created index file. In other words, it will scan through
   * all relations in the existing data file and insert them into a blank tree
   *
   * @param relationName			The relation name that we are going to scan through
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType				Datatype of attribute over which index is built
   */
	void constructNew(const std::string & relationName, const int attrByteOffset, const Datatype attrType);

	 /*
   * Construct a btree from the existing index file
   *
   * @param relationName			The relation name that we are going to scan through
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType				Datatype of attribute over which index is built
   */
	void constructFromExist(const std::string & relationName, const int attrByteOffset, const Datatype attrType);

   /*
    * Recursively insert an entry into the B+ tree starting from the root. This is the private implementation
	* of the public insertEntry method. In other words, we will recursively call this method instead of the
	* public method to achieve insertion
	*
	* @param nodePtr		The node pointer we are currently looking at
	* @param entry			The key rid pari we would like to insert
	* @param newChildEntry	The potential new entry that we might create due to splitting a node
	*/
	template<class RIDPairFormat, class PagePairFormat, class T_leafFormat, class T_nonleafFormat> void insert(T_leafFormat* leafNodePtr, T_nonleafFormat* nonleafNodePtr, RIDPairFormat entry, PagePairFormat* &newChildEntry, bool& isLeaf, int pageNum);

   /*
	* This method deals will the situation that we are inserting an entry when our root node is a leaf node.
	* Generally this is the situation when the first couple pages are inserting. As long as the root node get filled
	* up, we will split it and generate a new root node. From that on, the root node will be a non-leaf node and
	* this method will not be invoked
	*
	* Note that we use template here because we may dealing with different node format
	*
	* @param nodePtr		The node pointer we are currently looking at
	* @param entry			The key rid pari we would like to insert
	* @param newChildEntry	The potential new entry that we might create due to splitting a node
	*/
	template<class RIDPairFormat, class PagePairFormat, class T_leafFormat, class T_nonleafFormat> void insert_leafRoot(T_leafFormat* nodePtr, RIDPairFormat entry, PagePairFormat* &newChildEntry, bool& isLeaf, int pageNum);

	/*
	* This method deals will the situation that we are inserting an entry to the a leaf node
	* Generally we will put that entry on the leaf node and check if we need to split the leaf node, 
	* if we do, we split it and return
	*
	* Note that we use template here because we may dealing with different node format
	*
	* @param nodePtr		The node pointer we are currently looking at
	* @param entry			The key rid pari we would like to insert
	* @param newChildEntry	The potential new entry that we might create due to splitting a node
	*/
	template<class RIDPairFormat, class PagePairFormat, class T_leafFormat, class T_nonleafFormat> void insert_leaf(T_leafFormat* nodePtr, RIDPairFormat entry, PagePairFormat* &newChildEntry, bool& isLeaf, int pageNum);

   /*
    * This method simply puts an entry on a leaf node, and set the newChildEntry to NULL
	*
	* Note that we use template here because we may dealing with different node format
	*
	* @param leafNode		The leaf node we wanna add things
	* @param entry			The key rid pari we would like to insert
	* @param newChildEntry	The potential new entry that we might create due to splitting a node
	*/
	template<class RIDPairFormat, class PagePairFormat, class T_leafFormat> void putLeafNodeEntry(T_leafFormat* &leafNode, RIDPairFormat entry, PagePairFormat* &newChildEntry);

	void putLeafString(void* leafNode, void* key, int index);

   /*
    * This method splits a leaf node and therefore create a new child entry node
	*
	* Note that we use template here because we may dealing with different node format
	*
	* @param leafNode		The leaf node we wanna add things
	* @param entry			The key rid pari we would like to insert
	* @param newChildEntry	The new entry that we create and return
	*
	* @return the newly created node
	*/
	template<class RIDPairFormat, class PagePairFormat, class T_leafFormat> PageId splitLeafNode(T_leafFormat* &leafNode, RIDPairFormat entry, PagePairFormat* &newChildEntry);

	/*
	* This method handles the case that we encounter a nonleaf node during the inserting recurrsion
	* It will first recursively invoke the insert method, and then if necessary, it will split
	* itself and return the newChildEntry to the upper level
	*
	* Note that we use template here because we may dealing with different node format
	*
	* @param nonleafNodePtr	The nonleaf node we are currently looking at
	* @param entry			The key rid pari we would like to insert
	* @param newChildEntry	The potential new entry that we might create due to splitting a node
	*/
	template<class RIDPairFormat, class PagePairFormat, class T_leafFormat, class T_nonleafFormat> void insert_nonleaf(T_nonleafFormat* nonleafNodePtr, RIDPairFormat entry, PagePairFormat* &newChildEntry, bool& isLeaf, int pageNum);

	/*
    * This method simply puts an entry on a non-leaf node, and set the newChildEntry to NULL
	*
	* Note that we use template here because we may dealing with different node format
	*
	* @param nonleafNode	The non-leaf node we wanna add things
	* @param entry			The page key pair we would like to insert
	* @param newChildEntry	The potential new entry that we might create due to splitting a node
	*/
	template<class PagePairFormat, class T_nonleafFormat> void putNonleafNodeEntry(T_nonleafFormat* &nonleafNode, PagePairFormat entry, PagePairFormat* &newChildEntry);

	/*
    * This method splits a non-leaf node and therefore create a new child entry node
	*
	* Note that we use template here because we may dealing with different node format
	*
	* @param nonleafNode	The non-leaf node we wanna add things
	* @param entry			The page key pair we would like to insert
	* @param newChildEntry	The new entry that we create and return
	*
	* @return the newly created node
	*/
	template<class PagePairFormat, class T_nonleafFormat> PageId splitNonleafNode(T_nonleafFormat* &nonleafNode, PagePairFormat entry, PagePairFormat* &newChildEntry);

	/*
    * This method determins what subtree (child page) should be picked in order to 
	* recursively process the insert operation.
	* It will compare the given entry's key with all keys in the non-leaf node and hence
	* determine what pageNo we should pick from the pageNoArray
	*
	* Note that we use template here because we may dealing with different node format
	*
	* @param nonleafNodePtr	The non-leaf node we are looking at
	* @param entry			The key rid pari we would like to insert
	*
	* @return the index of the pageNoArray we choose
	*/
	template <class RIDPairFormat, class T_nonleafFormat> PageId chooseSubtree(T_nonleafFormat* nonleafNodePtr, RIDPairFormat entry);

   /*
    * This method creates a new root node and will be called after splitting the original root node
	*
	* @param nodePtr1		One of the two nodes result from root splitting
	* @param nodePtr2		One of the two nodes result from root splitting
	* @param entry			The key rid pari we would like to insert
	* @param newChildEntry	The new entry that we create and return
	*/
	template<class T_nodeFormat, class T_nonleafFormat> void createNewRoot(T_nodeFormat* nodePtr1, int pageNumA, int pageNumB, bool& isLeaf);
   /*
    * This method simply check whether a given non-leaf node still has space.
	* Since initially all elements in PageNoArray of a nonLeafNode is 0, and
	* PageId is always greater than zero, we simply need to check the last element
	* of PageNoArray, if it is 0, the node has space; otherwise, it hasn't
	*
	* Note that we use template here because we may dealing with different node format\
	*
	* @param nonLeafNode	The nonLeafNode we want to check
	*/
	template<class T_nonLeafFormat> bool nonLeafNodeHasSpace(T_nonLeafFormat* nonLeafNode);

	/*
    * This method simply check whether a given leaf node still has space.
	* Since initially each page_number of each element in ridArray of a leafNode is 0,
	* and pageNumber is always greater than zero, we simply need to check the last element's
	* page_number in leafNode, if it is 0, the node has space; otherwise, it hasn't
	*
	* Note that we use template here because we may dealing with different node format\
	*
	* @param leafNode		The leafNode we want to check
	*/
	template<class T_leafFormat> bool leafNodeHasSpace(T_leafFormat* leafNode);

   /*
    * This is a method that convert a page to non-leaf node, 
	* and initialize the level member of  the non-leaf node
	*
	* Note that we use template here because we may dealing with different node format
	* 
	* @param page			The page we would convert
	* @param level			The initial value of member level
	*
	*/
	template <class T_nonleafFormat> void allocateNonleafNode(Page* &page, T_nonleafFormat* &node);
	

   /*
    * This is a method that convert a page to leaf node
	*
	* Note that we use template here because we may dealing with different node format
	* 
	* @param page			The page we would convert
	*
	*/
	template <class T_leafFormat> void allocateLeafNode(Page* &page, T_leafFormat* &node);

  /*
   * This method will be called recursively in order to find a leaf node that contains
   * the lowVal
   *
   * Note that several types are passed in this template function, since there will be several situations
   *
   * @param lowVal			The value we are locating
   * @param nonleafNode		The non-leaf node (if it is) we are looking at
   * @param leafNode		The leaf node (if it is) we are looking at
   *
   * @return the leaf node we found
   */
	template <class T_type, class T_leafFormat, class T_nonleafFormat> T_leafFormat* scan(T_type lowVal, T_nonleafFormat* nonleafNode, T_leafFormat* leafNode);

	template <class T_leafFormat, class T_type> void getNextRId(RecordId& outRid, T_type highVal);

  /*
   * This method compares two keys and return the comparison result
   *
   * @param key1			The first key we are going to compare
   * @param key2			The second key we are going to compare
   * 
   * @return 0 if key1 <= key2, 1 otherwise
   */
	int compareKeys(void* key1, void* key2);

  /*
   * This method compares two constant keys and return the comparison result
   *
   * @param key1			The first key we are going to compare
   * @param key2			The second key we are going to compare
   * 
   * @return 0 if key1 <= key2, 1 otherwise
   */
	int compareConstKeys(const void* key1, const void* key2);
	
   /*
   * This method compares two keys with an operator. The operator
   * describes the expected relationship between key1 and key2, and
   * if this relationship is obtained, return true.
   *
   * @param key1			The first key we are going to compare
   * @param key2			The second key we are going to compare
   * @param op				The operator provided to describe the relationship
   * 
   * @return 1 if key1 op key2, 0 otherwise. 
   *        (i.e. if op == LT, return 1 if key1 < key2, 0 otherwise)
   *        (i.e. if op == LTE, return 1 if key1 <= key2, 0 otherwise)
   *        (i.e. if op == GT, return 1 if key1 > key2, 0 otherwise)
   *        (i.e. if op == GTE, return 1 if key1 >= key2, 0 otherwise)
   */
	int compareKeysWithOperator(void* key1, void* key2, Operator op);

 public:

  /**
   * BTreeIndex Constructor. 
	 * Check to see if the corresponding index file exists. If so, open the file.
	 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager Instance
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType						Datatype of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
   */
	BTreeIndex(const std::string & relationName, std::string & outIndexName,
						BufMgr *bufMgrIn,	const int attrByteOffset,	const Datatype attrType);
	

  /**
   * BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
	 * */
	~BTreeIndex();


  /**
	 * Insert a new entry using the pair <value,rid>. 
	 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
	 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
	 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
	 * Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char string
   * @param rid			Record ID of a record whose entry is getting inserted into the index.
	**/
	const void insertEntry(const void* key, const RecordId rid);


  /**
	 * Begin a filtered scan of the index.  For instance, if the method is called 
	 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
	 * greater than "a" and less than or equal to "d".
	 * If another scan is already executing, that needs to be ended here.
	 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
	 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
   * @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	**/
	const void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);


  /**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	**/
	const void scanNext(RecordId& outRid);  // returned record id


  /**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
	const void endScan();
	
};

}
