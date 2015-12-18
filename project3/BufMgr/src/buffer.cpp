/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

using namespace std;
namespace badgerdb { 

	BufMgr::BufMgr(std::uint32_t bufs)
		: numBufs(bufs) {
			bufDescTable = new BufDesc[bufs];

			for (FrameId i = 0; i < bufs; i++) 
			{
				bufDescTable[i].frameNo = i;
				bufDescTable[i].valid = false;
			}

			bufPool = new Page[bufs];

			int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
			hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

			clockHand = bufs - 1;
		}


	BufMgr::~BufMgr() {
		for(uint32_t i = 0; i < numBufs; i++){
			BufDesc b = bufDescTable[i];
			if (b.dirty && b.valid) {
				b.file->writePage(bufPool[b.frameNo]);
			}
		}	
		delete [] bufPool;
		delete [] bufDescTable;
		delete hashTable;
	}

	void BufMgr::advanceClock()
	{
		clockHand = (clockHand+1)%numBufs;	
	}

	void BufMgr::allocBuf(FrameId & frame) 
	{
		uint32_t num_pinned = 0;
		uint32_t orig_clockHand = clockHand;
		bool found = false;
		BufDesc b;

		advanceClock();

		// exit the loop if we found a good frame to use
		// or all frames are already pinned
		while (num_pinned < numBufs && !found) {
			// need to clear pinned counter if we cycle back to origin
			if (clockHand == orig_clockHand) {
				num_pinned = 0;
			}
			b = bufDescTable[clockHand];
			// if the frame is invalid, just use it
			if (!b.valid) {
				frame = b.frameNo;
				found = true;
			}
			// if the frame is valid, but the refbit is set
			// clear refbit and advance clock hand
			else if (b.refbit) {
				bufDescTable[clockHand].refbit = false;
				advanceClock();
			}
			// if the frame is valid and refbit is not set, but the pin count is not 0
			// the frame is currently pinned so cannot be used, advance clock hand
			// also increment counter for total # of pinned frames
			else if (b.pinCnt > 0) {
				advanceClock();
				num_pinned++;
			}
			// the frame is valid, and neither referenced nor pinned
			// use this frame, write it to disk if dirty bit is set, then clear the frame for our use
			else {
				if (b.dirty) {
					// flush page to disk
					bufDescTable[clockHand].file->writePage(bufPool[b.frameNo]);
				}
				// remove the frame from hash table
				hashTable->remove(b.file, b.pageNo);
				// clear the frame in buffer
				bufDescTable[clockHand].Clear();
				// return the frame number
				frame = b.frameNo;
				found = true;
			}
		}
		// not found means the buffer is full
		if (!found)
			throw BufferExceededException();
	}


	void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
	{
		FrameId frameNo;
		try {
			// look up the desired page in hashtable
			hashTable->lookup(file, pageNo, frameNo);
			// found the page in hash table, set refbit to true
			bufDescTable[frameNo].refbit = true;
			// increment pin count
			bufDescTable[frameNo].pinCnt++;
			// return the page by reference     
			page = &bufPool[frameNo];
		}
		catch (HashNotFoundException h) {
			// the page is not in hashtable, which indicates a buffer miss
			// so we need to read from the disk
			Page p = file->readPage(pageNo);
			// allocate a buffer frame to place the page
			// ATTENTION: this line may throw BufferExceededException
			allocBuf(frameNo);
			// add the page to buffer pool
			bufPool[frameNo] = p;
			// insert a record in hash table
			hashTable->insert(file, pageNo, frameNo);
			// set the appropriate frame attributes (pinCnt=1, valid=1, refbit=1, dirty=0)
			bufDescTable[frameNo].Set(file, pageNo);
			// return the page by reference
			page = &bufPool[frameNo];
		}
							
	}

	void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {
		FrameId frameNo;
		// look up hashtable
		// ATTENTION: this line might throw BufferExceededException
		hashTable->lookup(file, pageNo, frameNo);
		// find this frame in bufDescTable
		BufDesc frame = bufDescTable[frameNo];
		// if this page is already unpinned, throw exception
		if (frame.pinCnt <= 0) 
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		// set the dirty bit if we need to
		if (dirty) 
			bufDescTable[frameNo].dirty = true;
		// decrement pin count
		bufDescTable[frameNo].pinCnt--;	
	}

	void BufMgr::flushFile(const File* file){
		// traverse the buffer
		for(uint32_t i = 0; i < numBufs; i++){
			BufDesc frame = bufDescTable[i];
			// if the current frame belongs to the desired file
			if (frame.file == file ) {
				// if the frame is not valid, throw BadBufferException
				if (!frame.valid) 
					throw BadBufferException(frame.frameNo, frame.dirty, frame.valid, frame.refbit);
				// if the frame is pinned, throw PagePinnedException
				if (frame.pinCnt > 0) 
					throw PagePinnedException(file->filename(), frame.pageNo, frame.frameNo);
				// if the frame is dirty, write it to disk, then set dirty to false
				if (frame.dirty) {
					Page p = bufPool[frame.frameNo];
					bufDescTable[i].file->writePage(p);
					bufDescTable[i].dirty = false;
				}
				// remove the page from hashtable
				hashTable->remove(file, frame.pageNo);
				// clear the buffer frame
				bufDescTable[i].Clear();
			} 
		}

	}

	void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
	{	
		FrameId frameNo;
		// allocate a new page in file
		Page p = file->allocatePage();
		// allocate a buffer frame
		// ATTENTION: this line might throw BufferExceededException
		allocBuf(frameNo);
		// put the new page in buffer
		bufPool[frameNo] = p;
		// insert a new entry in hashtable
		hashTable->insert(file, p.page_number(), frameNo);
		// call Set()
		bufDescTable[frameNo].Set(file, p.page_number());
		// set return values
		pageNo = p.page_number();
		page = &bufPool[frameNo];
	}

	void BufMgr::disposePage(File* file, const PageId PageNo)
	{
		FrameId frameNo;
		// try to find the page in hash table
		// ATTENTION: this line might throw HashNotFoundException
		hashTable->lookup(file,PageNo,frameNo);
		// remove the hash table record
		hashTable->remove(file,PageNo);
		// clear the corresponding buffer frame
		bufDescTable[frameNo].Clear();
		// delete this page on disk
		file->deletePage(PageNo);
	}

	void BufMgr::printSelf(void) 
	{
		BufDesc* tmpbuf;
		int validFrames = 0;

		for (std::uint32_t i = 0; i < numBufs; i++)
		{
			tmpbuf = &(bufDescTable[i]);
			std::cout << "FrameNo:" << i << " ";
			tmpbuf->Print();

			if (tmpbuf->valid == true)
				validFrames++;
		}

		std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
	}

}
