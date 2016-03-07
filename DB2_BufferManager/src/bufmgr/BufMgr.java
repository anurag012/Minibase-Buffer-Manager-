package bufmgr;

import global.GlobalConst;
import diskmgr.DiskMgr;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.Arrays;
import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager reads disk pages into a main memory page as needed. The
 * collection of main memory pages (called frames) used by the buffer manager
 * for this purpose is called the buffer pool. This is just an array of Page
 * objects. The buffer manager is used by access methods, heap files, and
 * relational operators to read, write, allocate, and de-allocate pages.
 */
public class BufMgr implements GlobalConst {
	
    /** Actual pool of pages (can be viewed as an array of byte arrays). */
    protected Page[] bufpool;

    /** Array of descriptors, each containing the pin count, dirty status, etc\
	. */
    protected FrameDesc[] frametab;

    /** Maps current page numbers to frames; used for efficient lookups. */
    protected HashMap<Integer, FrameDesc> pagemap;

    /** The replacement policy to use. */
    protected Replacer replacer;
//-------------------------------------------------------------



  /**
   * Constructs a buffer mamanger with the given settings.
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * @param numbufs number of buffers in the buffer pool
   */
  public BufMgr(int numbufs) {

	  bufpool = new Page[numbufs];
	  frametab = new FrameDesc[numbufs];
	  for(int i = 0; i<numbufs;i++)
	  {
		  bufpool[i] = new Page();
		  frametab[i] = new FrameDesc(i);
	  }
	  pagemap = new HashMap<Integer, FrameDesc>(numbufs);
	  replacer = new Clock(this);

  }

  /**
   * Allocates a set of new pages, and pins the first one in an appropriate
   * frame in the buffer pool.
   * 
   * @param firstpg holds the contents of the first page
   * @param run_size number of pages to allocate
   * @return page id of the first new page
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */ 

  public PageId newPage(Page firstpg, int run_size) throws IllegalArgumentException, IllegalStateException {
	PageId firstPage = Minibase.DiskManager.allocate_page(run_size);
	try {
		pinPage(firstPage,firstpg,true);
	} catch(Exception e){
		for (int i=0;i<run_size;i++){
			firstPage.pid +=i;
			Minibase.DiskManager.deallocate_page(firstPage);
		}
		throw e;
	}
	replacer.newPage((FrameDesc)pagemap.get(Integer.valueOf(firstPage.pid)));
	return firstPage;
  }


/**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) throws IllegalArgumentException{
	FrameDesc fdesc = (FrameDesc)pagemap.get(Integer.valueOf(pageno.pid));
	if(fdesc!=null){
		if(fdesc.pincnt==12){
			throw new IllegalArgumentException();
		}
		pagemap.remove(Integer.valueOf(pageno.pid));
		fdesc.pageno.pid=-1;
		fdesc.pincnt=0;
		fdesc.dirty=false;
		replacer.freePage(fdesc);
	}
	Minibase.DiskManager.deallocate_page(pageno);

  }

  /**
   * Pins a disk page into the buffer pool. If the page is already pinned, this
   * simply increments the pin count. Otherwise, this selects another page in
   * the pool to replace, flushing it to disk if dirty.
   * 
   * @param pageno identifies the page to pin
   * @param page holds contents of the page, either an input or output param
   * @param skipRead PIN_MEMCPY (replace in pool); PIN_DISKIO (read the page in)
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public void pinPage(PageId pageno, Page page, boolean skipRead)throws IllegalArgumentException, IllegalStateException {
	  
		FrameDesc fdesc = (FrameDesc)pagemap.get(Integer.valueOf(pageno.pid));
		if(fdesc!=null)
			if(PIN_DISKIO){
				throw new IllegalArgumentException();
			} 
			else {
				fdesc.pincnt++;
				replacer.pinPage(fdesc);
				page.setPage(bufpool[fdesc.index]);
				return;
			}
		int replace = replacer.pickVictim();
		if(replace<0)
			throw new IllegalStateException();
		fdesc = frametab[replace];
		if(fdesc.pageno.pid!=-1){
			pagemap.remove(Integer.valueOf(pageno.pid));
			if(fdesc.dirty)
				Minibase.DiskManager.write_page(fdesc.pageno, bufpool[replace]);
		}
		if(PIN_MEMCPY)
			bufpool[replace].copyPage(page);
		else
			Minibase.DiskManager.read_page(pageno, bufpool[replace]);
		page.setPage(bufpool[replace]);
		fdesc.pageno.pid=pageno.pid;
		fdesc.pincnt=1;
		fdesc.dirty=false;
		pagemap.put(Integer.valueOf(pageno.pid), fdesc);
		replacer.pinPage(fdesc);
			
		}

  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherrwise
   * @throws IllegalArgumentException if the page is not present or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) throws IllegalArgumentException {
	 
		FrameDesc fdesc = (FrameDesc)pagemap.get(Integer.valueOf(pageno.pid));
		if(fdesc==null || fdesc.pincnt==0)
			throw new IllegalArgumentException();
		else {
			
			fdesc.pincnt--;
			fdesc.dirty=dirty;
			replacer.unpinPage(fdesc);
			return;
		}

  }

  /**
   * Immediately writes a page in the buffer pool to disk, if dirty.
   */
  public void flushPage(PageId pageno) throws IllegalArgumentException {
	
	  for(int i=0; i<bufpool.length;i++)
		  if(frametab[i].pageno.pid==pageno.pid&&frametab[i].dirty){
			  Minibase.DiskManager.write_page(frametab[i].pageno, bufpool[i]);
			  frametab[i].dirty=false;
		  }

  }

  /**
   * Immediately writes all dirty pages in the buffer pool to disk.
   */
  public void flushAllPages() {
	  for(int i=0; i<bufpool.length;i++){
		  if(frametab[i].pageno==null&&frametab[i].dirty){
			  Minibase.DiskManager.write_page(frametab[i].pageno, bufpool[i]);
			  frametab[i].dirty=false;
		  } else
			  break;
		  }

  }

  /**
   * Gets the total number of buffer frames.
   */
  public int getNumBuffers() { 
	  return bufpool.length;
   
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {
	  int counter=0;
	  for (int i=0; i<bufpool.length;i++){
		  if( frametab[i].pincnt==0){
			  counter++;
		  }
	  }
	  return counter;
  
  }

} // public class BufMgr implements GlobalConst
	