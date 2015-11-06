package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

	private HeapScan heapScan;
	private boolean opened;
	private RID currentRid = null;
	private HeapFile myFile;

	/**
	 * Constructs a file scan, given the schema and heap file.
	 */

	public FileScan(Schema schema, HeapFile file) {
		this.heapScan = file.openScan();
		this.setSchema(schema);
		this.opened = true;
		this.myFile = file;

	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("File Scan Iterator : ");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		if (heapScan != null) {
			heapScan.close();
			heapScan = myFile.openScan();
		}
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return opened;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		if (heapScan != null) {
			heapScan.close();
			heapScan = null;
			opened = false;
			currentRid = null;
		}
		
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (heapScan != null) {
			return heapScan.hasNext();
		} else {
			throw new UnsupportedOperationException("Heap Scan is null");
		}
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (heapScan != null) {
			currentRid = new RID();
			byte[] record = heapScan.getNext(currentRid);
			if (record == null)
				throw new IllegalStateException("No more tuples in the heap file");
			Tuple tuple = new Tuple(this.getSchema(), record);
			return tuple;
		} else {
			throw new UnsupportedOperationException("Heap Scan is null");
		}
	}

	/**
	 * Gets the RID of the last tuple returned.
	 */
	public RID getLastRID() {
		return currentRid;
	}

} // public class FileScan extends Iterator
