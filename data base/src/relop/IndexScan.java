package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
	private BucketScan bucketScan;
	private boolean opened;
	private HashIndex index;
	private HeapFile myFile;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public IndexScan(Schema schema, HashIndex index, HeapFile file) {

		this.setSchema(schema);
		this.index = index;
		this.bucketScan = this.index.openScan();
		this.opened = true;
		this.myFile = file;
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("Index Scan Iterator : ");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		if (bucketScan != null) {
			bucketScan.close();
			bucketScan = index.openScan();
		} else {
			throw new UnsupportedOperationException("Bucket Scan is null");
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
		if (bucketScan != null) {
			bucketScan.close();
			bucketScan = null;
			opened = false;
			index = null;
		} else {
			throw new UnsupportedOperationException("Bucket Scan is null");
		}
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (bucketScan != null) {
			return bucketScan.hasNext();
		} else {
			throw new UnsupportedOperationException("Bucket Scan is null");
		}
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (bucketScan != null) {
			RID rid = bucketScan.getNext();
			byte[] record = myFile.selectRecord(rid);
			if (record == null)
				throw new IllegalStateException("No more tuples in the heap file");
			Tuple tuple = new Tuple(this.getSchema(), record);
			return tuple;
		} else {
			throw new UnsupportedOperationException("Bucket Scan is null");
		}
	}

	/**
	 * Gets the key of the last tuple returned.
	 */
	public SearchKey getLastKey() {
		if (bucketScan != null) {
			SearchKey key = bucketScan.getLastKey();
			if (key == null)
				throw new UnsupportedOperationException("Bucket Scan is empty");
			else
				return key;
		} else {
			throw new UnsupportedOperationException("Bucket Scan is null");
		}
	}

	/**
	 * Returns the hash value for the bucket containing the next tuple, or
	 * maximum number of buckets if none.
	 */
	public int getNextHash() {
		if (bucketScan != null) {
			int hashValue = bucketScan.getNextHash();
			return hashValue;
		} else {
			throw new UnsupportedOperationException("Bucket Scan is null");
		}
	}

} // public class IndexScan extends Iterator
