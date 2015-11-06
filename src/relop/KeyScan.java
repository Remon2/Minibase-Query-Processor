package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
	private HashScan hashScan;
	private boolean opened;
	private HashIndex index;
	private SearchKey key;
	private HeapFile myFile;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
		this.key = key;
		this.index = index;
		this.hashScan = this.index.openScan(key);
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
		System.out.println("Key Scan Iterator : ");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		if (hashScan != null) {
			hashScan.close();
			hashScan = index.openScan(key);
		} else {
			throw new UnsupportedOperationException("Hash Scan is null");
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
		if (hashScan != null) {
			hashScan.close();
			hashScan = null;
			opened = false;
			key = null;
			index = null;
		} else {
			throw new UnsupportedOperationException("Hash Scan is null");
		}
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (hashScan != null) {
			boolean next = hashScan.hasNext();
			return  next;
		} else {
			throw new UnsupportedOperationException("Hash Scan is null");
		}
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (hashScan != null) {
			RID rid = hashScan.getNext();
			byte[] record = myFile.selectRecord(rid);
			if (record == null)
				throw new IllegalStateException("No more tuples in the heap file");
			Tuple tuple = new Tuple(this.getSchema(), record);
			return tuple;
		} else {
			throw new UnsupportedOperationException("Hash Scan is null");
		}
	}

} // public class KeyScan extends Iterator
