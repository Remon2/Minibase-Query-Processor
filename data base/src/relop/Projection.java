package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

	Iterator iter;
	Integer[] fields;

	/**
	 * Constructs a projection, given the underlying iterator and field numbers.
	 */
	public Projection(Iterator iter, Integer... fields) {
		this.iter = iter;
		this.fields = fields;
		this.setSchema(new Schema(fields.length));
		for (int i = 0; i < fields.length; i++) {
			getSchema().initField(i, iter.getSchema().types[fields[i]],
					iter.getSchema().lengths[fields[i]],
					iter.getSchema().names[fields[i]]);
		}
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		System.out.println("Projection:");
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		iter.restart();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return iter.isOpen();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		iter.close();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return iter.hasNext();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		Tuple tuple = new Tuple(getSchema());
		if (iter.hasNext()) {
			Tuple allTuple = iter.getNext();
			for (int i = 0; i < getSchema().getCount(); i++) {
				tuple.setField(i, allTuple.getField(fields[i]));
			}
		} else {
			throw new IllegalStateException("No More Tuples.");
		}

		return tuple;
		// throw new UnsupportedOperationException("Not implemented");
	}
} // public class Projection extends Iterator
