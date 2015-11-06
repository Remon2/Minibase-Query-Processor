package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by AND operators.
 */
public class Selection extends Iterator {
	private Iterator iter = null;
	private Predicate[][] preds;
	private boolean opened = false;
	private Tuple tuple = null;

	/**
	 * Constructs a selection, given the underlying iterator and predicates.
	 */
	public Selection(Iterator iter, Predicate... preds) {
		// throw new UnsupportedOperationException("Not implemented");
		this.iter = iter;

		this.preds = new Predicate[preds.length][1];
		for (int i = 0; i < preds.length; i++)
			this.preds[i][0] = preds[i];

		opened = true;
		tuple = null;
		this.setSchema(iter.getSchema());

	}

	public Selection(Iterator iter2, Predicate[][] currentPreds) {
		this.iter = iter2;
		this.preds = currentPreds;
		opened = true;
		tuple = null;
		this.setSchema(iter2.getSchema());
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// throw new UnsupportedOperationException("Not implemented");
		indent(depth);
		System.out.println("Selection : ");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		// throw new UnsupportedOperationException("Not implemented");
		iter.restart();
		tuple = null;
		opened = true;

	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		// throw new UnsupportedOperationException("Not implemented");
		return opened;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		// throw new UnsupportedOperationException("Not implemented");
		iter.close();
		tuple = null;
		opened = false;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (tuple != null) {
			return true;
		}
		while (iter.hasNext()) {
			boolean checkAvailability = false;
			Tuple tupleTemp = iter.getNext();
			for (int i = 0; i < preds.length&&preds[i]!=null; i++) {
				checkAvailability = false;
				
				for (int j = 0; j < preds[i].length&&preds[i][j]!=null; j++) {
					checkAvailability = preds[i][j].evaluate(tupleTemp);
					if (checkAvailability) {
						break;
					}
				}
				if (!checkAvailability) {
					break;
				}
			}
			if (checkAvailability) {
				tuple = tupleTemp;
				return true;
			}

		}
		return false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 *
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		// throw new UnsupportedOperationException("Not implemented");
		if (tuple != null) {
			Tuple foundTuple = tuple;
			tuple = null;
			return foundTuple;
		}
		if (tuple == null) {
			while (iter.hasNext()) {
				Tuple foundTuple = iter.getNext();
				boolean checkAvailability = false;
				for (int i = 0; i < preds.length&&preds[i]!=null; i++) {
					checkAvailability = false;
					for (int j = 0; j < preds[i].length; j++) {
						checkAvailability = preds[i][j].evaluate(tuple);
						if (checkAvailability) {
							break;
						}
					}
					if (!checkAvailability) {
						break;
					}
				}
				if (checkAvailability) {
					tuple = null;
					return foundTuple;
				}

			}
		}
		return null;
	}

} // public class Selection extends Iterator