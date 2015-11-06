package relop;

import java.util.Arrays;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

	private Iterator leftIterator = null;
	private Iterator rightIterator = null;
	private Predicate predicates[] = null;
	Predicate[][] cnf; //Conjunctive Normal Form
	private Tuple nextTuple = null;
	private Tuple currentLeftTuple = null;

	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
		leftIterator = left;
		rightIterator = right;
		predicates = preds;
		
		cnf = new Predicate[preds.length][1];
		for (int i = 0; i < preds.length; i++) {
			cnf[i][0] = preds[i];
		}
		
		leftIterator.restart();
		rightIterator.restart();
		// The new schema is the joining of left schema and the right schema.
		this.setSchema(Schema.join(leftIterator.getSchema(),
				rightIterator.getSchema()));
		calculateNextTuple();
	}

	public SimpleJoin(Iterator currentT, Iterator right, Predicate[][] preds) {
		leftIterator = currentT;
		rightIterator = right;
		cnf = preds;
		leftIterator.restart();
		rightIterator.restart();
		// The new schema is the joining of left schema and the right schema.
		this.setSchema(Schema.join(leftIterator.getSchema(),
				rightIterator.getSchema()));
		calculateNextTuple();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("SimpleJoin : " + Arrays.toString(predicates));
		leftIterator.explain(depth + 1);
		rightIterator.explain(depth + 1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		leftIterator.restart();
		rightIterator.restart();
		currentLeftTuple = null;
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		// check if the left and the right iterators are open.
		return leftIterator.isOpen() && rightIterator.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		leftIterator.close();
		rightIterator.close();
		currentLeftTuple = nextTuple = null;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return nextTuple != null;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (nextTuple == null || !isOpen())
			throw new IllegalStateException("No more tuples");
		Tuple toReturn = nextTuple;
		calculateNextTuple();
		return toReturn;
	}

	private void calculateNextTuple() {
		Tuple tempJoinedTuple = null;
		if (currentLeftTuple == null && leftIterator.hasNext())
			currentLeftTuple = leftIterator.getNext();
		while (currentLeftTuple != null && tempJoinedTuple == null) {
			if (!rightIterator.hasNext()) // restart iterator if we reached the
											// end
			{
				if (leftIterator.hasNext())
					currentLeftTuple = leftIterator.getNext();
				else {
					currentLeftTuple = null;
					break;
				}
				rightIterator.restart();
			}
			while (rightIterator.hasNext()) {
				// merge right and left tuples
				tempJoinedTuple = Tuple.join(currentLeftTuple,
						rightIterator.getNext(), getSchema());

//				boolean predicatesIsSatisfied = true;

				// Check for the validation of the predicates.
//				for (int i = 0; i < predicates.length; i++) {
//					if (!predicates[i].evaluate(tempJoinedTuple)) {
//						predicatesIsSatisfied = false;
//						break;
//					}
//				}

				boolean valid = true;
				for (int i = 0; i < cnf.length&&cnf[i]!=null; i++) {
					boolean validRow=false;
					for (int j = 0; j < cnf[i].length; j++) {
						validRow |= cnf[i][j].evaluate(tempJoinedTuple);
					}
					valid &= validRow;
				}
				
				if (!valid) {
					tempJoinedTuple = null; // invalid next tuple
				} else {
					// valid candidate
					nextTuple = tempJoinedTuple;
					return;
				}
			}
		}
		nextTuple = tempJoinedTuple;
	}

} // public class SimpleJoin extends Iterator
