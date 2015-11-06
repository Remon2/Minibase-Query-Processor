package query;

import global.Minibase;
import global.SortKey;
import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

	String[] columns;
	String[] tables;
	Predicate[][] preds;
	SortKey[] orders;
	Schema[] schema;
	Iterator[] iterators;
	Schema allSchemas;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if validation fails
	 */
	public Select(AST_Select tree) throws QueryException {
		columns = tree.getColumns();
		tables = tree.getTables();
		preds = tree.getPredicates();
		orders = tree.getOrders();
		allSchemas = new Schema(0);
		iterators = new Iterator[tables.length];

		for (int i = 0; i < tables.length; i++)
			QueryCheck.tableExists(tables[i]);

		for (int i = 0; i < tables.length; i++) {
			allSchemas = Schema.join(allSchemas,
					Minibase.SystemCatalog.getSchema(tables[i]));
		}

		for (int i = 0; i < columns.length; i++) {
			QueryCheck.columnExists(allSchemas, columns[i]);

		}

		for (int i = 0; i < preds.length; i++) {
			for (int j = 0; j < preds[i].length; j++) {
				Predicate p = preds[i][j];
				if (!p.validate(allSchemas))
					throw new QueryException("The Predicate" + p.toString()
							+ " doesn't match ");
			}
		}

		for (int i = 0; i < tables.length; i++) {
			iterators[i] = new FileScan(
					Minibase.SystemCatalog.getSchema(tables[i]), new HeapFile(
							tables[i]));
		}

	}// public Select(AST_Select tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */

	public void execute() {
		String[] belong = new String[preds.length];
		for (int t = 0; t < tables.length; t++) {
			for (int i = 0; i < preds.length; i++) {
				boolean ok = true;
				for (int j = 0; j < preds[i].length; j++) {
					ok &= preds[i][j].validate(global.Minibase.SystemCatalog
							.getSchema(tables[t]));
					if (!ok)
						break;
				}
				if (ok) {
					belong[i] = t + "";
				}
			}
		}

		for (int t = 0; t < tables.length; t++) {
			Predicate[][] currentPreds = new Predicate[preds.length][];
			int index = 0;
			boolean repeat = false;
			for (int i = 0; i < belong.length; i++) {

				if (belong[i] != null) {
					if (Integer.parseInt(belong[i]) == t) {
						currentPreds[index] = preds[i];
						index++;
						repeat = true;

					}
				}
			}
			if (repeat) {
				iterators[t] = new Selection(iterators[t], currentPreds);
				while (iterators[t].hasNext())
					iterators[t].getNext();
			}
		}
		for (int i = 0; i < iterators.length; i++) {
			iterators[i].restart();
		}
		// start doing left deep joins, note further optimizations could be done
		// by reordering the array tableIters according to some criteria
		Schema current = iterators[0].getSchema();
		Iterator currentT = iterators[0];
		for (int i = 1; i < iterators.length; i++) {
			int size = 0;
			Schema next = Schema.join(current, iterators[i].getSchema());
			for (int j = 0; j < preds.length; j++) {
				if (belong[j] == null) {
					boolean matched = true;
					for (Predicate p : preds[j])
						matched &= p.validate(next);
					if (matched) {
						belong[j] = "0" + i;
						size++;
					}
				}
			}
			Predicate[][] joinPreds = new Predicate[size][];
			int index = 0;
			for (int i2 = 0; i2 < belong.length && belong[i2] != null; i2++) {
				String s = "0" + i;
				if (belong[i2].contains(s)) {
					joinPreds[index] = preds[i2];
					index++;
				}
			}
			current = next;
			currentT = new SimpleJoin(currentT, iterators[i], joinPreds);
		}
		// if not select *
		if (columns.length > 0) {
			// Do the projection
			Integer[] fields = new Integer[columns.length];
			for (int i = 0; i < columns.length; i++)
				fields[i] = current.fieldNumber(columns[i]);
			currentT = new Projection(currentT, fields);
		}
		currentT.execute();// print out the result;
		currentT.close();
	}
} // class Select implements Plan
