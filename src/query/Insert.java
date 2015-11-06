package query;

import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Insert;
import parser.ParseException;
import relop.Schema;
import relop.Tuple;

/**	  QueryCheck.tableExists(indexTable);

 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

	/** Name of the table to insert in. */
	protected String fileName;

	/** Schema of the table to insert in. */
	protected Schema schema;

	/** Values of the object to create. */
	protected Object[] values;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if table doesn't exists or values are invalid
	 */
	public Insert(AST_Insert tree) throws QueryException {

		fileName = tree.getFileName();

		// get and validate the requested schema and make sure the file already
		// exists
		try {
			schema = Minibase.SystemCatalog.getSchema(fileName);
		} catch (Exception e) {
			throw new QueryException(e.getMessage());
		}

		// get the values of the inserted record
		values = tree.getValues();
	} // public Insert(AST_Insert tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		//first we check whether the table exists
		  try {
			QueryCheck.tableExists(fileName);
		} catch (QueryException e1) {
			// TODO Auto-generated catch block
			return;
		}
		// second check whether the record is suitable for insertion or not
		try {
			QueryCheck.insertValues(schema, values);
		} catch (QueryException e) {
			return;
		}

		// make the record
		Tuple newTuple = new Tuple(schema, values);
		// open the heap file
		HeapFile myFile = new HeapFile(fileName);
		// get the byte data of the record
		byte[] record = newTuple.getData();
		// add the new record to the heap file
		RID rid = myFile.insertRecord(record);
		// now we want to update the list of indexes and add the new index
		// lets get all the indexes first
		IndexDesc[] desc = Minibase.SystemCatalog.getIndexes(fileName);
		for (int i = 0; i < desc.length; i++) {
			String changedColomn = desc[i].columnName;
			String changedIndex = desc[i].indexName;
			HashIndex hash = new HashIndex(changedIndex);
			SearchKey key = new SearchKey(changedColomn);
			hash.insertEntry(key, rid);
		}
		// print the output message
		System.out.println("1 row affected.");

	} // public void execute()

} // class Insert implements Plan

