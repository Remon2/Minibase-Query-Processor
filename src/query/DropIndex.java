package query;

import global.Minibase;
import index.HashIndex;
import parser.AST_DropIndex;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {

	// Name of the index which want to delete.
	protected String fileName;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if index doesn't exist
	 */
	public DropIndex(AST_DropIndex tree) throws QueryException {
		fileName = tree.getFileName();
		
		// Check if the index file is exist.
		QueryCheck.indexExists(fileName);
	} // public DropIndex(AST_DropIndex tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		HashIndex hashIndex=new HashIndex(fileName);
		hashIndex.deleteFile();
		Minibase.SystemCatalog.dropIndex(fileName);

		// print the output message
		System.out.println("Index dropped!");
	} // public void execute()

} // class DropIndex implements Plan
