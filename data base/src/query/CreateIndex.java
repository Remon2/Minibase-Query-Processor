package query;

import heap.HeapFile;
import index.HashIndex;
import global.Minibase;
import global.SearchKey;
import parser.AST_CreateIndex;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

	//Name of the index which we want to create.
	protected String fileName;

	//Name of the table which we want to create index on it.
	protected String indexTable;

	//Name of the column to index.
	protected String indexColumn;
	protected Schema schema;
	
	
	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
	  //Get the name of the index file.
	  fileName = tree.getFileName();
	  
	  //Get the table name which we want to create index on it.
	  indexTable = tree.getIxTable();
	  
	  //Get the column name which we want to select it as an index.
	  indexColumn = tree.getIxColumn();
	  
	  //Check if the table exists.
	  QueryCheck.tableExists(indexTable);
	  
	  //Check if the index file is not exist.
	  QueryCheck.fileNotExists(fileName);
	  
	  //Get the schema of the index table.
	  schema=Minibase.SystemCatalog.getSchema(indexTable);
	  
	  //Check if the column that we want to select it as an index exist. 
	  QueryCheck.columnExists(schema, indexColumn);
	  
	  IndexDesc []indexDesc=Minibase.SystemCatalog.getIndexes(indexTable);
		for (int i = 0; i < indexDesc.length; i++) {
			if(indexDesc[i].columnName.endsWith(indexColumn))
				throw new QueryException("This column already has an index,\nthere's no point of creating new one!!!");
		}
  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  	/**
	 * Executes the plan and prints applicable output.
	 */
  public void execute() {
		HashIndex hashIndex = new HashIndex(fileName);
		FileScan fileScan = new FileScan(schema, new HeapFile(indexTable));
		int fieldNumber = schema.fieldNumber(indexColumn);
		while (fileScan.hasNext()) {
			Tuple tuple = fileScan.getNext();
			hashIndex.insertEntry(new SearchKey(tuple.getField(fieldNumber)),fileScan.getLastRID());
		}
		fileScan.close();
		Minibase.SystemCatalog.createIndex(fileName, indexTable, indexColumn);
		System.out.println("Index " + fileName + " Created on table "+ indexTable + " Column" + indexColumn);
  } // public void execute()

} // class CreateIndex implements Plan
