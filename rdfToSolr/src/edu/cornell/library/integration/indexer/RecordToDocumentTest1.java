package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

public class RecordToDocumentTest1 extends RecordToDocumentBase {


	@Override
	List<? extends FieldMaker> getFieldMakers() {
		return Arrays.asList(
				
				new SPARQLFieldMakerImpl().					
				setName("fieldMakerTest1.field1").
				addMainStoreQueries( "query1", "SELECT * WHERE { bla bla }").
				setResultSetToFields( new AllResultsToField("f1") ) ,						
				
				new SPARQLFieldMakerImpl().					
				setName("fieldMakerTest1.field2").
				addMainStoreQueries( "query2", "SELECT * WHERE {Bla bla bla}").
				setResultSetToFields( new AllResultsToField( "f2") )			
		);
	}

}
