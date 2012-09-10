package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

public class RecordToDocumentTest1 extends RecordToDocumentBase {


	@Override
	List<? extends FieldMaker> getFieldMakers() {
		return Arrays.asList(
				
				new SPARQLFieldMakerImpl().					
					setName("fieldMakerTest1.field1"). 
					addMainStoreQuery( "query1", "SELECT * WHERE { $recordURI$ ?p ?o }").
					setResultSetToFields( new AllResultsToField("solrField1") ) ,						
				
				new SPARQLFieldMakerImpl().					
				 	setName("fieldMakerTest1.field2").
				 	addMainStoreQuery( "query2", "SELECT * WHERE { $recordURI$ ?p ?o }").
				 	setResultSetToFields( new AllResultsToField( "solrField2") )			
		);
	}

}
