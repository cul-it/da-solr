package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;
import edu.cornell.library.integration.indexer.fieldMaker.SubfieldCodeMaker;
import edu.cornell.library.integration.indexer.resultSetToFields.AllResultsToField;
import edu.cornell.library.integration.indexer.resultSetToFields.ExampleResultSetToFields;

/**
 * An example RecordToDocument implementation. 
 */
public class RecordToDocumentTest1 extends RecordToDocumentBase {	
	
	@Override
	List<? extends FieldMaker> getFieldMakers() {
		String badquery = 
		"PREFIX marcrdf:  <http://marcrdf.library.cornell.edu/canonical/0.1/> \n"+
	    "SELECT (SUBSTR(?val,8,4) as ?date1) (SUBSTR(?val,12,4) AS ?date2)    \n"+ 
	    "WHERE { <http://fbw4-dev.library.cornell.edu/individuals/b4722> marcrdf:hasField ?f. \n"+ 
	    "        ?f marcrdf:tag \"008\".\n"+
	    "        ?f marcrdf:value ?val }  ";

		
		return Arrays.asList(
				
				new SubfieldCodeMaker("title_display","245","a"),
				
				new SPARQLFieldMakerImpl().					
					setName("fieldMakerTest1.field1"). 
					addMainStoreQuery( "query1", "SELECT * WHERE { $recordURI$ ?p ?o }").
					addResultSetToFields( new AllResultsToField("id") ) 
				
//				new SPARQLFieldMakerImpl().					
//					setName("fieldMakerTest1.field0"). 
//					addMainStoreQuery( "badquery", badquery).
//					addResultSetToFields( new AllResultsToField("solrField0_t") ) ,								
//				
//				new SPARQLFieldMakerImpl().					
//					setName("fieldMakerTest1.field1"). 
//					addMainStoreQuery( "query1", "SELECT * WHERE { $recordURI$ ?p ?o }").
//					addResultSetToFields( new AllResultsToField("solrField1_t") ) ,						
//				
//				new SPARQLFieldMakerImpl().					
//				 	setName("fieldMakerTest1.field2").
//				 	addMainStoreQuery( "query2", "SELECT * WHERE { $recordURI$ ?p ?o }").
//				 	addResultSetToFields( new ExampleResultSetToFields() ) ,
				
//				new SubfieldCodeMaker("050","abcde","solr_field_name_for_050_t")					
				 	
		);
	}

}
