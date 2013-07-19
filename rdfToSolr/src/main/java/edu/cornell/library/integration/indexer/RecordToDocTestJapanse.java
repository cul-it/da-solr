package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultsToStdout;

public class RecordToDocTestJapanse extends RecordToDocumentBase{
//	http://marcrdf.library.cornell.edu/canonical/0.1/hasField 	
//		http://da-rdf.library.cornell.edu/individual/b34669_27 	
//			http://marcrdf.library.cornell.edu/canonical/0.1/hasSubfield 	
//				http://da-rdf.library.cornell.edu/individual/b34669_27_4 	
//					http://marcrdf.library.cornell.edu/canonical/0.1/value 	監修池田彌三郎, ドナルド．キーン ; 編集常名鉾二郎, 朝日イブニングニュース社.


	@Override
	List<? extends FieldMaker> getFieldMakers() {
		System.out.println("id should be: 監修池田彌三郎, ドナルド．キーン ; 編集常名鉾二郎, 朝日イブニングニュース社");
		String query= 
		"PREFIX marcrdf:  <http://marcrdf.library.cornell.edu/canonical/0.1/> \n"+
	    "SELECT * WHERE {   \n"+ 
	    "  <http://da-rdf.library.cornell.edu/individual/b34669_27_4> marcrdf:value ?v " +
	    "}";
		
		return Arrays.asList(						
				
				new SPARQLFieldMakerImpl().					
					setName("fieldWithJapanese"). 
					addMainStoreQuery( "query", query).
					addResultSetToFields( new ResultsToStdout() ) 
				 	
		);
	}



}
