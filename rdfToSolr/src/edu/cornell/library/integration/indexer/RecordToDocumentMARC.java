package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.fieldMaker.*;
import edu.cornell.library.integration.indexer.resultSetToFields.*;

/**
 * An example RecordToDocument implementation. 
 */
public class RecordToDocumentMARC extends RecordToDocumentBase {

	
	
	@Override
	List<DocumentPostProcess> getDocumentPostProcess() {
		// TODO Auto-generated method stub
		return super.getDocumentPostProcess();
	}

	@Override
	List<? extends FieldMaker> getFieldMakers() {
		return Arrays.asList(
				 	
				new SPARQLFieldMakerImpl().
				    setName("publication_date").
				    addMainStoreQuery("machine_dates",
				    		"SELECT (SUBSTR(?val,8,4) as ?date1) (SUBSTR(?val,12,4) AS ?date2) \n" +
				    		"WHERE { $recordURI$ marcrdf:hasField ?f. \n" +
				    		"        ?f marcrdf:tag \"008\". \n" +
				    		"        ?f marcrdf:value ?val } \n" ).
				    addMainStoreQuery("human_dates",
				    		"SELECT ?date \n" +
				    		"WHERE { $recordURI$ marcrdf:hasField ?f. \n" +
				    		"        {?f marcrdf:tag \"260\"} UNION {?f marcrdf:tag \"264\"} \n" +
				    		"        ?f marcrdf:hasSubfield ?s. \n" +
				    		"        ?s marcrdf:code \"c\". \n" +
				    		"        ?s marcrdf:value ?date } ").
				    addResultSetToFields( new DateResultSetToFields() ) ,
				    
				new SPARQLFieldMakerImpl().
					setName("id").
					addMainStoreQuery("bib_id","SELECT ?id WHERE { $recordURI$ rdfs:label ?id}").
					addResultSetToFields( new AllResultsToField("id") ),
				    
				new SubfieldCodeMaker().
					setSubfieldCodes("a").
					setMarcFieldNumber("245").
					setSolrFieldName("title_display")	,			

				new SubfieldCodeMaker().
					setSubfieldCodes("abcdefghijklmnopqrstuvwxyz").
					setMarcFieldNumber("130").
					setSolrFieldName("title_addl_t"),				
				new SubfieldCodeMaker().
					setSubfieldCodes("ab").
					setMarcFieldNumber("210").
					setSolrFieldName("title_addl_t"),				
			    new SubfieldCodeMaker().
					setSubfieldCodes("ab").
					setMarcFieldNumber("222").
					setSolrFieldName("title_addl_t"),
				new SubfieldCodeMaker().
					setSubfieldCodes("abcdefgklmnopqrs").
					setMarcFieldNumber("240").
					setSolrFieldName("title_addl_t"),
				new SubfieldCodeMaker().
					setSubfieldCodes("abnp").
					setMarcFieldNumber("242").
					setSolrFieldName("title_addl_t"),
				new SubfieldCodeMaker().
					setSubfieldCodes("abcdefgklmnopqrs").
					setMarcFieldNumber("243").
					setSolrFieldName("title_addl_t"),
   	            new SubfieldCodeMaker().
					setSubfieldCodes("abnps").
					setMarcFieldNumber("245").
					setSolrFieldName("title_addl_t"),
				new SubfieldCodeMaker().
					setSubfieldCodes("abcdefgklmnopqrs").
					setMarcFieldNumber("246").
					setSolrFieldName("title_addl_t"),
				new SubfieldCodeMaker().
					setSubfieldCodes("abcdefgnp").
					setMarcFieldNumber("247").
					setSolrFieldName("title_addl_t"),
	
				new SubfieldCodeMaker().
					setSubfieldCodes("gklmnoprst").
					setMarcFieldNumber("700").
					setSolrFieldName("title_added_entry_t"),
				new SubfieldCodeMaker().
					setSubfieldCodes("fgklmnopqrst").
					setMarcFieldNumber("710").
					setSolrFieldName("title_added_entry_t"),
				new SubfieldCodeMaker().
					setSubfieldCodes("fgklnpst").
					setMarcFieldNumber("711").
					setSolrFieldName("title_added_entry_t"),
				new SubfieldCodeMaker().
					setSubfieldCodes("abcdefgklmnopqrst").
					setMarcFieldNumber("730").
					setSolrFieldName("title_added_entry_t"),
				new SubfieldCodeMaker().
					setSubfieldCodes("anp").
					setMarcFieldNumber("740").
					setSolrFieldName("title_added_entry_t")
	

					
		);
	}

}
