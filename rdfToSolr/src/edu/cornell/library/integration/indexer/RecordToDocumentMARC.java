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
					setName("id").
					addMainStoreQuery("bib_id","SELECT ?id WHERE { $recordURI$ rdfs:label ?id}").
					addResultSetToFields( new AllResultsToField("id") ),
			    
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
				  
				 //pub_info does not exist in solr schema.xml, using dyanmic field
				new SubfieldCodeMaker("260", "abc", "pub_info_t"),
				new SubfieldCodeMaker("264", "abc", "pub_info_t"),
				
				new SubfieldCodeMaker("245", "a","title_display"),
				new SubfieldCodeMaker("245", "b", "subtitle_display"),
				new SubfieldCodeMaker("130", "abcdefghijklmnopqrst,vwxyz","title_addl_t" ),
				new SubfieldCodeMaker("210", "ab", "title_addl_t"),
                new SubfieldCodeMaker("222", "ab","title_addl_t"),
				new SubfieldCodeMaker("240", "abcdefgklmnopqrs","title_addl_t"),
				new SubfieldCodeMaker("242", "abnp","title_addl_t"),
				new SubfieldCodeMaker("243", "abcdefgklmnopqrs","title_addl_t"),
				new SubfieldCodeMaker("245", "abnps","title_addl_t"),
				new SubfieldCodeMaker("246", "abcdefgklmnopqrs","title_addl_t"),
				new SubfieldCodeMaker("247", "abcdefgnp","title_addl_t"),
				new SubfieldCodeMaker("700", "gklmnoprst","title_added_entry_t"),
				new SubfieldCodeMaker("710", "fgklmnopqrst","title_added_entry_t"),
				new SubfieldCodeMaker("711", "fgklnpst","title_added_entry_t"),
				new SubfieldCodeMaker("730", "abcdefgklmnopqrst","title_added_entry_t"),
				new SubfieldCodeMaker("740", "anp","title_added_entry_t"),	
				new SubfieldCodeMaker("440", "anpv","title_series_t"),
				new SubfieldCodeMaker("490", "av","title_series_t"),

				new SPARQLFieldMakerImpl().
					setName("titles").
					addMainStoreQuery("245_880",
			    		"SELECT ?ind2 ?code ?value ?vern_val\n" +
			    		"WHERE { $recordURI$ marcrdf:hasField ?f. \n" +
			    		"        ?f marcrdf:tag \"245\". \n" +
			    		"        ?f marcrdf:ind2 ?ind2 . \n" +
			    		"        ?f marcrdf:hasSubfield ?sf .\n" +
			    		"        ?sf marcrdf:code ?code.\n" +
			    		"        ?sf marcrdf:value ?value.\n" +
			    		"        OPTIONAL {" +
			    		"           $recordURI$ marcrdf:hasField ?f2.\n" +
			    		"           ?f2 marcrdf:tag \"880\".\n" +
			    		"           ?f2 marcrdf:hasSubfield ?sf2.\n" +
			    		"           ?sf2 marcrdf:code ?code.\n" +
				        "           ?sf2 marcrdf:value ?vern_val.\n" +
                        "           ?f2 marcrdf:hasSubfield ?sf2_6.\n" +
				        "           ?sf2_6 marcrdf:code \"6\".\n" +
                        "           ?sf2_6 marcrdf:value ?link2.\n" +
 				        "           ?f marcrdf:hasSubfield ?sf_6.\n" +
				        "           ?sf_6 marcrdf:code \"6\".\n" +
                        "           ?sf_6 marcrdf:value ?link.\n" +
                        "           FILTER(  SUBSTR( xsd:string(?link),5,2 ) = SUBSTR( xsd:string(?link2),5,2 ) ) }\n" +
				        "      }\n").
			    	addResultSetToFields( new TitleResultSetToFields())
			);
	}
			

}
