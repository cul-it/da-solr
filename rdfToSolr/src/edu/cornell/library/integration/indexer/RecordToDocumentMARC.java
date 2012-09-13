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
					setName("format").
					addMainStoreQuery("bib_id","SELECT (SUBSTR(?leader,7,1) as ?rectype)\n" +
					        "                          (SUBSTR(?leader,8,1) as ?biblvl)\n" +
							"                          (SUBSTR(?seven,1,1) as ?cat)\n" +
							"                    WHERE { $recordURI$ marcrdf:leader ?leader.\n" +
							"                            OPTIONAL {\n" +
							"                            $recordURI$ marcrdf:hasField ?f.\n" +
							"                            ?f marcrdf:tag \"007\".\n" +
							"                            ?f marcrdf:value ?seven. }}").
					addResultSetToFields( new FormatResultSetToFields() ),
			    
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
				    
				new SubfieldCodeMaker("260","abc","pub_info_display"),
				new SubfieldCodeMaker("264","abc","pub_info_display"),
					
				new SubfieldCodeMaker("250","ab","edition_display"),
					
				new SubfieldCodeMaker("245","a","title_display"),
				new SubfieldCodeMaker("245","b","subtitle_display"),
					
				new SubfieldCodeMaker("130","abcdefghijklmnopqrstuvwxyz","title_addl_t"),
				new SubfieldCodeMaker("210","ab","title_addl_t"),
				new SubfieldCodeMaker("222","ab","title_addl_t"),
				new SubfieldCodeMaker("240","abcdefgklmnopqrs","title_addl_t"),
				new SubfieldCodeMaker("242","abnp","title_addl_t"),
				new SubfieldCodeMaker("243","abcdefgklmnopqrs","title_addl_t"),
				new SubfieldCodeMaker("245","abnps","title_addl_t"),
				new SubfieldCodeMaker("246","abcdefgklmnopqrs","title_addl_t"),
				new SubfieldCodeMaker("247","abcdefgnp","title_addl_t"),
	
				new SubfieldCodeMaker("700","gklmnoprst","title_added_entry_t"),
				new SubfieldCodeMaker("710","fgklmnopqrst","title_added_entry_t"),
				new SubfieldCodeMaker("711","fgklnpst","title_added_entry_t"),
				new SubfieldCodeMaker("730","abcdefgklmnopqrst","title_added_entry_t"),
				new SubfieldCodeMaker("740","anp","title_added_entry_t"),

				new SubfieldCodeMaker("440","anpv","title_series_t"),
				new SubfieldCodeMaker("490","av","title_series_t"),
				
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
                        "           FILTER( SUBSTR( xsd:string(?link2),5,2 ) " +
                        "                   = SUBSTR( xsd:string(?link),5,2 ) ) }\n" +
				        "      }\n").
			    	addResultSetToFields( new TitleResultSetToFields()),
			    	
				new SubfieldCodeMaker("500","a","notes_display"),
				new SubfieldCodeMaker("501","a","notes_display"),
				new SubfieldCodeMaker("502","a","notes_display"),
				new SubfieldCodeMaker("503","a","notes_display"),
				new SubfieldCodeMaker("504","ab","notes_display"),
				new SubfieldCodeMaker("508","a","notes_display"),
				new SubfieldCodeMaker("513","ab","notes_display"),
				new SubfieldCodeMaker("518","adop","notes_display"),
				new SubfieldCodeMaker("521","a","notes_display"),
				new SubfieldCodeMaker("522","a","notes_display"),
				new SubfieldCodeMaker("523","a","notes_display"),
				new SubfieldCodeMaker("525","a","notes_display"),
				new SubfieldCodeMaker("527","a","notes_display"),				
				new SubfieldCodeMaker("530","abc3","notes_display"),
				new SubfieldCodeMaker("533","aebcdfn3","notes_display"),
				new SubfieldCodeMaker("534","abcefmpt","notes_display"),
				new SubfieldCodeMaker("535","abcd3","notes_display"),
				new SubfieldCodeMaker("537","a","notes_display"),
				new SubfieldCodeMaker("538","a","notes_display"),
				new SubfieldCodeMaker("544","a","notes_display"),
				new SubfieldCodeMaker("546","ab","notes_display"),
				new SubfieldCodeMaker("547","a","notes_display"),
				new SubfieldCodeMaker("550","a","notes_display"),
				new SubfieldCodeMaker("556","a","notes_display"),
				new SubfieldCodeMaker("561","ab3","notes_display"),
				new SubfieldCodeMaker("565","a","notes_display"),
				new SubfieldCodeMaker("567","a","notes_display"),
				new SubfieldCodeMaker("570","a","notes_display"),
				new SubfieldCodeMaker("580","a","notes_display"),
				new SubfieldCodeMaker("582","a","notes_display"),
				new SubfieldCodeMaker("588","a","notes_display"),
				new SubfieldCodeMaker("940","a","notes_display"),
				new SubfieldCodeMaker("856","3z","notes_display"),
				new SubfieldCodeMaker("856","m","notes_display")
					
		);
	}

}
