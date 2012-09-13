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
				    setName("language").
				    addMainStoreQuery("language_main",
				    		"SELECT DISTINCT ?language\n" +
				    		" WHERE {$recordURI$ marcrdf:hasField ?f.\n" +
				    		"        ?f marcrdf:tag \"008\".\n" +
				    		"        ?f marcrdf:value ?val.\n" +
				    		"        ?l rdf:type intlayer:Language.\n" +
				    		"        ?l intlayer:code ?langcode.\n" +
				    		"        FILTER( SUBSTR( xsd:string(?val),36,3) = xsd:string(?langcode) )\n" +
				    		"        ?l rdfs:label ?language.\n" +
				    		"	}").
				    addMainStoreQuery("languages_041",
				    		"SELECT DISTINCT ?language\n"+
				            " WHERE {$recordURI$ marcrdf:hasField ?f.\n" +
				            "        ?f marcrdf:tag \"041\".\n" +
				            "        ?f marcrdf:hasSubfield ?sf.\n" +
				            "        {?sf marcrdf:code \"a\"} UNION {?sf marcrdf:code \"d\"}" +
				            "        ?sf marcrdf:value ?langcode.\n" +
				            "        ?l intlayer:code ?langcode.\n" +
				            "        ?l rdfs:label ?language.\n" +
				            "}").
				    addResultSetToFields( new AllResultsToField("language_facet")),
				    		
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
				    
				new SubfieldCodeMaker("pub_info_display","260","abc"),
				new SubfieldCodeMaker("pub_info_display","264","abc"),
					
				new SubfieldCodeMaker("edition_display","250","ab"),
					
				new SubfieldCodeMaker("title_display","245","a"),
				new SubfieldCodeMaker("subtitle_display","245","b"),
					
				new SubfieldCodeMaker("title_addl_t","130","abcdefghijklmnopqrstuvwxyz"),
				new SubfieldCodeMaker("title_addl_t","210","ab"),
				new SubfieldCodeMaker("title_addl_t","222","ab"),
				new SubfieldCodeMaker("title_addl_t","240","abcdefgklmnopqrs"),
				new SubfieldCodeMaker("title_addl_t","242","abnp"),
				new SubfieldCodeMaker("title_addl_t","243","abcdefgklmnopqrs"),
				new SubfieldCodeMaker("title_addl_t","245","abnps"),
				new SubfieldCodeMaker("title_addl_t","246","abcdefgklmnopqrs"),
				new SubfieldCodeMaker("title_addl_t","247","abcdefgnp"),
	
				new SubfieldCodeMaker("title_added_entry_t","700","gklmnoprst"),
				new SubfieldCodeMaker("title_added_entry_t","710","fgklmnopqrst"),
				new SubfieldCodeMaker("title_added_entry_t","711","fgklnpst"),
				new SubfieldCodeMaker("title_added_entry_t","730","abcdefgklmnopqrst"),
				new SubfieldCodeMaker("title_added_entry_t","740","anp"),

				new SubfieldCodeMaker("title_series_t","440","anpv"),
				new SubfieldCodeMaker("title_series_t","490","av"),
				
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
			    	
				new SubfieldCodeMaker("notes_display","500","a"),
				new SubfieldCodeMaker("notes_display","501","a"),
				new SubfieldCodeMaker("notes_display","502","a"),
				new SubfieldCodeMaker("notes_display","503","a"),
				new SubfieldCodeMaker("notes_display","504","ab"),
				new SubfieldCodeMaker("notes_display","508","a"),
				new SubfieldCodeMaker("notes_display","513","ab"),
				new SubfieldCodeMaker("notes_display","518","adop"),
				new SubfieldCodeMaker("notes_display","521","a"),
				new SubfieldCodeMaker("notes_display","522","a"),
				new SubfieldCodeMaker("notes_display","523","a"),
				new SubfieldCodeMaker("notes_display","525","a"),
				new SubfieldCodeMaker("notes_display","527","a"),				
				new SubfieldCodeMaker("notes_display","530","abc3"),
				new SubfieldCodeMaker("notes_display","533","aebcdfn3"),
				new SubfieldCodeMaker("notes_display","534","abcefmpt"),
				new SubfieldCodeMaker("notes_display","535","abcd3"),
				new SubfieldCodeMaker("notes_display","537","a"),
				new SubfieldCodeMaker("notes_display","538","a"),
				new SubfieldCodeMaker("notes_display","544","a"),
				new SubfieldCodeMaker("notes_display","546","ab"),
				new SubfieldCodeMaker("notes_display","547","a"),
				new SubfieldCodeMaker("notes_display","550","a"),
				new SubfieldCodeMaker("notes_display","556","a"),
				new SubfieldCodeMaker("notes_display","561","ab3"),
				new SubfieldCodeMaker("notes_display","565","a"),
				new SubfieldCodeMaker("notes_display","567","a"),
				new SubfieldCodeMaker("notes_display","570","a"),
				new SubfieldCodeMaker("notes_display","580","a"),
				new SubfieldCodeMaker("notes_display","582","a"),
				new SubfieldCodeMaker("notes_display","588","a"),
				new SubfieldCodeMaker("notes_display","940","a"),
				new SubfieldCodeMaker("notes_display","856","3z"),
				new SubfieldCodeMaker("notes_display","856","m"),
				
				new SubfieldCodeMaker("author_t","100","abcegqu"),
				new SubfieldCodeMaker("author_t","110","abcdefghijklmnopqrstuvwxyz"),
				new SubfieldCodeMaker("author_t","111","acdegjnqu"),

				new SubfieldCodeMaker("author_addl_t","700","abcegqu"),
				new SubfieldCodeMaker("author_addl_t","710","abcdegnu"),
				new SubfieldCodeMaker("author_addl_t","711","acdegjnqu"),
				
				new SubfieldCodeMaker("author_display","100","abcdq"),
				new SubfieldCodeMaker("author_display","110","abcdefghijklmnopqrstuvwxyz"),
				new SubfieldCodeMaker("author_display","111","abcdefghijklmnopqrstuvwxyz"),
				new SubfieldCodeMaker("author_display","700","abcdq"),
				new SubfieldCodeMaker("author_display","710","abcdefghijklmnopqrstuvwxyz"),
				new SubfieldCodeMaker("author_display","711","abcdefghijklmnopqrstuvwxyz")
				
					
		);
	}

}
