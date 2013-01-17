package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.documentPostProcess.DocumentPostProcess;
import edu.cornell.library.integration.indexer.documentPostProcess.SinglePubDateSort;
import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerStepped;
import edu.cornell.library.integration.indexer.fieldMaker.SubfieldCodeMaker;
import edu.cornell.library.integration.indexer.resultSetToFieldsStepped.*;
import edu.cornell.library.integration.indexer.resultSetToFields.*;

/**
 * An example RecordToDocument implementation. 
 */
public class RecordToDocumentMARC extends RecordToDocumentBase {
	
	
	@Override
	List<? extends DocumentPostProcess> getDocumentPostProcess() {
		return (List<? extends DocumentPostProcess>) Arrays.asList(
				new SinglePubDateSort()
		);
	}

	@Override
	List<? extends FieldMaker> getFieldMakers() {
		return Arrays.asList(
				 	
				new SPARQLFieldMakerImpl().
					setName("id").
					addMainStoreQuery("bib_id","SELECT ?id WHERE { $recordURI$ rdfs:label ?id}").
					addResultSetToFields( new AllResultsToField("id") ),
					
				new SPARQLFieldMakerImpl().
				    setName("marc").
				    addMainStoreQuery("marc_leader",
				    		"SELECT ?l\n" +
				    		" WHERE {  $recordURI$ marcrdf:leader ?l. }").
				    addMainStoreQuery("marc_control_fields",
				    		"SELECT ?f ?t ?v\n" +
				    		" WHERE {\n" +
				    		"   $recordURI$ marcrdf:hasField ?f.\n" +
				    		"   ?f marcrdf:tag ?t.\n" +
				    		"   ?f marcrdf:value ?v. }").
				    addMainStoreQuery("marc_data_fields",
				    		"SELECT ?f ?t ?i1 ?i2 ?sf ?c ?v\n" +
				    		" WHERE {\n" +
				    		"   $recordURI$ marcrdf:hasField ?f.\n" +
				    		"   ?f marcrdf:tag ?t.\n" +
				    		"   ?f marcrdf:ind1 ?i1.\n" +
				    		"   ?f marcrdf:ind2 ?i2.\n" +
				    		"   ?f marcrdf:hasSubfield ?sf.\n" +
				    		"   ?sf marcrdf:code ?c.\n" +
				    		"   ?sf marcrdf:value ?v. }").
				    addResultSetToFields( new MARCResultSetToFields() ),

				    		
				new SPARQLFieldMakerImpl().
					setName("format").
					addMainStoreQuery("format_leader_seven",
							"SELECT (SUBSTR(?leader,7,1) as ?rectype)\n" +
					        "       (SUBSTR(?leader,8,1) as ?biblvl)\n" +
							"       (SUBSTR(?seven,1,1) as ?cat)\n" +
							" WHERE { $recordURI$ marcrdf:leader ?leader.\n" +
							"       OPTIONAL {\n" +
							"          $recordURI$ marcrdf:hasField ?f.\n" +
							"          ?f marcrdf:tag \"007\".\n" +
							"          ?f marcrdf:value ?seven. }}").
					addMainStoreQuery("format_653",
							"SELECT ?sf653a\n" +
							" WHERE { $recordURI$ marcrdf:hasField ?f.\n" +
							"       ?f marcrdf:tag \"653\".\n" +
							"       ?f marcrdf:hasSubfield ?sf653.\n" +
							"       ?sf653 marcrdf:code \"a\".\n" +
							"       ?sf653 marcrdf:value ?sf653a. }").
					addMainStoreQuery("format_948",
							"SELECT ?sf948f\n" +
							" WHERE { $recordURI$ marcrdf:hasField ?f.\n" +
							"       ?f marcrdf:tag \"948\".\n" +
							"       ?f marcrdf:hasSubfield ?sf948.\n" +
							"       ?sf948 marcrdf:code \"f\".\n" +
							"       ?sf948 marcrdf:value ?sf948f. }").
					addMainStoreQuery("format_245",
							"SELECT ?sf245h\n" +
							" WHERE { $recordURI$ marcrdf:hasField ?f.\n" +
							"       ?f marcrdf:tag \"245\".\n" +
							"       ?f marcrdf:hasSubfield ?sf245.\n" +
							"       ?sf245 marcrdf:code \"h\".\n" +
							"       ?sf245 marcrdf:value ?sf245h. }").
					addMainStoreQuery("format_loccode",
				        	"SELECT ?loccode \n"+
				        	"WHERE {\n"+
				        	"  $recordURI$ rdfs:label ?bib_id.\n"+
					    	"  ?hold marcrdf:hasField ?hold04.\n" +
					    	"  ?hold04 marcrdf:tag \"004\".\n" +
				        	"  ?hold04 marcrdf:value ?bib_id.\n" +
				        	"  ?hold marcrdf:hasField ?hold852.\n" +
				        	"  ?hold852 marcrdf:tag \"852\".\n" +
				        	"  ?hold852 marcrdf:hasSubfield ?hold852b.\n" +
				        	"  ?hold852b marcrdf:code \"b\".\n" +
					    	"  ?hold852b marcrdf:value ?loccode.\n" +
							"}").
					addResultSetToFields( new FormatResultSetToFields() ),
					
				getLanguageFieldMaker(),
				new SubfieldCodeMaker("language_display","546","ab"),
				    		
			    new SPARQLFieldMakerStepped().
			        setName("call_numbers").
			        addMainStoreQuery("holdings_callno",
			        	"SELECT ?part1 ?part2\n"+
			        	"WHERE {\n"+
                        "  $recordURI$ rdfs:label ?bib_id.\n"+
			        	"  ?hold marcrdf:hasField ?hold04.\n" +
			        	"  ?hold04 marcrdf:tag \"004\".\n" +
			        	"  ?hold04 marcrdf:value ?bib_id.\n" +
			        	"  ?hold marcrdf:hasField ?hold852.\n" +
			        	"  ?hold852 marcrdf:tag \"852\".\n" +
			        	"  ?hold852 marcrdf:hasSubfield ?hold852h.\n" +
			        	"  ?hold852h marcrdf:code \"h\".\n" +
			        	"  ?hold852h marcrdf:value ?part1.\n" +
			        	"  OPTIONAL {\n" +
			        	"    ?hold852 marcrdf:hasSubfield ?hold852i.\n" +
			        	"    ?hold852i marcrdf:code \"i\".\n" +
			        	"    ?hold852i marcrdf:value ?part2. }\n" +
			        	"}").
				    addMainStoreQuery("bib_callno",
					    "SELECT ?part1 ?part2\n"+
				    	"WHERE {\n"+
		                "  $recordURI$ marcrdf:hasField ?f50.\n" +
				    	"  ?f50 marcrdf:tag \"050\".\n" +
				    	"  ?f50 marcrdf:hasSubfield ?f50a.\n" +
				      	"  ?f50a marcrdf:code \"a\".\n" +
				      	"  ?f50a marcrdf:value ?part1.\n" +
					    "  OPTIONAL {\n" +
					  	"    ?f50 marcrdf:hasSubfield ?f50b.\n" +
					   	"    ?f50b marcrdf:code \"b\".\n" +
					   	"    ?f50b marcrdf:value ?part2. }\n" +
					    "}").
			        addResultSetToFieldsStepped( new CallNumberResultSetToFields() ),
			    	
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
				
				new SubfieldCodeMaker("publisher_t","260","b"),
				new SubfieldCodeMaker("publisher_t","264","b"),
					
				new SubfieldCodeMaker("edition_display","250","ab"),
									
				new SubfieldCodeMaker("title_addl_t","210","ab"),
				new SubfieldCodeMaker("title_addl_t","222","ab"),
				new SubfieldCodeMaker("title_addl_t","242","abnp"),
				new SubfieldCodeMaker("title_addl_t","243","abcdefgklmnopqrs"),
				new SubfieldCodeMaker("title_addl_t","245","abnps"),
				new SubfieldCodeMaker("title_addl_t","246","abcdefgklmnopqrs"),
				new SubfieldCodeMaker("title_addl_t","247","abcdefgnp"),
				new SubfieldCodeMaker("title_addl_t","740","anp"),

				new SubfieldCodeMaker("title_uniform_t","130","abcdefghijklmnopqrstuvwxyz"),
				new SubfieldCodeMaker("title_uniform_t","240","abcdefgklmnopqrs"),
				new SubfieldCodeMaker("title_uniform_t","730","abcdefgklmnopqrst"),
				new SubfieldCodeMaker("title_uniform_t","700","gklmnoprst"),
				new SubfieldCodeMaker("title_uniform_t","710","fgklmnopqrst"),
				new SubfieldCodeMaker("title_uniform_t","711","fgklnpst"),

				new SubfieldCodeMaker("title_series_t","400","abdfklnptvcegu"),				
				new SubfieldCodeMaker("title_series_t","410","abdfklnptvcegu"),				
				new SubfieldCodeMaker("title_series_t","411","acdefklnptgquv"),
				new SubfieldCodeMaker("title_series_t","440","anpv"),
				new SubfieldCodeMaker("title_series_t","800","abcdefghklmnopqrstuv"),
				new SubfieldCodeMaker("title_series_t","810","abcdefghklmnopqrstuv"),
				new SubfieldCodeMaker("title_series_t","811","acdefghklnpqstuv"),
				new SubfieldCodeMaker("title_series_t","830","adfghklmnoprstv"),
				new SubfieldCodeMaker("title_series_t","490","anpv"),

				new SubfieldCodeMaker("title_other_display","243","adfgklmnoprs",":/ "),
				new SubfieldCodeMaker("title_other_display","246","iabfnpg",":/ "),
				new SubfieldCodeMaker("title_other_display","247","abfgnpx",":/ "),
				new SubfieldCodeMaker("title_other_display","740","iahnp",":/ "),
				
				new SubfieldCodeMaker("title_uniform_display","130","aplskfmnordgt"),
				new SubfieldCodeMaker("title_uniform_display","240","adghplskfmnor"),				
				new SubfieldCodeMaker("title_uniform_display","700","tgklmnoprs"),
				new SubfieldCodeMaker("title_uniform_display","710","tfgklmnopqrs"),
				new SubfieldCodeMaker("title_uniform_display","711","tfgklnps"),
				new SubfieldCodeMaker("title_uniform_display","730","iaplskfmnordgh",":/ "),
				
				new SPARQLFieldMakerImpl().
					setName("titles").
					addMainStoreQuery("title_main",
						"SELECT ?code ?value\n" +
						" WHERE { $recordURI$ marcrdf:hasField ?f245.\n" +
			    		"        ?f245 marcrdf:tag \"245\". \n" +
			    		"        ?f245 marcrdf:hasSubfield ?f245sf .\n" +
			    		"        ?f245sf marcrdf:code ?code.\n" +
			    		"        ?f245sf marcrdf:value ?value.\n" +
			    		" }").
			    	addMainStoreQuery("title_vern",
			    		"SELECT ?code ?value\n" +
						" WHERE { $recordURI$ marcrdf:hasField ?f880.\n" +
				   		"        ?f880 marcrdf:tag \"880\". \n" +
						"        ?f880 marcrdf:hasSubfield ?f880sf .\n" +
						"        ?f880sf marcrdf:code ?code.\n" +
						"        ?f880sf marcrdf:value ?value.\n" +
						"        ?f880 marcrdf:hasSubfield ?f880sf6.\n" +
						"        ?f880sf6 marcrdf:code \"6\".\n" +
						"        ?f880sf6 marcrdf:value ?value6.\n" +
						"        FILTER( regex( xsd:string(?value6), \"^245\" ))\n" +
			    		" }"	).
			    	addMainStoreQuery("title_sort_offset",
					    "SELECT ?ind2 \n" +
						"WHERE { $recordURI$ marcrdf:hasField ?f245. \n" +
						"        ?f245 marcrdf:tag \"245\". \n" +
					    "        ?f245 marcrdf:ind2 ?ind2 . \n" +
			    		"      }\n").
			    	addResultSetToFields( new TitleResultSetToFields()),
			    	
			    new SPARQLFieldMakerImpl().
			        setName("title_changes").
			        addMainStoreQuery("title_changes", 
			        	"SELECT *\n" +
			        	" WHERE {\n" +
			        	"  $recordURI$ marcrdf:hasField ?f.\n" +
			        	"  ?f marcrdf:tag ?t.\n" +
			        	"  FILTER( regex( xsd:string(?t), \"^7\" ))\n" +
			        	"  ?f marcrdf:ind2 ?i2.\n" +
			        	"  ?f marcrdf:ind1 ?i1.\n" +
			        	"  ?f marcrdf:hasSubfield ?sf.\n" +
			        	"  ?sf marcrdf:code ?c.\n" +
			        	"  ?sf marcrdf:value ?v. }").
			        addResultSetToFields( new TitleChangeResultSetToFields()),
			        
			    new SPARQLFieldMakerStepped().
			        setName("title_series_display").
			        addMainStoreQuery("title_series_830", 
				        	"SELECT *\n" +
				        	" WHERE {\n" +
				        	"  $recordURI$ marcrdf:hasField ?f.\n" +
				        	"  ?f marcrdf:tag \"830\".\n" +
				        	"  ?f marcrdf:hasSubfield ?sf.\n" +
				        	"  ?sf marcrdf:code ?c.\n" +
				        	"  ?sf marcrdf:value ?v. }").
			        addResultSetToFieldsStepped( new TitleSeriesResultSetToFields()),
			    	
			    new SPARQLFieldMakerImpl().
			        setName("table of contents").
			        addMainStoreQuery("table of contents", 
			        	"SELECT *\n" +
			        	" WHERE {\n" +
			        	"  $recordURI$ marcrdf:hasField ?f.\n" +
			        	"  ?f marcrdf:tag \"505\".\n" +
			        	"  ?f marcrdf:tag ?t.\n" +
			        	"  ?f marcrdf:ind2 ?i2.\n" +
			        	"  ?f marcrdf:ind1 ?i1.\n" +
			        	"  ?f marcrdf:hasSubfield ?sf.\n" +
			        	"  ?sf marcrdf:code ?c.\n" +
			        	"  ?sf marcrdf:value ?v. }").
			        addResultSetToFields( new TOCResultSetToFields()),

			   new SPARQLFieldMakerImpl().
			        setName("urls").
			        addMainStoreQuery("urls", 
			        	"SELECT *\n" +
			        	" WHERE {\n" +
			        	"  $recordURI$ marcrdf:hasField ?f.\n" +
			        	"  ?f marcrdf:tag \"856\".\n" +
			        	"  ?f marcrdf:ind1 ?i1.\n" +
			        	"  ?f marcrdf:ind2 ?i2.\n" +
			        	"  ?f marcrdf:hasSubfield ?sf.\n" +
			        	"  ?sf marcrdf:code ?c.\n" +
			        	"  ?sf marcrdf:value ?v. }").
			        addResultSetToFields( new URLResultSetToFields()),

			    new SPARQLFieldMakerImpl().
			        setName("Locations").
			        addMainStoreQuery("location",
			        	"SELECT ?bib_id ?location_name ?library_name ?group_name \n"+
			        	"WHERE {\n"+
                        "  $recordURI$ rdfs:label ?bib_id.\n"+
			        	"  OPTIONAL {\n" +
			        	"    ?hold marcrdf:hasField ?hold04.\n" +
			        	"    ?hold04 marcrdf:tag \"004\".\n" +
			        	"    ?hold04 marcrdf:value ?bib_id.\n" +
			        	"    ?hold marcrdf:hasField ?hold852.\n" +
			        	"    ?hold852 marcrdf:tag \"852\".\n" +
			        	"    ?hold852 marcrdf:hasSubfield ?hold852b.\n" +
			        	"    ?hold852b marcrdf:code \"b\".\n" +
			        	"    ?hold852b marcrdf:value ?loccode.\n" +
			        	"    ?location intlayer:code ?loccode.\n" +
			        	"    ?location rdfs:label ?location_name.\n" +
			        	"    OPTIONAL {\n" +
			        	"      ?location intlayer:hasLibrary ?library.\n" +
			        	"      ?library rdfs:label ?library_name.\n" +
			        	"      OPTIONAL {\n" +
			        	"        ?library intlayer:hasGroup ?libgroup.\n" +
			        	"        ?libgroup rdfs:label ?group_name.\n" +
			        	"}}}}").
			        addResultSetToFields( new LocationResultSetToFields() ),
			    	
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

				new SubfieldCodeMaker("summary_display","520","ab"),
				
				new SubfieldCodeMaker("description_display","300","abcefg"),
				
				new SubfieldCodeMaker("subject_era_facet","650","y","."),
				new SubfieldCodeMaker("subject_era_facet","651","y","."),
				new SubfieldCodeMaker("subject_era_facet","654","y","."),
				new SubfieldCodeMaker("subject_era_facet","655","y","."),

				new SubfieldCodeMaker("subject_geo_facet","651","a","."),
				new SubfieldCodeMaker("subject_geo_facet","650","z","."),

				new SubfieldCodeMaker("subject_topic_facet","600","abcdq",",."),
				new SubfieldCodeMaker("subject_topic_facet","610","ab",",."),
				new SubfieldCodeMaker("subject_topic_facet","611","ab",",."),
				new SubfieldCodeMaker("subject_topic_facet","630","a",",."),
				new SubfieldCodeMaker("subject_topic_facet","630","ap",",."),
				new SubfieldCodeMaker("subject_topic_facet","650","a",",."),
				new SubfieldCodeMaker("subject_topic_facet","653","a","."),
				new SubfieldCodeMaker("subject_topic_facet","654","ab","."),
				new SubfieldCodeMaker("subject_topic_facet","655","ab","."),
				new SPARQLFieldMakerImpl().
					setName("fact_or_fiction").
					addMainStoreQuery("fact_or_fiction",
			    		"SELECT (SUBSTR(?val,34,1) as ?char33) \n" +
			    		"WHERE { $recordURI$ marcrdf:hasField ?f. \n" +
			    		"        ?f marcrdf:tag \"008\". \n" +
			    		"        ?f marcrdf:value ?val } \n" ).
			    	addResultSetToFields( new FactOrFictionResultSetToFields() ) ,

				new SubfieldCodeMaker("subject_t","600","abcdefghijklmnopqrstu"),
				new SubfieldCodeMaker("subject_t","610","abcdefghijklmnopqrstu"),
				new SubfieldCodeMaker("subject_t","611","abcdefghijklmnopqrstu"),
				new SubfieldCodeMaker("subject_t","630","abcdefghijklmnopqrst"),
				new SubfieldCodeMaker("subject_t","650","abcde"),
				new SubfieldCodeMaker("subject_t","651","ae"),
				new SubfieldCodeMaker("subject_t","653","a"),
				new SubfieldCodeMaker("subject_t","654","abcde"),
				new SubfieldCodeMaker("subject_t","655","abc"),
				
				new SubfieldCodeMaker("subject_addl_t","600","abcdefghkjlmnopqrstuvwxyz","."),
				new SubfieldCodeMaker("subject_addl_t","610","abcdefghklmnoprstuvwxyz","."),
				new SubfieldCodeMaker("subject_addl_t","611","acdefghklnpqstuvwxyz","."),
				new SubfieldCodeMaker("subject_addl_t","630","adfghklmnoprstvwxyz","."),
				new SubfieldCodeMaker("subject_addl_t","650","abcdvwxyz","."),
				new SubfieldCodeMaker("subject_addl_t","651","avwxyz","."),
				new SubfieldCodeMaker("subject_addl_t","653","vwxyz","."),
				new SubfieldCodeMaker("subject_addl_t","654","vwxyz","."),
				new SubfieldCodeMaker("subject_addl_t","655","avwxyz","."),
				new SubfieldCodeMaker("subject_addl_t","692","a","."),
				new SubfieldCodeMaker("subject_addl_t","693","a","."),
				new SubfieldCodeMaker("subject_addl_t","694","a","."),
				new SubfieldCodeMaker("subject_addl_t","695","a","."),
				new SubfieldCodeMaker("subject_addl_t","696","a","."),
				new SubfieldCodeMaker("subject_addl_t","697","a","."),
				new SubfieldCodeMaker("subject_addl_t","698","a","."),
				new SubfieldCodeMaker("subject_addl_t","699","a","."),
				
			    new SPARQLFieldMakerImpl().
			    	setName("subject display").
			    	addMainStoreQuery("subject display", 
		        	"SELECT *\n" +
		        	" WHERE {\n" +
		        	"  $recordURI$ marcrdf:hasField ?f.\n" +
		        	"  ?f marcrdf:tag ?t.\n" +
		        	"  FILTER( REGEX( ?t, \"^6\" ))\n" +
		        	"  ?f marcrdf:ind2 ?i2.\n" +
		        	"  ?f marcrdf:ind1 ?i1.\n" +
		        	"  ?f marcrdf:hasSubfield ?sf.\n" +
		        	"  ?sf marcrdf:code ?c.\n" +
		        	"  ?sf marcrdf:value ?v. }").
		        	addResultSetToFields( new SubjectResultSetToFields()),
				
				new SubfieldCodeMaker("donor_s","902","b"),
				
				new SubfieldCodeMaker("frequency_display","310","a"),
				new SubfieldCodeMaker("isbn_display","020","a"),				
				new SubfieldCodeMaker("issn_display","022","a"),				

				new SubfieldCodeMaker("author_t","100","abcdqegu"),
				new SubfieldCodeMaker("author_t","110","abcdefghijklmnopqrstuvwxyz"),
				new SubfieldCodeMaker("author_t","111","abcdefghijklmnopqrstuvwxyz"),

				new SubfieldCodeMaker("author_addl_t","700","abcdqegu"),
				new SubfieldCodeMaker("author_addl_t","710","abcdefghijklmnopqrstuvwxyz"),
				new SubfieldCodeMaker("author_addl_t","711","abcdefghijklmnopqrstuvwxyz"),
				
				new SubfieldCodeMaker("author_facet","100","abcdq",".,"),
				new SubfieldCodeMaker("author_facet","110","abcdefghijklmnopqrstuvwxyz",".,"),
				new SubfieldCodeMaker("author_facet","111","abcdefghijklmnopqrstuvwxyz",".,"),
				new SubfieldCodeMaker("author_facet","700","abcdq",".,"),
				new SubfieldCodeMaker("author_facet","710","abcdefghijklmnopqrstuvwxyz",".,"),
				new SubfieldCodeMaker("author_facet","711","abcdefghijklmnopqrstuvwxyz",".,"),

				new SubfieldCodeMaker("author_display","100","abcdq",".,"),
				new SubfieldCodeMaker("author_display","110","abcdefghijklmnopqrstuvwxyz",".,"),
				new SubfieldCodeMaker("author_display","111","abcdefghijklmnopqrstuvwxyz",".,"),
				new SubfieldCodeMaker("author_addl_display","700","abcdq",".,"),
				new SubfieldCodeMaker("author_addl_display","710","abcdefghijklmnopqrstuvwxyz",".,"),
				new SubfieldCodeMaker("author_addl_display","711","abcdefghijklmnopqrstuvwxyz",".,")
				
					
		);
	}
	
	public static SPARQLFieldMakerImpl getLanguageFieldMaker() {
		
	return	new SPARQLFieldMakerImpl().
	    setName("language").
	    addMainStoreQuery("language_main",
	    		"SELECT DISTINCT ?language\n" +
	    		" WHERE {$recordURI$ marcrdf:hasField ?f.\n" +
	    		"        ?f marcrdf:tag \"008\".\n" +
	    		"        ?f marcrdf:value ?val.\n" +
	    		"        ?l rdf:type intlayer:Language.\n" +
	    		"        ?l intlayer:code ?langcode.\n" +
	    		"        FILTER( SUBSTR( ?val,36,3) = ?langcode )\n" +
	    		"        ?l rdfs:label ?language.\n" +
	    		"	}").
	    addMainStoreQuery("languages_041",
	    		"SELECT DISTINCT ?c ?language\n"+
	            " WHERE {$recordURI$ marcrdf:hasField ?f.\n" +
	            "        ?f marcrdf:tag \"041\".\n" +
	            "        ?f marcrdf:hasSubfield ?sf.\n" +
	            "        ?sf marcrdf:code ?c.\n" +
	            "        ?sf marcrdf:value ?langcode.\n" +
	            "        ?l intlayer:code ?langcode.\n" +
	            "        ?l rdfs:label ?language.\n" +
	            "}").
	    addResultSetToFields( new LanguageResultSetToFields());
	}

}
