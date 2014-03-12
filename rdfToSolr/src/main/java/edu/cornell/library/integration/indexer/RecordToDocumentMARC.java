package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.documentPostProcess.*;
import edu.cornell.library.integration.indexer.documentPostProcess.SingleValueField.Correction;
import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerStepped;
import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker.VernMode;
import edu.cornell.library.integration.indexer.resultSetToFieldsStepped.*;
import edu.cornell.library.integration.indexer.resultSetToFields.*;

/**
 * An example RecordToDocument implementation. 
 */
public class RecordToDocumentMARC extends RecordToDocumentBase {
	
	
	@Override
	List<? extends DocumentPostProcess> getDocumentPostProcess() {
		return (List<? extends DocumentPostProcess>) Arrays.asList(				
				new SingleValueField("pub_date_sort",Correction.firstValue),
				new SingleValueField("author_display",Correction.firstValue),
				new SingleValueField("author_sort",Correction.firstValue),
				new SingleValueField("author_t",Correction.firstValue),
				new SingleValueField("format_main_facet",Correction.firstValue),
				new RecordBoost(),
				new SuppressUnwantedValues(),
				new MissingTitleReport(),
				new SuppressShadowRecords(),
				new LoadItemData()
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
					setName("holdings ids").
					addMainStoreQuery("hold_ids",
							"SELECT ?id \n"+
				        	"WHERE {\n"+
					    	"  ?hold marcrdf:hasBibliographicRecord $recordURI$.\n" +
				        	"  ?hold marcrdf:hasField001 ?f.\n" +
				        	"  ?f marcrdf:value ?id.\n" +
							"}").
					addResultSetToFields( new AllResultsToField("holdings_display")),
			        
			    new SPARQLFieldMakerImpl().
			        setName("holdings_data").
				    addMainStoreQuery("holdings_control_fields",
				    		"SELECT *\n" +
				    		" WHERE {\n" +
				    		"   ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n" +
				    		"   ?mfhd marcrdf:hasField ?field.\n" +
				    		"   ?field marcrdf:tag ?tag.\n" +
				    		"   ?field marcrdf:value ?value. }").
			        addMainStoreQuery("holdings_data_fields",
			        	"SELECT * \n"+
			        	"WHERE {\n" +
			        	"  ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n" +
			        	"  ?mfhd marcrdf:hasField ?field.\n" +
			        	"  ?field marcrdf:tag ?tag.\n" +
			        	"  ?field marcrdf:ind1 ?ind1.\n" +
			        	"  ?field marcrdf:ind2 ?ind2.\n" +
			        	"  ?field marcrdf:hasSubfield ?sfield.\n" +
			        	"  ?sfield marcrdf:code ?code.\n" +
			        	"  ?sfield marcrdf:value ?value. }").
			        addMainStoreQuery("location",
					   	"SELECT DISTINCT ?locuri ?name ?library ?code \n"+
					   	"WHERE {\n"+
		                "  ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n"+
					  	"  OPTIONAL {\n" +
					   	"    ?mfhd marcrdf:hasField852 ?mfhd852.\n" +
					    "    ?mfhd852 marcrdf:hasSubfield ?mfhd852b.\n" +
					    "    ?mfhd852b marcrdf:code \"b\".\n" +
					    "    ?mfhd852b marcrdf:value ?code.\n" +
					    "    ?locuri rdf:type intlayer:Location.\n" +
					    "    ?locuri intlayer:code ?code.\n" +
					    "    ?locuri rdfs:label ?name.\n" +
					    "    OPTIONAL {\n" +
					    "      ?locuri intlayer:hasLibrary ?liburi.\n" +
					    "      ?liburi rdfs:label ?library.\n" +
					    "}}}").
		        addResultSetToFields( new HoldingsResultSetToFields()),

				new StandardMARCFieldMaker("lc_controlnum_display","010","a"),
				new StandardMARCFieldMaker("lc_controlnum_s","010","a"),
				new StandardMARCFieldMaker("other_id_display","035","a"),
				new StandardMARCFieldMaker("other_id_s","035","a"),
				

				new SPARQLFieldMakerImpl().
					setName("boost").
					addMainStoreQuery("boostType","SELECT ?boostType WHERE { $recordURI$ intlayer:boost ?boostType}").
					addResultSetToFields( new AllResultsToField("boost") ),
					
				new SPARQLFieldMakerImpl().
				    setName("marc").
				    addMainStoreQuery("marc_leader",
				    		"SELECT ?l\n" +
				    		" WHERE {  $recordURI$ marcrdf:leader ?l. }").
				    addMainStoreQuery("marc_control_fields",
				    		"SELECT *\n" +
				    		" WHERE {\n" +
				    		"   $recordURI$ marcrdf:hasField ?field.\n" +
				    		"   ?field marcrdf:tag ?tag.\n" +
				    		"   ?field marcrdf:value ?value. }").
				    addMainStoreQuery("marc_data_fields",
				    		"SELECT *\n" +
				    		" WHERE {\n" +
				    		"   $recordURI$ marcrdf:hasField ?field.\n" +
				    		"   ?field marcrdf:tag ?tag.\n" +
				    		"   ?field marcrdf:ind1 ?ind1.\n" +
				    		"   ?field marcrdf:ind2 ?ind2.\n" +
				    		"   ?field marcrdf:hasSubfield ?sfield.\n" +
				    		"   ?sfield marcrdf:code ?code.\n" +
				    		"   ?sfield marcrdf:value ?value. }").
				    addResultSetToFields( new MARCResultSetToFields() ),
				    
				new SPARQLFieldMakerImpl().
				    setName("multival").
				    addMainStoreQuery("holdings_descr",
				    		"SELECT ?f (SUBSTR(?leader,7,1) as ?rectype)\n"+
				            " WHERE { ?h marcrdf:hasBibliographicRecord $recordURI$.\n"+
				    				" ?h marcrdf:leader ?leader.\n"+
				    				" OPTIONAL {?h ?p ?f.\n"+
				                    "   ?p rdfs:subPropertyOf marcrdf:TextualHoldingsStatementField. }}").
				    addResultSetToFields( new MultivolResultSetToFields() ),

				                    
				    		
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
				    addMainStoreQuery("typeOfContinuingResource",
					   		"SELECT (SUBSTR(?val,22,1) as ?typeOfContinuingResource)\n" +
				    		"WHERE { $recordURI$ marcrdf:hasField ?f. \n" +
				    		"        ?f marcrdf:tag \"008\". \n" +
				    		"        ?f marcrdf:value ?val } \n" ).
					addMainStoreQuery("format_502",
							"SELECT ?f502\n" +
							" WHERE { $recordURI$ marcrdf:hasField ?f502.\n" +
							"       ?f502 marcrdf:tag \"502\". }").
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
					    	"  ?hold marcrdf:hasBibliographicRecord $recordURI$.\n" +
				        	"  ?hold marcrdf:hasField ?hold852.\n" +
				        	"  ?hold852 marcrdf:tag \"852\".\n" +
				        	"  ?hold852 marcrdf:hasSubfield ?hold852b.\n" +
				        	"  ?hold852b marcrdf:code \"b\".\n" +
					    	"  ?hold852b marcrdf:value ?loccode.\n" +
							"}").
					addResultSetToFields( new FormatResultSetToFields() ),
										
				getLanguageFieldMaker(),
				new StandardMARCFieldMaker("language_display","546","ab"),
				    		
			    new SPARQLFieldMakerStepped().
			        setName("call_numbers").
			        addMainStoreQuery("holdings_callno",
			        	"SELECT ?part1 ?part2 ?ind1\n"+
			        	"WHERE {\n"+
			        	"  ?hold marcrdf:hasBibliographicRecord $recordURI$.\n" +
			        	"  ?hold marcrdf:hasField852 ?hold852.\n" +
			        	"  ?hold852 marcrdf:ind1 ?ind1." +
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
				    
				new StandardMARCFieldMaker("pub_info_display","260","abc"),
			    new SPARQLFieldMakerImpl().
			    	setName("pub_info_264").
			    	addMainStoreQuery("pub_info", 
			    			"SELECT *\n" +
			    			" WHERE {\n" +
			    			"  $recordURI$ marcrdf:hasField264 ?field.\n" +
			    			"  ?field marcrdf:tag ?tag.\n" +
			    			"  ?field marcrdf:ind1 ?ind1.\n" +
			    			"  ?field marcrdf:ind2 ?ind2.\n" +
			    			"  ?field marcrdf:hasSubfield ?sfield.\n" +
			    			"  ?sfield marcrdf:code ?code.\n" +
			    			"  ?sfield marcrdf:value ?value. }").
			    	addResultSetToFields( new PubInfoResultSetToFields()),
//				new StandardMARCFieldMaker("pub_info_display","264","abc"),
				
				new StandardMARCFieldMaker("publisher_t","260","b"),
				new StandardMARCFieldMaker("publisher_t","264","b"),
					
				new StandardMARCFieldMaker("pubplace_t","260","a"),
				new StandardMARCFieldMaker("pubplace_t","264","a"),

				new StandardMARCFieldMaker("edition_display","250","ab"),
									
				new StandardMARCFieldMaker("title_addl_t","210","ab"),
				new StandardMARCFieldMaker("title_addl_t","222","ab"),
				new StandardMARCFieldMaker("title_addl_t","242","abnp"),
				new StandardMARCFieldMaker("title_addl_t","243","abcdefgklmnopqrs"),
				new StandardMARCFieldMaker("title_addl_t","245","abnps"),
				new StandardMARCFieldMaker("title_addl_t","246","abcdefgklmnopqrs"),
				new StandardMARCFieldMaker("title_addl_t","247","abcdefgnp"),
				new StandardMARCFieldMaker("title_addl_t","740","anp"),

				new StandardMARCFieldMaker("title_uniform_t","130","adfgklmnoprst"),
				new StandardMARCFieldMaker("title_uniform_t","240","adfgklmnoprs"),
				new StandardMARCFieldMaker("title_uniform_t","730","tklfnpmoqrs"),
				new StandardMARCFieldMaker("title_uniform_t","700","tklfnpmors"),
				new StandardMARCFieldMaker("title_uniform_t","710","tklfnpmors"),
				new StandardMARCFieldMaker("title_uniform_t","711","tklfnpmors"),

				new StandardMARCFieldMaker("title_series_t","400","abdfklnptvcegu"),				
				new StandardMARCFieldMaker("title_series_t","410","abdfklnptvcegu"),				
				new StandardMARCFieldMaker("title_series_t","411","acdefklnptgquv"),
				new StandardMARCFieldMaker("title_series_t","440","anpv"),
				new StandardMARCFieldMaker("title_series_t","800","abcdefghklmnopqrstuv"),
				new StandardMARCFieldMaker("title_series_t","810","abcdefghklmnopqrstuv"),
				new StandardMARCFieldMaker("title_series_t","811","acdefghklnpqstuv"),
				new StandardMARCFieldMaker("title_series_t","830","adfghklmnoprstv"),
				new StandardMARCFieldMaker("title_series_t","490","anpv"),

				new StandardMARCFieldMaker("title_other_display","243","adfgklmnoprs",":/ "),
				new StandardMARCFieldMaker("title_other_display","246","iabfnpg",":/ "),
				new StandardMARCFieldMaker("continues_display","247","abfgnpx",":/ "),

				new SPARQLFieldMakerImpl().
					setName("title130").
					addMainStoreQuery("title_130",
							"SELECT *\n" +
							" WHERE { $recordURI$ marcrdf:hasField130 ?field.\n" +
						    "        ?field marcrdf:tag ?tag.\n" +
						    "        ?field marcrdf:ind1 ?ind1.\n" +
						    "        ?field marcrdf:ind2 ?ind2.\n" +
							"        ?field marcrdf:hasSubfield ?sfield .\n" +
							"        ?sfield marcrdf:code ?code.\n" +
							"        ?sfield marcrdf:value ?value.\n" +
							" }").
					addResultSetToFields( new Title130ResultSetToFields()),

				
				new SPARQLFieldMakerStepped().
				    setName("title240").
					addMainStoreQuery("title_240",
							"SELECT *\n" +
							" WHERE { $recordURI$ marcrdf:hasField240 ?field.\n" +
				    		"        ?field marcrdf:tag ?tag. \n" +
				    		"        ?field marcrdf:ind1 ?ind1. \n" +
				    		"        ?field marcrdf:ind2 ?ind2. \n" +
				    		"        ?field marcrdf:hasSubfield ?sfield .\n" +
				    		"        ?sfield marcrdf:code ?code.\n" +
				    		"        ?sfield marcrdf:value ?value.\n" +
				    		" }").
				    addMainStoreQuery("main_entry_a", 
							"SELECT *\n" +
							" WHERE { $recordURI$ ?p ?field.\n" +
							"        ?p rdfs:subPropertyOf marcrdf:MainEntry.\n"+
				    		"        ?field marcrdf:tag ?tag. \n" +
				    		"        ?field marcrdf:ind1 ?ind1. \n" +
				    		"        ?field marcrdf:ind2 ?ind2. \n" +
				    		"        ?field marcrdf:hasSubfield ?sfield .\n" +
				    		"        ?sfield marcrdf:code ?code.\n" +
				    		"        ?sfield marcrdf:value ?value.\n" +
				    		" }").
			        addResultSetToFieldsStepped( new Title240ResultSetToFields()),
				
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
			    	addMainStoreQuery("title_sort_offset",
					    "SELECT ?ind2 \n" +
						"WHERE { $recordURI$ marcrdf:hasField ?f245. \n" +
						"        ?f245 marcrdf:tag \"245\". \n" +
					    "        ?f245 marcrdf:ind2 ?ind2 . \n" +
			    		"      }\n").
			    	addResultSetToFields( new TitleResultSetToFields()),
			    new StandardMARCFieldMaker("title_display","245","a",VernMode.SING_VERN,".,;:/ "),
			    new StandardMARCFieldMaker("subtitle_display","245","bdefgknpqsv",VernMode.SING_VERN,".,;:/ "),
			    new StandardMARCFieldMaker("fulltitle_display","245","abdefgknpqsv",VernMode.SING_VERN,".,;:/ "),
			    new StandardMARCFieldMaker("title_responsibility_display","245","c",VernMode.SINGULAR,".,;:/ "),
			    new StandardMARCFieldMaker("title_t","245","abcdefgknpqsv",VernMode.SINGULAR,".,;:/ "),
			    	
			    new SPARQLFieldMakerImpl().
			        setName("title_changes").
			        addMainStoreQuery("title_changes", 
							"SELECT *\n" +
							" WHERE { $recordURI$ ?p ?field.\n" +
							"        {?p rdfs:subPropertyOf marcrdf:AddedEntry} " +
							"                 UNION {?p rdfs:subPropertyOf marcrdf:LinkingEntry} \n"+
				    		"        ?field marcrdf:tag ?tag. \n" +
				    		"        ?field marcrdf:ind1 ?ind1. \n" +
				    		"        ?field marcrdf:ind2 ?ind2. \n" +
				    		"        ?field marcrdf:hasSubfield ?sfield .\n" +
				    		"        ?sfield marcrdf:code ?code.\n" +
				    		"        ?sfield marcrdf:value ?value.\n" +
				    		" }").
			        addResultSetToFields( new TitleChangeResultSetToFields()),
			    new StandardMARCFieldMaker("map_format_display","255","abcdefg"),
			    new StandardMARCFieldMaker("in_display","773","abdghikmnopqrstuw"),

			        
			    new SPARQLFieldMakerStepped().
			        setName("title_series_display").
			        addMainStoreQuery("title_series_830", 
				    "SELECT *\n" +
		        	" WHERE {\n" +
		        	"  $recordURI$ marcrdf:hasField830 ?field.\n" +
		        	"  ?field marcrdf:tag ?tag.\n" +
		        	"  ?field marcrdf:ind2 ?ind2.\n" +
		        	"  ?field marcrdf:ind1 ?ind1.\n" +
					"  ?field marcrdf:hasSubfield ?sfield.\n" +
			     	"  ?sfield marcrdf:code ?code.\n" +
				 	"  ?sfield marcrdf:value ?value. }").
			        addResultSetToFieldsStepped( new TitleSeriesResultSetToFields()),
			    	
			    new SPARQLFieldMakerImpl().
			        setName("table of contents").
			        addMainStoreQuery("table of contents",
			        	"SELECT *\n" +
			        	" WHERE {\n" +
			        	"  $recordURI$ marcrdf:hasField505 ?field.\n" +
			        	"  ?field marcrdf:tag ?tag.\n" +
			        	"  ?field marcrdf:ind2 ?ind2.\n" +
			        	"  ?field marcrdf:ind1 ?ind1.\n" +
			        	"  ?field marcrdf:hasSubfield ?sfield.\n" +
			        	"  ?sfield marcrdf:code ?code.\n" +
			        	"  ?sfield marcrdf:value ?value. }").
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
				   addMainStoreQuery("urls_mfhd", 
						"SELECT *\n" +
				    	" WHERE {\n" +
						"  ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n" +
			        	"  ?mfhd marcrdf:hasField ?f.\n" +
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
			        	"    ?location rdf:type intlayer:Location.\n" +
			        	"    ?location rdfs:label ?location_name.\n" +
			        	"    OPTIONAL {\n" +
			        	"      ?location intlayer:hasLibrary ?library.\n" +
			        	"      ?library rdfs:label ?library_name.\n" +
			        	"      OPTIONAL {\n" +
			        	"        ?library intlayer:hasGroup ?libgroup.\n" +
			        	"        ?libgroup rdfs:label ?group_name.\n" +
			        	"}}}}").
			        addResultSetToFields( new LocationResultSetToFields() ),
			        
			    new SPARQLFieldMakerImpl().
			    	setName("citation_reference_note").
			    	addMainStoreQuery("field510", 
			    			"SELECT *\n" +
			    			" WHERE {\n" +
			    			"  $recordURI$ marcrdf:hasField510 ?field.\n" +
			    			"  ?field marcrdf:tag ?tag.\n" +
			    			"  ?field marcrdf:ind1 ?ind1.\n" +
			    			"  ?field marcrdf:ind2 ?ind2.\n" +
			    			"  ?field marcrdf:hasSubfield ?sfield.\n" +
			    			"  ?sfield marcrdf:code ?code.\n" +
			    			"  ?sfield marcrdf:value ?value. }").
			    	addResultSetToFields( new CitationReferenceNoteResultSetToFields()),
				new StandardMARCFieldMaker("notes","510","abcux3"),
			    
				new StandardMARCFieldMaker("notes","500","a"),
				new StandardMARCFieldMaker("notes","501","a"),
				new StandardMARCFieldMaker("notes","502","a"),
				new StandardMARCFieldMaker("notes","503","a"),
				new StandardMARCFieldMaker("notes","504","ab"),
				new StandardMARCFieldMaker("notes","508","a"),
				new StandardMARCFieldMaker("notes","513","ab"),
				new StandardMARCFieldMaker("notes","518","adop"),
				new StandardMARCFieldMaker("notes","521","a"),
				new StandardMARCFieldMaker("notes","522","a"),
				new StandardMARCFieldMaker("notes","523","a"),
				new StandardMARCFieldMaker("notes","525","a"),
				new StandardMARCFieldMaker("notes","527","a"),				
				new StandardMARCFieldMaker("notes","530","abc3"),
				new StandardMARCFieldMaker("notes","533","aebcdfn3"),
				new StandardMARCFieldMaker("notes","534","abcefmpt"),
				new StandardMARCFieldMaker("notes","535","abcd3"),
				new StandardMARCFieldMaker("notes","537","a"),
				new StandardMARCFieldMaker("notes","538","a"),
				new StandardMARCFieldMaker("notes","544","a"),
				new StandardMARCFieldMaker("notes","547","a"),
				new StandardMARCFieldMaker("notes","550","a"),
				new StandardMARCFieldMaker("notes","556","a"),
				new StandardMARCFieldMaker("notes","561","ab3"),
				new StandardMARCFieldMaker("notes","565","a"),
				new StandardMARCFieldMaker("notes","567","a"),
				new StandardMARCFieldMaker("notes","570","a"),
				new StandardMARCFieldMaker("notes","580","a"),
				new StandardMARCFieldMaker("notes","582","a"),
				new StandardMARCFieldMaker("notes","588","a"),
				new StandardMARCFieldMaker("notes","940","a"),
				new StandardMARCFieldMaker("notes","856","m"),

				new StandardMARCFieldMaker("restrictions_display","506","3abce"),
				new StandardMARCFieldMaker("restrictions_display","540","3abcu"),
				new StandardMARCFieldMaker("cite_as_display","524","a3"),
				new StandardMARCFieldMaker("finding_aids_display","555","3abcdu"),
				new StandardMARCFieldMaker("historical_note_display","545","3abcu"),
				
				new StandardMARCFieldMaker("summary_display","520","ab"),
				
				new StandardMARCFieldMaker("description_display","300","abcefg"),
				
				new StandardMARCFieldMaker("subject_era_facet","650","y",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_era_facet","651","y",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_era_facet","654","y",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_era_facet","655","y",VernMode.SEPARATE,"."),

				new StandardMARCFieldMaker("subject_geo_facet","651","a",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_geo_facet","650","z",VernMode.SEPARATE,"."),

				new StandardMARCFieldMaker("subject_topic_facet","600","abcdq",VernMode.SEPARATE,",."),
				new StandardMARCFieldMaker("subject_topic_facet","610","ab",VernMode.SEPARATE,",."),
				new StandardMARCFieldMaker("subject_topic_facet","611","ab",VernMode.SEPARATE,",."),
				new StandardMARCFieldMaker("subject_topic_facet","630","a",VernMode.SEPARATE,",."),
				new StandardMARCFieldMaker("subject_topic_facet","630","ap",VernMode.SEPARATE,",."),
				new StandardMARCFieldMaker("subject_topic_facet","650","a",VernMode.SEPARATE,",."),
				new StandardMARCFieldMaker("subject_topic_facet","653","a",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_topic_facet","654","ab",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_topic_facet","655","ab",VernMode.SEPARATE,"."),
				new SPARQLFieldMakerImpl().
					setName("fact_or_fiction").
					addMainStoreQuery("fact_or_fiction",
			    		"SELECT (SUBSTR(?val,34,1) as ?char33) \n" +
			    		"WHERE { $recordURI$ marcrdf:hasField ?f. \n" +
			    		"        ?f marcrdf:tag \"008\". \n" +
			    		"        ?f marcrdf:value ?val } \n" ).
					addMainStoreQuery("record_type",
				    	"SELECT (SUBSTR(?l,7,1) as ?char6) \n" +
				   		"WHERE { $recordURI$ marcrdf:leader ?l. } \n").
			    	addResultSetToFields( new FactOrFictionResultSetToFields() ) ,

				new StandardMARCFieldMaker("subject_t","600","abcdefghijklmnopqrstu"),
				new StandardMARCFieldMaker("subject_t","610","abcdefghijklmnopqrstu"),
				new StandardMARCFieldMaker("subject_t","611","abcdefghijklmnopqrstu"),
				new StandardMARCFieldMaker("subject_t","630","abcdefghijklmnopqrst"),
				new StandardMARCFieldMaker("subject_t","650","abcde"),
				new StandardMARCFieldMaker("subject_t","651","ae"),
				new StandardMARCFieldMaker("subject_t","653","a"),
				new StandardMARCFieldMaker("subject_t","654","abcde"),
				new StandardMARCFieldMaker("subject_t","655","abc"),
				
				new StandardMARCFieldMaker("subject_addl_t","600","abcdefghkjlmnopqrstuvwxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","610","abcdefghklmnoprstuvwxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","611","acdefghklnpqstuvwxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","630","adfghklmnoprstvwxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","648","avxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","650","abcdvwxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","651","avwxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","653","avwxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","654","abevwxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","655","avwxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","656","akvxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","657","avxyz","."),
				new StandardMARCFieldMaker("subject_addl_t","658","abcd","."),
				new StandardMARCFieldMaker("subject_addl_t","662","abcdfgh","."),
				new StandardMARCFieldMaker("subject_addl_t","692","a","."),
				new StandardMARCFieldMaker("subject_addl_t","693","a","."),
				new StandardMARCFieldMaker("subject_addl_t","694","a","."),
				new StandardMARCFieldMaker("subject_addl_t","695","a","."),
				new StandardMARCFieldMaker("subject_addl_t","696","a","."),
				new StandardMARCFieldMaker("subject_addl_t","697","a","."),
				new StandardMARCFieldMaker("subject_addl_t","698","a","."),
				new StandardMARCFieldMaker("subject_addl_t","699","a","."),
				
			    new SPARQLFieldMakerImpl().
			    	setName("subject display").
			    	addMainStoreQuery("subject display",
					"SELECT *\n" +
					" WHERE { $recordURI$ ?p ?field.\n" +
					"        ?p rdfs:subPropertyOf marcrdf:SubjectTermEntry.\n"+
				    "        ?field marcrdf:tag ?tag. \n" +
				    "        ?field marcrdf:ind1 ?ind1. \n" +
				    "        ?field marcrdf:ind2 ?ind2. \n" +
				    "        ?field marcrdf:hasSubfield ?sfield .\n" +
				    "        ?sfield marcrdf:code ?code.\n" +
				    "        ?sfield marcrdf:value ?value.\n" +
				    " }").
		        	addResultSetToFields( new SubjectResultSetToFields()),
				
				new StandardMARCFieldMaker("donor_display","902","b"),
				
				new StandardMARCFieldMaker("frequency_display","310","a"),
				new StandardMARCFieldMaker("isbn_display","020","a"),				
				new StandardMARCFieldMaker("issn_display","022","a"),

				new StandardMARCFieldMaker("isbn_t","020","a"),				
				new StandardMARCFieldMaker("issn_t","022","a"),
				
			//	new StandardMARCFieldMaker("eightninenine_s","899","a"),
				new StandardMARCFieldMaker("eightninenine_t","899","ab"),
				
				new StandardMARCFieldMaker("other_identifier_display","024","a"),
				new StandardMARCFieldMaker("id_t","024","a"),
				new StandardMARCFieldMaker("publisher_number_display","028","a"),
				new StandardMARCFieldMaker("id_t","028","a"),
				
			    new SPARQLFieldMakerImpl().
		    	setName("author display").
			    addMainStoreQuery("main_entry", 
						"SELECT *\n" +
						" WHERE { $recordURI$ ?p ?field.\n" +
						"        ?p rdfs:subPropertyOf marcrdf:MainEntry.\n"+
			    		"        ?field marcrdf:tag ?tag. \n" +
			    		"        ?field marcrdf:ind1 ?ind1. \n" +
			    		"        ?field marcrdf:ind2 ?ind2. \n" +
			    		"        ?field marcrdf:hasSubfield ?sfield .\n" +
			    		"        ?sfield marcrdf:code ?code.\n" +
			    		"        ?sfield marcrdf:value ?value.\n" +
			    		" }").
		        addResultSetToFields( new AuthorResultSetToFields()),

				new StandardMARCFieldMaker("author_t","100","abcdqegu",VernMode.COMBINED),
				new StandardMARCFieldMaker("author_t","110","abcdefghijklmnopqrstuvwxyz",VernMode.COMBINED),
				new StandardMARCFieldMaker("author_t","111","abcdefghijklmnopqrstuvwxyz",VernMode.COMBINED),

				new StandardMARCFieldMaker("author_addl_t","700","abcdqegu"),
				new StandardMARCFieldMaker("author_addl_t","710","abcdefghijklmnopqrstuvwxyz"),
				new StandardMARCFieldMaker("author_addl_t","711","abcdefghijklmnopqrstuvwxyz"),
				
				new StandardMARCFieldMaker("author_facet","100","abcdq",VernMode.SEPARATE,".,"),
				new StandardMARCFieldMaker("author_facet","110","abcdefghijklmnopqrstuvwxyz",VernMode.SEPARATE,".,"),
				new StandardMARCFieldMaker("author_facet","111","abcdefghijklmnopqrstuvwxyz",VernMode.SEPARATE,".,"),
				new StandardMARCFieldMaker("author_facet","700","abcdq",VernMode.SEPARATE,".,"),
				new StandardMARCFieldMaker("author_facet","710","abcdefghijklmnopqrstuvwxyz",VernMode.SEPARATE,".,"),
				new StandardMARCFieldMaker("author_facet","711","abcdefghijklmnopqrstuvwxyz",VernMode.SEPARATE,".,")

									
		);
	}
	
	public static SPARQLFieldMakerImpl getLanguageFieldMaker() {
		
	return	new SPARQLFieldMakerImpl().
	    setName("language").
	    addMainStoreQuery("language_main",
	    		"SELECT DISTINCT ?language\n" +
	    		" WHERE {$recordURI$ marcrdf:hasField008 ?f.\n" +
	    		"        ?f marcrdf:value ?val.\n" +
	    		"        FILTER( SUBSTR( ?val,36,3) = ?langcode )\n" +
	    		"        ?l rdf:type intlayer:Language.\n" +
	    		"        ?l intlayer:code ?langcode.\n" +
	    		"        ?l rdfs:label ?language.\n" +
	    		"	}").
	    addMainStoreQuery("languages_041",
	    		"SELECT DISTINCT ?c ?language\n"+
	            " WHERE {$recordURI$ marcrdf:hasField041 ?f.\n" +
	            "        ?f marcrdf:hasSubfield ?sf.\n" +
	            "        ?sf marcrdf:code ?c.\n" +
	            "        ?sf marcrdf:value ?langcode.\n" +
	            "        ?l rdf:type intlayer:Language.\n" +
	            "        ?l intlayer:code ?langcode.\n" +
	            "        ?l rdfs:label ?language.\n" +
	            "}").
	    addResultSetToFields( new LanguageResultSetToFields());
	}

}
