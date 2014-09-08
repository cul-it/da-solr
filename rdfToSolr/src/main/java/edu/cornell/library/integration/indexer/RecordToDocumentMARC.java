package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.documentPostProcess.BarcodeSearch;
import edu.cornell.library.integration.indexer.documentPostProcess.DocumentPostProcess;
import edu.cornell.library.integration.indexer.documentPostProcess.LoadItemData;
import edu.cornell.library.integration.indexer.documentPostProcess.MissingTitleReport;
import edu.cornell.library.integration.indexer.documentPostProcess.NoAccessUrlsUnlessOnline;
import edu.cornell.library.integration.indexer.documentPostProcess.RecordBoost;
import edu.cornell.library.integration.indexer.documentPostProcess.RemoveDuplicateTitleData;
import edu.cornell.library.integration.indexer.documentPostProcess.SingleValueField;
import edu.cornell.library.integration.indexer.documentPostProcess.SingleValueField.Correction;
import edu.cornell.library.integration.indexer.documentPostProcess.SuppressUnwantedValues;
import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.IndicatorReq;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerStepped;
import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker.VernMode;
import edu.cornell.library.integration.indexer.resultSetToFields.AllResultsToField;
import edu.cornell.library.integration.indexer.resultSetToFields.AuthorResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.CitationReferenceNoteResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.DBCodeRSTF;
import edu.cornell.library.integration.indexer.resultSetToFields.DateResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.FactOrFictionResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.FormatResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.HoldingsResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.LanguageResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.LocationResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.MARCResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.PubInfoResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.RecordTypeRSTF;
import edu.cornell.library.integration.indexer.resultSetToFields.SubjectResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.TOCResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.Title130ResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.TitleChangeResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.TitleResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFields.URLResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFieldsStepped.CallNumberResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFieldsStepped.Title240ResultSetToFields;
import edu.cornell.library.integration.indexer.resultSetToFieldsStepped.TitleSeriesResultSetToFields;

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
				new SingleValueField("author_t",Correction.concatenate),
				new SingleValueField("title_t",Correction.concatenate),
				new SingleValueField("format_main_facet",Correction.firstValue),
				new RecordBoost(),
				new SuppressUnwantedValues(),
				new MissingTitleReport(),
//				new SuppressShadowRecords(),
				new LoadItemData(),
				new BarcodeSearch(),
				new RemoveDuplicateTitleData(),
				new NoAccessUrlsUnlessOnline()
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
				    		"   ?mfhd ?p ?field.\n" +
				    		"   ?p rdfs:subPropertyOf marcrdf:ControlFields. \n"+
				    		"   ?field marcrdf:tag ?tag.\n" +
				    		"   ?field marcrdf:value ?value. }").
			        addMainStoreQuery("holdings_data_fields",
			        	"SELECT * \n"+
			        	"WHERE {\n" +
			        	"  ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n" +
			    		"  ?mfhd ?p ?field.\n" +
			    		"  ?p rdfs:subPropertyOf marcrdf:DataFields. \n"+
			        	"  ?field marcrdf:tag ?tag.\n" +
			        	"  ?field marcrdf:ind1 ?ind1.\n" +
			        	"  ?field marcrdf:ind2 ?ind2.\n" +
			        	"  ?field marcrdf:hasSubfield ?sfield.\n" +
			        	"  ?sfield marcrdf:code ?code.\n" +
			        	"  ?sfield marcrdf:value ?value. }").
			        addMainStoreQuery("location",
					   	"SELECT DISTINCT ?code ?name ?library ?locuri \n"+
					   	"WHERE {\n"+
		                "  ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n"+
					   	"  ?mfhd marcrdf:hasField852 ?mfhd852.\n" +
					    "  ?mfhd852 marcrdf:hasSubfield ?mfhd852b.\n" +
					    "  ?mfhd852b marcrdf:code \"b\"^^xsd:string.\n" +
                        "  ?mfhd852b marcrdf:value ?code. \n" +
					    "  OPTIONAL {\n" +
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
				new StandardMARCFieldMaker("id_t","035","a"),
				

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
				    		"   $recordURI$ ?p ?field.\n" +
				    		"   ?p rdfs:subPropertyOf marcrdf:ControlFields. \n"+
				    		"   ?field marcrdf:tag ?tag.\n" +
				    		"   ?field marcrdf:value ?value. }").
				    addMainStoreQuery("marc_data_fields",
				    		"SELECT *\n" +
				    		" WHERE {\n" +
				    		"   $recordURI$ ?p ?field.\n" +
				    		"   ?p rdfs:subPropertyOf marcrdf:DataFields. \n"+
				    		"   ?field marcrdf:tag ?tag.\n" +
				    		"   ?field marcrdf:ind1 ?ind1.\n" +
				    		"   ?field marcrdf:ind2 ?ind2.\n" +
				    		"   ?field marcrdf:hasSubfield ?sfield.\n" +
				    		"   ?sfield marcrdf:code ?code.\n" +
				    		"   ?sfield marcrdf:value ?value. }").
				    addResultSetToFields( new MARCResultSetToFields() ),
				    
				/*  The new logic for multivol uses item data not available in this step. Field
				 * population moving to LoadItemData document post-processor.
				new SPARQLFieldMakerImpl().
				    setName("multivol").
				    addMainStoreQuery("holdings_descr",
				    		"SELECT ?f (SUBSTR(?leader,7,1) as ?rectype)\n"+
				            " WHERE { ?h marcrdf:hasBibliographicRecord $recordURI$.\n"+
				    				" ?h marcrdf:leader ?leader.\n"+
				    				" OPTIONAL {?h ?p ?f.\n"+
				                    "   ?p rdfs:subPropertyOf marcrdf:TextualHoldingsStatementField. }}").
				    addResultSetToFields( new MultivolRSTF() ), */

				new SPARQLFieldMakerImpl().
				    setName("rec_type").
				    addMainStoreQuery("shadow_rec",
							"SELECT ?sf948h\n" +
							" WHERE { $recordURI$ marcrdf:hasField948 ?f.\n" +
							"       ?f marcrdf:hasSubfield ?sf948.\n" +
							"       ?sf948 marcrdf:code \"h\"^^xsd:string.\n" +
							"       ?sf948 marcrdf:value ?sf948h. }").
					addMainStoreQuery("shadow_rec_mfhd",
							"SELECT ?sf852x\n" +
							" WHERE {?h marcrdf:hasBibliographicRecord $recordURI$.\n" +
							"        ?h marcrdf:hasField852 ?f.\n" +
							"        ?f marcrdf:hasSubfield ?sf.\n" +
							"        ?sf marcrdf:code \"x\"^^xsd:string.\n" +
							"        ?sf marcrdf:value ?sf852x. }").
				    addResultSetToFields( new RecordTypeRSTF() ),                  
				    		
				new SPARQLFieldMakerImpl().
					setName("format").
					addMainStoreQuery("format_leader_seven",
							"SELECT (SUBSTR(?leader,7,1) as ?rectype)\n" +
					        "       (SUBSTR(?leader,8,1) as ?biblvl)\n" +
							"       (SUBSTR(?seven,1,1) as ?cat)\n" +
							" WHERE { $recordURI$ marcrdf:leader ?leader.\n" +
							"       OPTIONAL {\n" +
							"          $recordURI$ marcrdf:hasField007 ?f.\n" +
							"          ?f marcrdf:value ?seven. }}").
				    addMainStoreQuery("typeOfContinuingResource",
					   		"SELECT (SUBSTR(?val,22,1) as ?typeOfContinuingResource)\n" +
				    		"WHERE { $recordURI$ marcrdf:hasField008 ?f. \n" +
				    		"        ?f marcrdf:value ?val } \n" ).
					addMainStoreQuery("format_502",
							"SELECT ?f502\n" +
							" WHERE { $recordURI$ marcrdf:hasField502 ?f502. }").
					addMainStoreQuery("format_653",
							"SELECT ?sf653a\n" +
							" WHERE { $recordURI$ marcrdf:hasField653 ?f.\n" +
							"       ?f marcrdf:hasSubfield ?sf653.\n" +
							"       ?sf653 marcrdf:code \"a\"^^xsd:string.\n" +
							"       ?sf653 marcrdf:value ?sf653a. }").
					addMainStoreQuery("format_948",
							"SELECT ?sf948f\n" +
							" WHERE { $recordURI$ marcrdf:hasField948 ?f.\n" +
							"       ?f marcrdf:hasSubfield ?sf948.\n" +
							"       ?sf948 marcrdf:code \"f\"^^xsd:string.\n" +
							"       ?sf948 marcrdf:value ?sf948f. }").
					addMainStoreQuery("format_245",
							"SELECT ?sf245h\n" +
							" WHERE { $recordURI$ marcrdf:hasField245 ?f.\n" +
							"       ?f marcrdf:hasSubfield ?sf245.\n" +
							"       ?sf245 marcrdf:code \"h\"^^xsd:string.\n" +
							"       ?sf245 marcrdf:value ?sf245h. }").
					addMainStoreQuery("format_loccode",
				        	"SELECT ?loccode \n"+
				        	"WHERE {\n"+
					    	"  ?hold marcrdf:hasBibliographicRecord $recordURI$.\n" +
				        	"  ?hold marcrdf:hasField852 ?hold852.\n" +
				        	"  ?hold852 marcrdf:hasSubfield ?hold852b.\n" +
				        	"  ?hold852b marcrdf:code \"b\"^^xsd:string.\n" +
					    	"  ?hold852b marcrdf:value ?loccode.\n" +
							"}").
					addResultSetToFields( new FormatResultSetToFields() ),
										
				getLanguageFieldMaker(),
				new StandardMARCFieldMaker("language_display","546","ab"),
				    		
			    new SPARQLFieldMakerStepped().
			        setName("call_numbers").
			        addMainStoreQuery("holdings_callno",
			        	"SELECT ?part1 ?part2 ?prefix ?ind1\n"+
			        	"WHERE {\n"+
			        	"  ?hold marcrdf:hasBibliographicRecord $recordURI$.\n" +
			        	"  ?hold marcrdf:hasField852 ?hold852.\n" +
			        	"  ?hold852 marcrdf:ind1 ?ind1." +
			        	"  ?hold852 marcrdf:hasSubfield ?hold852h.\n" +
			        	"  ?hold852h marcrdf:code \"h\"^^xsd:string.\n" +
			        	"  ?hold852h marcrdf:value ?part1.\n" +
			        	"  OPTIONAL {\n" +
			        	"    ?hold852 marcrdf:hasSubfield ?hold852i.\n" +
			        	"    ?hold852i marcrdf:code \"i\"^^xsd:string.\n" +
			        	"    ?hold852i marcrdf:value ?part2. }\n" + 
			        	"  OPTIONAL {\n" +
			        	"    ?hold852 marcrdf:hasSubfield ?hold852k.\n" +
			        	"    ?hold852k marcrdf:code \"k\"^^xsd:string.\n" +
			        	"    ?hold852k marcrdf:value ?prefix. }\n" + 
			        	"}").
				    addMainStoreQuery("bib_callno",
					    "SELECT ?part1 ?part2\n"+
				    	"WHERE {\n"+
		                "  $recordURI$ marcrdf:hasField050 ?f50.\n" +
				    	"  ?f50 marcrdf:hasSubfield ?f50a.\n" +
				      	"  ?f50a marcrdf:code \"a\"^^xsd:string.\n" +
				      	"  ?f50a marcrdf:value ?part1.\n" +
					    "  OPTIONAL {\n" +
					  	"    ?f50 marcrdf:hasSubfield ?f50b.\n" +
					   	"    ?f50b marcrdf:code \"b\"^^xsd:string.\n" +
					   	"    ?f50b marcrdf:value ?part2. }\n" +
					    "}").
			        addResultSetToFieldsStepped( new CallNumberResultSetToFields() ),
			    	
				new SPARQLFieldMakerImpl().
				    setName("publication_date").
				    addMainStoreQuery("machine_dates",
				    		"SELECT (SUBSTR(?val,8,4) as ?date1) (SUBSTR(?val,12,4) AS ?date2) \n" +
				    		"WHERE { $recordURI$ marcrdf:hasField008 ?f. \n" +
				    		"        ?f marcrdf:value ?val } \n" ).
				    addMainStoreQuery("human_dates",
				    		"SELECT ?date \n" +
				    		"WHERE { { $recordURI$ marcrdf:hasField260 ?f }" +
				    		"         UNION { $recordURI$ marcrdf:hasField264 ?f } \n" +
				    		"        ?f marcrdf:hasSubfield ?s. \n" +
				    		"        ?s marcrdf:code \"c\"^^xsd:string. \n" +
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
			    	addResultSetToFields( new PubInfoResultSetToFields()), // IndicatorReq to be added?
				
			    // publisher_display and pubplace_display from field 260 handled by PubInfoResultSetToField
			    new StandardMARCFieldMaker("publisher_display","260","b",VernMode.COMBINED,"/:,， "),
				new StandardMARCFieldMaker("publisher_t","260","b",VernMode.SEARCH),
				new StandardMARCFieldMaker("publisher_t","264","b",VernMode.SEARCH),
					
				new StandardMARCFieldMaker("pubplace_display","260","a",VernMode.COMBINED,"/:,， "),
				new StandardMARCFieldMaker("pubplace_t","260","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("pubplace_t","264","a",VernMode.SEARCH),

				new StandardMARCFieldMaker("edition_display","250","ab"),
									
				new StandardMARCFieldMaker("title_addl_t","210","ab",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","222","ab",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","242","abnp",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","243","abcdefgklmnopqrs",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_245c_t","245","c",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","246","abcdefgklmnopqrs",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","247","abcdefgnp",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","740","anp",VernMode.SEARCH),

				new StandardMARCFieldMaker("title_uniform_t","130","adfgklmnoprst",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_uniform_t","240","adfgklmnoprs",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_uniform_t","730","tklfnpmoqrs",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_uniform_t","700","tklfnpmors",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_uniform_t","710","tklfnpmors",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_uniform_t","711","tklfnpmors",VernMode.SEARCH),

				new StandardMARCFieldMaker("title_series_t","400","abdfklnptvcegu",VernMode.SEARCH),				
				new StandardMARCFieldMaker("title_series_t","410","abdfklnptvcegu",VernMode.SEARCH),				
				new StandardMARCFieldMaker("title_series_t","411","acdefklnptgquv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","440","anpv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","800","abcdefghklmnopqrstuv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","810","abcdefghklmnopqrstuv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","811","acdefghklnpqstuv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","830","adfghklmnoprstv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","490","anpv",VernMode.SEARCH),

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
							"        ?p rdfs:subPropertyOf marcrdf:MainEntryAuthor.\n"+
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
						"SELECT ?code ?value ?ind2\n" +
						" WHERE { $recordURI$ marcrdf:hasField245 ?f245.\n" +
			    		"        ?f245 marcrdf:hasSubfield ?f245sf .\n" +
			    		"        ?f245sf marcrdf:code ?code.\n" +
			    		"        ?f245sf marcrdf:value ?value.\n" +
					    "        ?f245 marcrdf:ind2 ?ind2 . \n" +
			    		" }").
			    	addResultSetToFields( new TitleResultSetToFields()),
			    new StandardMARCFieldMaker("title_display","245","a",VernMode.SING_VERN,".,;:：/／= "),
			    new StandardMARCFieldMaker("subtitle_display","245","bdefgknpqsv",VernMode.SING_VERN,".,;:：/／ "),
			    new StandardMARCFieldMaker("fulltitle_display","245","abdefgknpqsv",VernMode.SING_VERN,".,;:：/／ "),
			    new StandardMARCFieldMaker("title_responsibility_display","245","c",VernMode.SINGULAR,".,;:：/／ "),
			    new StandardMARCFieldMaker("title_t","245","abdefgknpqsv",VernMode.SEARCH),
			    	
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
			        	"  $recordURI$ marcrdf:hasField856 ?f.\n" +
			        	"  ?f marcrdf:ind1 ?i1.\n" +
			        	"  ?f marcrdf:ind2 ?i2.\n" +
			        	"  ?f marcrdf:hasSubfield ?sf.\n" +
			        	"  ?sf marcrdf:code ?c.\n" +
			        	"  ?sf marcrdf:value ?v. }").
				   addMainStoreQuery("urls_mfhd", 
						"SELECT *\n" +
				    	" WHERE {\n" +
						"  ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n" +
			        	"  ?mfhd marcrdf:hasField856 ?f.\n" +
			        	"  ?f marcrdf:ind1 ?i1.\n" +
			        	"  ?f marcrdf:ind2 ?i2.\n" +
			        	"  ?f marcrdf:hasSubfield ?sf.\n" +
			        	"  ?sf marcrdf:code ?c.\n" +
			        	"  ?sf marcrdf:value ?v. }").
		        addResultSetToFields( new URLResultSetToFields()),
		        
		        new SPARQLFieldMakerImpl().
		        	setName("database codes").
		        	addMainStoreQuery("url instructions", 
		        		"SELECT *\n" +
		        		" WHERE {\n" +
		        		"  $recordURI$ marcrdf:hasField856 ?f.\n" +
		        		"  ?f marcrdf:hasSubfield ?sf.\n" +
		        		"  ?sf marcrdf:code \"i\"^^xsd:string.\n" +
		        		"  ?sf marcrdf:value ?v. }").
		        	addResultSetToFields( new DBCodeRSTF()),
	        
	        
			    new SPARQLFieldMakerImpl().
			        setName("Locations").
			        addMainStoreQuery("location",
			        	"SELECT ?location_name ?library_name ?group_name \n"+
			        	"WHERE {\n"+
                        "    ?hold marcrdf:hasBibliographicRecord $recordURI$.\n"+
			        	"    ?hold marcrdf:hasField852 ?hold852.\n" +
			        	"    ?hold852 marcrdf:hasSubfield ?hold852b.\n" +
			        	"    ?hold852b marcrdf:code \"b\"^^xsd:string.\n" +
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
			        	"}}}").
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
			    	addResultSetToFields( new CitationReferenceNoteResultSetToFields()), //IndicatorReq Needed!
			    
				new StandardMARCFieldMaker("notes","500","a"),
				new StandardMARCFieldMaker("notes_t","500","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","501","a"),
				new StandardMARCFieldMaker("notes_t","501","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","502","a"),
				new StandardMARCFieldMaker("notes_t","502","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","503","a"),
				new StandardMARCFieldMaker("notes_t","503","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","504","ab"),
				new StandardMARCFieldMaker("notes_t","504","ab",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","508","a"),
				new StandardMARCFieldMaker("notes_t","508","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","510","abcux3"),
				new StandardMARCFieldMaker("notes_t","510","abcux3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","513","ab"),
				new StandardMARCFieldMaker("notes_t","513","ab",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","518","adop"),
				new StandardMARCFieldMaker("notes_t","518","adop",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","521","a"),
				new StandardMARCFieldMaker("notes_t","521","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","522","a"),
				new StandardMARCFieldMaker("notes_t","522","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","523","a"),
				new StandardMARCFieldMaker("notes_t","523","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","525","a"),
				new StandardMARCFieldMaker("notes_t","525","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","527","a"),				
				new StandardMARCFieldMaker("notes_t","527","a",VernMode.SEARCH),				
				new StandardMARCFieldMaker("notes","530","abc3"),
				new StandardMARCFieldMaker("notes_t","530","abc3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","533","aebcdfn3"),
				new StandardMARCFieldMaker("notes_t","533","aebcdfn3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","534","abcefmpt"),
				new StandardMARCFieldMaker("notes_t","534","abcefmpt",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","535","abcd3"),
				new StandardMARCFieldMaker("notes_t","535","abcd3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","537","a"),
				new StandardMARCFieldMaker("notes_t","537","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","544","a"),
				new StandardMARCFieldMaker("notes_t","544","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","547","a"),
				new StandardMARCFieldMaker("notes_t","547","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","550","a"),
				new StandardMARCFieldMaker("notes_t","550","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","556","a"),
				new StandardMARCFieldMaker("notes_t","556","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","561","ab3"),
				new StandardMARCFieldMaker("notes_t","561","ab3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","565","a"),
				new StandardMARCFieldMaker("notes_t","565","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","567","a"),
				new StandardMARCFieldMaker("notes_t","567","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","570","a"),
				new StandardMARCFieldMaker("notes_t","570","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","580","a"),
				new StandardMARCFieldMaker("notes_t","580","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","582","a"),
				new StandardMARCFieldMaker("notes_t","582","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","588","a"),
				new StandardMARCFieldMaker("notes_t","588","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","940","a"),
				new StandardMARCFieldMaker("notes_t","940","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","856","m"),
				new StandardMARCFieldMaker("notes_t","856","m",VernMode.SEARCH),
				new StandardMARCFieldMaker("restrictions_display","506","3abce"),
				new StandardMARCFieldMaker("restrictions_display","540","3abcu"),
				new StandardMARCFieldMaker("notes_t","506","3abce",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes_t","540","3abcu",VernMode.SEARCH),
				new StandardMARCFieldMaker("cite_as_display","524","a3"),
				new StandardMARCFieldMaker("notes_t","524","a3",VernMode.SEARCH),
				new StandardMARCFieldMaker("finding_aids_display","555","3abcdu"),
				new StandardMARCFieldMaker("notes_t","555","3abcdu",VernMode.SEARCH),
				new StandardMARCFieldMaker("historical_note_display","545","3abcu"),
				new StandardMARCFieldMaker("notes_t","545","3abcu",VernMode.SEARCH),
				
				new StandardMARCFieldMaker("summary_display","520","ab"),
				new StandardMARCFieldMaker("notes_t","520","ab",VernMode.SEARCH),
				
				new StandardMARCFieldMaker("description_display","300","abcefg"),
				new StandardMARCFieldMaker("description_display","538","a"),
				new StandardMARCFieldMaker("notes_t","538","a",VernMode.SEARCH),
	
				new StandardMARCFieldMaker("subject_era_facet","650","y",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_era_facet","651","y",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_era_facet","654","y",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_era_facet","655","y",VernMode.SEPARATE,"."),

				new StandardMARCFieldMaker("subject_geo_facet","651","a",VernMode.SEPARATE,"."),
				new StandardMARCFieldMaker("subject_geo_facet","650","z",VernMode.SEPARATE,"."),

				new SPARQLFieldMakerImpl().
					setName("fact_or_fiction").
					addMainStoreQuery("fact_or_fiction",
			    		"SELECT (SUBSTR(?val,34,1) as ?char33) \n" +
			    		"WHERE { $recordURI$ marcrdf:hasField008 ?f. \n" +
			    		"        ?f marcrdf:value ?val } \n" ).
					addMainStoreQuery("record_type",
				    	"SELECT (SUBSTR(?l,7,1) as ?char6) \n" +
				   		"WHERE { $recordURI$ marcrdf:leader ?l. } \n").
			    	addResultSetToFields( new FactOrFictionResultSetToFields() ) ,

				new StandardMARCFieldMaker("subject_t","600","abcdefghijklmnopqrstu",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","610","abcdefghijklmnopqrstu",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","611","abcdefghijklmnopqrstu",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","630","abcdefghijklmnopqrst",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","650","abcde",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","651","ae",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","653","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","654","abcde",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","655","abc",VernMode.SEARCH),
				
				new StandardMARCFieldMaker("subject_addl_t","600","abcdefghkjlmnopqrstuvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","610","abcdefghklmnoprstuvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","611","acdefghklnpqstuvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","630","adfghklmnoprstvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","648","avxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","650","abcdvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","651","avwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","653","avwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","654","abevwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","655","avwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","656","akvxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","657","avxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","658","abcd",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","662","abcdfgh",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","692","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","693","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","694","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","695","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","696","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","697","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","698","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_addl_t","699","a",VernMode.SEARCH),
				
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
				
				new StandardMARCFieldMaker("donor_display","541",new IndicatorReq(1,'1'),"a"),
				new StandardMARCFieldMaker("donor_display","902","b"),
				
				new StandardMARCFieldMaker("frequency_display","310","a"),
				new StandardMARCFieldMaker("isbn_display","020","a"),				
				new StandardMARCFieldMaker("issn_display","022","a"),

				new StandardMARCFieldMaker("isbn_t","020","a",VernMode.SEARCH),				
				new StandardMARCFieldMaker("issn_t","022","a",VernMode.SEARCH),
				
			//	new StandardMARCFieldMaker("eightninenine_s","899","a"),
				new StandardMARCFieldMaker("eightninenine_t","899","ab"),
				
				new StandardMARCFieldMaker("other_identifier_display","024","a"),
				new StandardMARCFieldMaker("id_t","024","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("publisher_number_display","028","a"),
				new StandardMARCFieldMaker("id_t","028","a",VernMode.SEARCH),
				
				new StandardMARCFieldMaker("barcode_t","903","p",VernMode.SEARCH),
 
			    new SPARQLFieldMakerImpl().
		    	setName("author display").
			    addMainStoreQuery("main_entry", 
						"SELECT *\n" +
						" WHERE { $recordURI$ ?p ?field.\n" +
						"        ?p rdfs:subPropertyOf marcrdf:MainEntryAuthor.\n"+
			    		"        ?field marcrdf:tag ?tag. \n" +
			    		"        ?field marcrdf:ind1 ?ind1. \n" +
			    		"        ?field marcrdf:ind2 ?ind2. \n" +
			    		"        ?field marcrdf:hasSubfield ?sfield .\n" +
			    		"        ?sfield marcrdf:code ?code.\n" +
			    		"        ?sfield marcrdf:value ?value.\n" +
			    		" }").
		        addResultSetToFields( new AuthorResultSetToFields()),

				new StandardMARCFieldMaker("author_t","100","abcdqegu",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_t","110","abcdefghijklmnopqrstuvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_t","111","abcdefghijklmnopqrstuvwxyz",VernMode.SEARCH),

				new StandardMARCFieldMaker("author_addl_t","700","abcdqegu",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_addl_t","710","abcdefghijklmnopqrstuvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_addl_t","711","abcdefghijklmnopqrstuvwxyz",VernMode.SEARCH),
				
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
