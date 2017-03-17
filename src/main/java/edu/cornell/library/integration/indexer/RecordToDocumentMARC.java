package edu.cornell.library.integration.indexer;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.documentPostProcess.BarcodeSearch;
import edu.cornell.library.integration.indexer.documentPostProcess.Collections;
import edu.cornell.library.integration.indexer.documentPostProcess.DocumentPostProcess;
import edu.cornell.library.integration.indexer.documentPostProcess.MissingTitleReport;
import edu.cornell.library.integration.indexer.documentPostProcess.ModifyCallNumbers;
import edu.cornell.library.integration.indexer.documentPostProcess.NoAccessUrlsUnlessOnline;
import edu.cornell.library.integration.indexer.documentPostProcess.RecordBoost;
import edu.cornell.library.integration.indexer.documentPostProcess.RemoveDuplicateTitleData;
import edu.cornell.library.integration.indexer.documentPostProcess.SingleValueField;
import edu.cornell.library.integration.indexer.documentPostProcess.SingleValueField.Correction;
import edu.cornell.library.integration.indexer.documentPostProcess.SuppressUnwantedValues;
import edu.cornell.library.integration.indexer.documentPostProcess.UpdateSolrInventoryDB;
import edu.cornell.library.integration.indexer.fieldMaker.FieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.IndicatorReq;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerStepped;
import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker;
import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker.VernMode;
import edu.cornell.library.integration.indexer.resultSetToFields.AllResultsToField;
import edu.cornell.library.integration.indexer.resultSetToFields.AuthorTitle;
import edu.cornell.library.integration.indexer.resultSetToFields.CitationReferenceNote;
import edu.cornell.library.integration.indexer.resultSetToFields.DBCode;
import edu.cornell.library.integration.indexer.resultSetToFields.Date;
import edu.cornell.library.integration.indexer.resultSetToFields.FactOrFiction;
import edu.cornell.library.integration.indexer.resultSetToFields.FindingAids;
import edu.cornell.library.integration.indexer.resultSetToFields.Format;
import edu.cornell.library.integration.indexer.resultSetToFields.HathiLinks;
import edu.cornell.library.integration.indexer.resultSetToFields.HoldingsAndItems;
import edu.cornell.library.integration.indexer.resultSetToFields.ISBN;
import edu.cornell.library.integration.indexer.resultSetToFields.Instrumentation;
import edu.cornell.library.integration.indexer.resultSetToFields.Language;
import edu.cornell.library.integration.indexer.resultSetToFields.MARC;
import edu.cornell.library.integration.indexer.resultSetToFields.NewBooks;
import edu.cornell.library.integration.indexer.resultSetToFields.PubInfo;
import edu.cornell.library.integration.indexer.resultSetToFields.RecordType;
import edu.cornell.library.integration.indexer.resultSetToFields.SeriesAddedEntry;
import edu.cornell.library.integration.indexer.resultSetToFields.Subject;
import edu.cornell.library.integration.indexer.resultSetToFields.TOC;
import edu.cornell.library.integration.indexer.resultSetToFields.Title130;
import edu.cornell.library.integration.indexer.resultSetToFields.TitleChange;
import edu.cornell.library.integration.indexer.resultSetToFields.URL;
import edu.cornell.library.integration.indexer.resultSetToFields.CallNumber;
import edu.cornell.library.integration.indexer.resultSetToFieldsStepped.TitleSeries;

/**
 * An example RecordToDocument implementation. 
 */
public class RecordToDocumentMARC extends RecordToDocumentBase {
	
	
	@Override
	List<? extends DocumentPostProcess> getDocumentPostProcess() {
		return Arrays.asList(				
				new SingleValueField("pub_date_sort",Correction.firstValue),
				new SingleValueField("author_display",Correction.firstValue),
				new SingleValueField("author_sort",Correction.firstValue),
				new SingleValueField("format_main_facet",Correction.firstValue),
				new RecordBoost(),
				new SuppressUnwantedValues(),
				new MissingTitleReport(),
				new ModifyCallNumbers(),
				new BarcodeSearch(),
				new RemoveDuplicateTitleData(),
				new NoAccessUrlsUnlessOnline(),
				new Collections(),
				new UpdateSolrInventoryDB()
		);
	}

	@Override
	List<? extends FieldMaker> getFieldMakers() {
		return Arrays.asList(

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
				    addMainStoreQuery("description",standardDataFieldSPARQL("300")).
				    addMainStoreQuery("rectypebiblvl",
						"SELECT (SUBSTR(?leader,7,2) as ?rectypebiblvl)\n" +
						" WHERE { $recordURI$ marcrdf:leader ?leader. }").
		            addResultSetToFields( new HoldingsAndItems()),
		        /*
		        new SPARQLFieldMakerImpl().
		        	addMainStoreQuery("bib_id","SELECT ?id WHERE { $recordURI$ rdfs:label ?id}").
		        	addResultSetToFields( new TitleMatchRSTF()),
		        	*/
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
				    		standardDataFieldGroupSPARQL("marcrdf:DataFields")).
				    addResultSetToFields( new MARC() ),

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
				    addResultSetToFields( new RecordType() ),                  
				    		
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
					addResultSetToFields( new Format() ),

				getLanguageFieldMaker(),

			    new SPARQLFieldMakerImpl().
			        setName("call_numbers").
			        addMainStoreQuery("holdings_callno",
			        	"SELECT ?part1 ?part2 ?prefix ?ind1 ?loc\n"+
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
				    addMainStoreQuery("bib_050callno",
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
				    addMainStoreQuery("bib_950callno",
				    		"SELECT ?part1 ?part2\n"+
				    		"WHERE {\n"+
				            "  $recordURI$ marcrdf:hasField950 ?f950.\n" +
				            "  ?f950 marcrdf:hasSubfield ?f950a.\n" +
				            "  ?f950a marcrdf:code \"a\"^^xsd:string.\n" +
				            "  ?f950a marcrdf:value ?part1.\n" +
				            "  OPTIONAL {\n" +
				            "    ?f950 marcrdf:hasSubfield ?f950b.\n" +
				            "    ?f950b marcrdf:code \"b\"^^xsd:string.\n" +
				            "    ?f950b marcrdf:value ?part2. }\n" +
				    		"}").
				    addResultSetToFields( new CallNumber() ),
			    	
				new SPARQLFieldMakerImpl().
				    setName("publication_date").
				    addMainStoreQuery("machine_dates",standardControlFieldSPARQL("008")).
				    addMainStoreQuery("human_dates_260",
				    		"SELECT ?date \n" +
				    		"WHERE { $recordURI$ marcrdf:hasField260 ?f. \n" +
				    		"        ?f marcrdf:hasSubfield ?s. \n" +
				    		"        ?s marcrdf:code \"c\"^^xsd:string. \n" +
				    		"        ?s marcrdf:value ?date. \n" +
				    		" } ").
				    addMainStoreQuery("human_dates_264",
				    		"SELECT ?date ?ind2 \n" +
				    		"WHERE { $recordURI$ marcrdf:hasField264 ?f. \n" +
				    		"        ?f marcrdf:hasSubfield ?s. \n" +
				    		"        ?s marcrdf:code \"c\"^^xsd:string. \n" +
				    		"        ?s marcrdf:value ?date. \n" +
				    		"        ?f marcrdf:ind2 ?ind2. \n" +
				    		" } ").
				    addResultSetToFields( new Date() ) ,
				    
				new StandardMARCFieldMaker("pub_info_display","260","3abc"),
			    new SPARQLFieldMakerImpl().
			    	setName("pub_info_264").
			    	addMainStoreQuery("pub_info",standardDataFieldSPARQL("264")).
			    	addResultSetToFields( new PubInfo() ), // IndicatorReq to be added?
				
			    // publisher_display and pubplace_display from field 260 handled by PubInfo()
			    new StandardMARCFieldMaker("publisher_display","260","b",VernMode.COMBINED,"/:,， "),
				new StandardMARCFieldMaker("publisher_t","260","b",VernMode.SEARCH),
				new StandardMARCFieldMaker("publisher_t","264","b",VernMode.SEARCH),
					
				new StandardMARCFieldMaker("pubplace_display","260","a",VernMode.COMBINED,"/:,， "),
				new StandardMARCFieldMaker("pubplace_t","260","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("pubplace_t","264","a",VernMode.SEARCH),

				new StandardMARCFieldMaker("edition_display","250","3ab"),
									
				new StandardMARCFieldMaker("title_addl_t","210","ab",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","222","ab",VernMode.SEARCH,true),
				new StandardMARCFieldMaker("title_addl_t","242","abnp",VernMode.SEARCH,true),
				new StandardMARCFieldMaker("title_addl_t","243","abcdefgklmnopqrs",VernMode.SEARCH,true),
				new StandardMARCFieldMaker("author_245c_t","245","c",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","246","abcdefgklmnopqrs",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","247","abcdefgnp",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_addl_t","740","anp",VernMode.SEARCH,true),

				new StandardMARCFieldMaker("title_series_t","400","abdfklnptvcegu",VernMode.SEARCH),				
				new StandardMARCFieldMaker("title_series_t","410","abdfklnptvcegu",VernMode.SEARCH),				
				new StandardMARCFieldMaker("title_series_t","411","acdefklnptgquv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","440","anpv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","800","abcdefghklmnopqrstuv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","810","abcdefghklmnopqrstuv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","811","acdefghklnpqstuv",VernMode.SEARCH),
				new StandardMARCFieldMaker("title_series_t","830","adfghklmnoprstv",VernMode.SEARCH,true),
				new StandardMARCFieldMaker("title_series_t","490","anpv",VernMode.SEARCH),

				new StandardMARCFieldMaker("title_other_display","243","adfgklmnoprs",":/ "),
				new StandardMARCFieldMaker("title_other_display","246","iabfnpg",":/ "),
				new StandardMARCFieldMaker("continues_display","247","abfgnpx",":/ "),

				new SPARQLFieldMakerImpl().
					setName("title130").
					addMainStoreQuery("title_130",standardDataFieldSPARQL("130")).
					addResultSetToFields( new Title130() ),

			    new SPARQLFieldMakerImpl().
			        setName("title_changes").
			        addMainStoreQuery("added_entry",
			        		standardDataFieldGroupSPARQL("marcrdf:AddedEntry")).
			        addMainStoreQuery("linking_entry",
			        		standardDataFieldGroupSPARQL("marcrdf:LinkingEntry")).
			        addResultSetToFields( new TitleChange() ),
			    new StandardMARCFieldMaker("map_format_display","255","abcdefg"),
			    new StandardMARCFieldMaker("in_display","773","abdghikmnopqrstuw"),

			        
			    new SPARQLFieldMakerStepped().
			        setName("title_series_display").
			        addMainStoreQuery("title_series_830",standardDataFieldSPARQL("830")).
			        addResultSetToFieldsStepped( new TitleSeries() ),

			    new SPARQLFieldMakerImpl().
			    	setName("author and main title").
			        addMainStoreQuery("title",standardDataFieldSPARQL("245")).
					addMainStoreQuery("title_240",standardDataFieldSPARQL("240")).
				    addMainStoreQuery("main_entry",
			        		standardDataFieldGroupSPARQL("marcrdf:MainEntryAuthor")).
			        addResultSetToFields( new AuthorTitle() ),

				new SPARQLFieldMakerImpl().
			    	setName("seriesaddedentry").
			    	addMainStoreQuery("seriesaddedentry",
			    			standardDataFieldGroupSPARQL("marcrdf:SeriesAddedEntry")).
				    addResultSetToFields( new SeriesAddedEntry() ),


			    new StandardMARCFieldMaker("author_t","100","abcdqegu",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_t","110","abcdefghijklmnopqrstuvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_t","111","abcdefghijklmnopqrstuvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_addl_t","700","abcdqegu",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_addl_t","710","abcdefghijklmnopqrstuvwxyz",VernMode.SEARCH),
				new StandardMARCFieldMaker("author_addl_t","711","abcdefghijklmnopqrstuvwxyz",VernMode.SEARCH),
    
			        
			    new SPARQLFieldMakerImpl().
			        setName("table of contents").
			        addMainStoreQuery("table of contents",standardDataFieldSPARQL("505")).
			        addResultSetToFields( new TOC() ),

			   new SPARQLFieldMakerImpl().
			   		setName("urls").
			       addMainStoreQuery("urls",standardDataFieldSPARQL("856")).
				   addMainStoreQuery("urls_mfhd",standardHoldingsDataFieldSPARQL("856")).
		        addResultSetToFields( new URL() ),

		        getHathiLinks(),

		        
		        new SPARQLFieldMakerImpl().
		        	setName("database codes").
		        	addMainStoreQuery("url instructions", 
		        		"SELECT ?v\n" +
		        		" WHERE {\n" +
		        		"  $recordURI$ marcrdf:hasField856 ?f.\n" +
		        		"  ?f marcrdf:hasSubfield ?sf.\n" +
		        		"  ?sf marcrdf:code \"i\"^^xsd:string.\n" +
		        		"  ?sf marcrdf:value ?v. }").
		        	addMainStoreQuery("record-level instructions", 
			        		"SELECT ?v\n" +
			        		" WHERE {\n" +
			        		"  $recordURI$ marcrdf:hasField899 ?f.\n" +
			        		"  ?f marcrdf:hasSubfield ?sf.\n" +
			        		"  ?sf marcrdf:code \"a\"^^xsd:string.\n" +
			        		"  ?sf marcrdf:value ?v. }").
		        	addResultSetToFields( new DBCode()),

			    new SPARQLFieldMakerImpl().
			    	setName("citation_reference_note").
			    	addMainStoreQuery("field510",standardDataFieldSPARQL("510")).
			    	addResultSetToFields( new CitationReferenceNote() ), //IndicatorReq Needed!
			    new SPARQLFieldMakerImpl().
			    	setName("instrumentation").
			    	addMainStoreQuery("instrumentation",standardDataFieldSPARQL("382")).
			    	addResultSetToFields( new Instrumentation()),
			    
			    new StandardMARCFieldMaker("notes","362","a"),
				new StandardMARCFieldMaker("notes_t","362","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","500","3a"),
				new StandardMARCFieldMaker("notes_t","500","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","501","3a"),
				new StandardMARCFieldMaker("notes_t","501","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("thesis_display","502","3abcdgo"),
				new StandardMARCFieldMaker("notes_t","502","abcdgo",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","503","3a"),
				new StandardMARCFieldMaker("notes_t","503","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","504","3ab"),
				new StandardMARCFieldMaker("notes_t","504","ab",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","508","3a"),
				new StandardMARCFieldMaker("notes_t","508","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","510","abcux3"),
				new StandardMARCFieldMaker("notes_t","510","abcux3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","511","3a"),
				new StandardMARCFieldMaker("notes_t","511","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","513","3ab"),
				new StandardMARCFieldMaker("notes_t","513","ab",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","515","a"),
				new StandardMARCFieldMaker("notes_t","515","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","518","3adop"),
				new StandardMARCFieldMaker("notes_t","518","adop",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","521","3a"),
				new StandardMARCFieldMaker("notes_t","521","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","522","3a"),
				new StandardMARCFieldMaker("notes_t","522","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","523","3a"),
				new StandardMARCFieldMaker("notes_t","523","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","525","3a"),
				new StandardMARCFieldMaker("notes_t","525","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","527","3a"),				
				new StandardMARCFieldMaker("notes_t","527","a",VernMode.SEARCH),				
				new StandardMARCFieldMaker("notes","530","abc3"),
				new StandardMARCFieldMaker("notes_t","530","abc3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","533","aebcdfn3"),
				new StandardMARCFieldMaker("notes_t","533","aebcdfn3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","534","3abcefmpt"),
				new StandardMARCFieldMaker("notes_t","534","abcefmpt",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","535","abcd3"),
				new StandardMARCFieldMaker("notes_t","535","abcd3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","537","3a"),
				new StandardMARCFieldMaker("notes_t","537","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","544","3ad"),
				new StandardMARCFieldMaker("notes_t","544","ad",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","547","3a"),
				new StandardMARCFieldMaker("notes_t","547","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","550","3a"),
				new StandardMARCFieldMaker("notes_t","550","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","556","3a"),
				new StandardMARCFieldMaker("notes_t","556","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","561","ab3"),
				new StandardMARCFieldMaker("notes_t","561","ab3",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","565","3a"),
				new StandardMARCFieldMaker("notes_t","565","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","567","3a"),
				new StandardMARCFieldMaker("notes_t","567","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","570","3a"),
				new StandardMARCFieldMaker("notes_t","570","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","580","3a"),
				new StandardMARCFieldMaker("notes_t","580","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("notes","582","3a"),
				new StandardMARCFieldMaker("notes_t","582","a",VernMode.SEARCH),
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
			    new SPARQLFieldMakerImpl().
			    	setName("finding_aids_index_notes").
			    	addMainStoreQuery("finding_index_notes",standardDataFieldSPARQL("555")).
			    	addResultSetToFields( new FindingAids()),
				new StandardMARCFieldMaker("historical_note_display","545","3abcu"),
				new StandardMARCFieldMaker("notes_t","545","3abcu",VernMode.SEARCH),
				
				new StandardMARCFieldMaker("summary_display","520","3abc"),
				new StandardMARCFieldMaker("notes_t","520","abc",VernMode.SEARCH),
				
				new StandardMARCFieldMaker("description_display","300","3abcefg"),
				new StandardMARCFieldMaker("description_display","538","3a"),
				new StandardMARCFieldMaker("notes_t","538","a",VernMode.SEARCH),

				new SPARQLFieldMakerImpl().
					setName("fact_or_fiction").
					addMainStoreQuery("fact_or_fiction",
			    		"SELECT (SUBSTR(?val,34,1) as ?char33) \n" +
			    		"WHERE { $recordURI$ marcrdf:hasField008 ?f. \n" +
			    		"        ?f marcrdf:value ?val } \n" ).
					addMainStoreQuery("record_type",
				    	"SELECT (SUBSTR(?l,7,2) as ?char67) \n" +
				   		"WHERE { $recordURI$ marcrdf:leader ?l. } \n").
			    	addResultSetToFields( new FactOrFiction() ) ,

		    	// subject_t is essentially a boost field. Should eventually be dropped or
		    	// population moved to SubjectRSTF.
				new StandardMARCFieldMaker("subject_t","600","abcdefghijklmnopqrstu",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","610","abcdefghijklmnopqrstu",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","611","abcdefghijklmnopqrstu",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","630","abcdefghijklmnopqrst",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","650","abcde",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","651","ae",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","653","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","654","abcde",VernMode.SEARCH),
				new StandardMARCFieldMaker("subject_t","655","abc",VernMode.SEARCH),

			    new SPARQLFieldMakerImpl().
			    	setName("subject").
			    	addMainStoreQuery("subjects",
			    			standardDataFieldGroupSPARQL("marcrdf:SubjectTermEntry")).
		        	addResultSetToFields( new Subject() ),

			  new SPARQLFieldMakerImpl().
			    	setName("newbooks").
			    	addMainStoreQuery("newbooks948",
					"SELECT ?ind1 ?a\n" +
					" WHERE { $recordURI$ marcrdf:hasField948 ?field.\n" +
				    "        ?field marcrdf:ind1 \"1\"^^xsd:string. \n" +
				    "        ?field marcrdf:hasSubfield ?sfield .\n" +
				    "        ?sfield marcrdf:code \"a\"^^xsd:string .\n" +
				    "        ?sfield marcrdf:value ?a.\n" +
				    " }").
			    	addMainStoreQuery("newbooksMfhd",
					"SELECT ?five ?code ?x ?z\n" +
					" WHERE { ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n" +
				   	"  ?mfhd marcrdf:hasField852 ?mfhd852.\n" +
				    "  ?mfhd852 marcrdf:hasSubfield ?mfhd852b.\n" +
				    "  ?mfhd852b marcrdf:code \"b\"^^xsd:string.\n" +
                    "  ?mfhd852b marcrdf:value ?code. \n" +
				    "  ?mfhd marcrdf:hasField005 ?mfhd005.\n"+
                    "  ?mfhd005 marcrdf:value ?five. \n" +
				    "  OPTIONAL {\n" +
				    "    ?mfhd852 marcrdf:hasSubfield ?mfhd852x.\n" +
				    "    ?mfhd852x marcrdf:code \"x\"^^xsd:string.\n" +
                    "    ?mfhd852x marcrdf:value ?x.  }\n"+
				    "  OPTIONAL {\n" +
				    "    ?mfhd852 marcrdf:hasSubfield ?mfhd852z.\n" +
				    "    ?mfhd852z marcrdf:code \"z\"^^xsd:string.\n" +
                    "    ?mfhd852z marcrdf:value ?z.  }\n"+
                    "}").
					addMainStoreQuery("seven",
							"SELECT (SUBSTR(?seven,1,1) as ?cat)\n" +
							" WHERE { $recordURI$ marcrdf:hasField007 ?f.\n" +
							"          ?f marcrdf:value ?seven. }").
					addMainStoreQuery("k",
						"SELECT ?callnumprefix\n" +
						" WHERE { ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n" +
					   	"  ?mfhd marcrdf:hasField852 ?mfhd852.\n" +
					    "  ?mfhd852 marcrdf:hasSubfield ?mfhd852k.\n" +
					    "  ?mfhd852k marcrdf:code \"k\"^^xsd:string.\n" +
				        "  ?mfhd852k marcrdf:value ?callnumprefix. \n" +
						" }").
		        	addResultSetToFields( new NewBooks() ),

		        	
				new StandardMARCFieldMaker("donor_display","541",new IndicatorReq(1," 1"),"3ac"),
				new StandardMARCFieldMaker("donor_display","902","b"),

				new StandardMARCFieldMaker("frequency_display","310","a"),

				new SPARQLFieldMakerImpl().
					setName("isbn").
					addMainStoreQuery("isbn",standardDataFieldSPARQL("020")).
					addResultSetToFields( new ISBN()),

		    	new StandardMARCFieldMaker("issn_display","022","a"),
				new StandardMARCFieldMaker("issn_t","022","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("issn_t","022","l",VernMode.SEARCH),

				new StandardMARCFieldMaker("eightninenine_t","899","ab"),
				new StandardMARCFieldMaker("eightninenine_display","899","ab"),

				new StandardMARCFieldMaker("other_identifier_display","024","a"),
				new StandardMARCFieldMaker("id_t","024","a",VernMode.SEARCH),
				new StandardMARCFieldMaker("publisher_number_display","028","a"),
				new StandardMARCFieldMaker("id_t","028","a",VernMode.SEARCH),
				
				new StandardMARCFieldMaker("barcode_t","903","p",VernMode.SEARCH)
 

		);
	}
	
	public static SPARQLFieldMakerImpl getHathiLinks() {
		return new SPARQLFieldMakerImpl().
	    setName("hathi links").
	    addMainStoreQuery("oclcid",
	    		"SELECT ?thirtyfive \n" +
	    		"WHERE { $recordURI$ marcrdf:hasField035 ?f. \n" +
	    		"        ?f marcrdf:hasSubfield ?s."
	    		+ "      ?s marcrdf:code \"a\"^^xsd:string."
	    		+ "      ?s marcrdf:value ?thirtyfive } \n" ).
	    addMainStoreQuery("903_barcode",
	    		"SELECT ?barcode \n" +
	    		"WHERE { $recordURI$ marcrdf:hasField903 ?f. \n" +
	    		"        ?f marcrdf:hasSubfield ?s."
	    		+ "      ?s marcrdf:code \"p\"^^xsd:string."
	    		+ "      ?s marcrdf:value ?barcode } \n" ).
	    addResultSetToFields( new HathiLinks() );

	}

	public static SPARQLFieldMakerImpl getLanguageFieldMaker() {
		return new SPARQLFieldMakerImpl().
	    setName("language").
	    addMainStoreQuery("language_008",standardControlFieldSPARQL("008")).
	    addMainStoreQuery("language_note",standardDataFieldSPARQL("546")).
	    addMainStoreQuery("languages_041",standardDataFieldSPARQL("041")).
	    addResultSetToFields( new Language() );
	}

	private static String standardControlFieldSPARQL( String tag ){
		return
		"SELECT * WHERE {\n"+
		"  $recordURI$ marcrdf:hasField"+tag+" ?field.\n" +
		"  ?field marcrdf:tag ?tag. \n" +
		"  ?field marcrdf:value ?value. }";
	}
	private static String standardDataFieldSPARQL( String tag ){
		return
	    "SELECT * WHERE {\n" +
	   	"  $recordURI$ marcrdf:hasField"+tag+" ?field.\n" +
 		"  ?field marcrdf:tag ?tag. \n" +
	   	"  ?field marcrdf:ind1 ?ind1. \n" +
	   	"  ?field marcrdf:ind2 ?ind2. \n" +
	   	"  ?field marcrdf:hasSubfield ?sfield .\n" +
		"  ?sfield marcrdf:code ?code.\n" +
	   	"  ?sfield marcrdf:value ?value. }";
	}
	private static String standardHoldingsDataFieldSPARQL( String tag ){
		return
		"SELECT * WHERE {\n" +
		"  ?mfhd marcrdf:hasBibliographicRecord $recordURI$.\n" +
		"  ?mfhd marcrdf:hasField"+tag+" ?field.\n" +
		"  ?field marcrdf:tag ?tag.\n" +
		"  ?field marcrdf:ind1 ?ind1.\n" +
		"  ?field marcrdf:ind2 ?ind2.\n" +
		"  ?field marcrdf:hasSubfield ?sfield.\n" +
		"  ?sfield marcrdf:code ?code.\n" +
		"  ?sfield marcrdf:value ?value. }";
	}
	private static String standardDataFieldGroupSPARQL( String group ){
		return
		"SELECT * WHERE {"+
		"  $recordURI$ ?p ?field.\n" +
		"  ?p rdfs:subPropertyOf "+group+".\n"+
	    "  ?field marcrdf:tag ?tag. \n" +
		"  ?field marcrdf:ind1 ?ind1. \n" +
		"  ?field marcrdf:ind2 ?ind2. \n" +
		"  ?field marcrdf:hasSubfield ?sfield .\n" +
		"  ?sfield marcrdf:code ?code.\n" +
		"  ?sfield marcrdf:value ?value. }";
	}

}
