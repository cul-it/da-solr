package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * processing various query results into complicated determination of item format,
 * and "online" status.  
 */
public class FormatResultSetToFields implements ResultSetToFields {

	protected boolean debug = false;
	
	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {

		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.

		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		String category ="";
		String record_type ="";
		String bibliographic_level ="";
		String typeOfContinuingResource = "";
		Boolean isThesis = false;
		Boolean isDatabase = false;
		Collection<String> sf653as = new HashSet<String>();
		Collection<String> sf245hs = new HashSet<String>();
		Collection<String> sf948fs = new HashSet<String>();
		Collection<String> loccodes = new HashSet<String>();
		
		Collection<String> rareLocCodes = new HashSet<String>();
		rareLocCodes.add("asia,ranx");
		rareLocCodes.add("asia,rare");
		rareLocCodes.add("ech,rare");
		rareLocCodes.add("ech,ranx");
		rareLocCodes.add("ent,rare");
		rareLocCodes.add("ent,rar2");
		rareLocCodes.add("gnva,rare");
		rareLocCodes.add("hote,rare");
		rareLocCodes.add("ilr,kanx");
		rareLocCodes.add("ilr,lmdc");
		rareLocCodes.add("ilr,lmdr");
		rareLocCodes.add("ilr,rare");
		rareLocCodes.add("lawr");
		rareLocCodes.add("lawr,anx");
		rareLocCodes.add("mann,spec");
		rareLocCodes.add("rmc");
		rareLocCodes.add("rmc,anx");
		rareLocCodes.add("rmc,hsci");
		rareLocCodes.add("rmc,icer");
		rareLocCodes.add("rmc,ref");
		rareLocCodes.add("sasa,ranx");
		rareLocCodes.add("sasa,rare");
		rareLocCodes.add("vet,rare");
		rareLocCodes.add("was,rare");
		rareLocCodes.add("was,ranx");
		String format = null;
		Boolean online = false;

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if (debug) System.out.println("Result Key: "+resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					Iterator<String> names = sol.varNames();
					while(names.hasNext() ){						
						String name = names.next();
						RDFNode node = sol.get(name);
						if (debug) System.out.println("Field: "+name);
						if (name.equals("cat")) {
							category = nodeToString( node );
							if (debug) System.out.println("category = "+category);
						} else if (name.equals("rectype")) {
							record_type = nodeToString( node );
							if (debug) System.out.println("record_type = "+record_type);
						} else if (name.equals("biblvl")) {
							bibliographic_level = nodeToString( node );
							if (debug) System.out.println("bibliographic_level = "+bibliographic_level);
						} else if (name.equals("sf245h")) {
							sf245hs.add(nodeToString( node ));
							if (debug) System.out.println("sf245h = "+nodeToString( node ));
						} else if (name.equals("sf653a")) {
							sf653as.add(nodeToString( node ));
							if (debug) System.out.println("sf653a = "+nodeToString( node ));
						} else if (name.equals("sf948f")) {
							sf948fs.add(nodeToString( node ));
							if (debug) System.out.println("sf948f = "+nodeToString( node ));
						} else if (name.equals("loccode")) {
							loccodes.add(nodeToString( node ));
							if (debug) System.out.println("location code = "+nodeToString( node ));
						} else if (name.equals("f502")) {
							isThesis = true;
							if (debug) System.out.println("It's a thesis.");
						} else if (name.equals("typeOfContinuingResource")) {
							typeOfContinuingResource = nodeToString( node );
							if (debug) System.out.println("type of continuing resource = "+typeOfContinuingResource);
						}
					}
				}
			}
		}

		Iterator<String> i = sf245hs.iterator();
		while (i.hasNext())
			if (i.next().toLowerCase().contains("[electronic resource]")) {
//				online = true;
//				if (debug) System.out.println("Online status due to 245h: [electronic resource].");
				Iterator<String> j = sf948fs.iterator();
				while (j.hasNext())
					if (j.next().toLowerCase().equals("j")) {
						format = "Journal";
						if (debug) System.out.println("Journal format due to 245h: [electronic resource] and 948f: j.");					
					}			
			}


		i = sf948fs.iterator();
		while (i.hasNext()) {
			String val = i.next();
			if ((val.equals("fd")) || (val.equals("webfeatdb"))) {
				format = "Database";
				if (debug) System.out.println("format:Database due to 948f: webfeatdb OR fd.");
				
				// format:Database differs from database_b flag.
				// The latter is used for Database ERMS.
				if (val.equals("webfeatdb")) {
					isDatabase = true;
					if (debug) System.out.println("Database_b true due to 948f: webfeatdb.");
				}
			}
		}

		i = sf653as.iterator();
		while (i.hasNext()) {
			String val = i.next();
			if (val.equalsIgnoreCase("research guide")) {
				format = "Research Guide";
				if (debug) System.out.println("format:Research Guide due to 653a: research guide.");
			} else if (val.equalsIgnoreCase("course guide")) {
				format = "Course Guide";
				if (debug) System.out.println("format:Course Guide due to 653a: course guide.");
			} else if (val.equalsIgnoreCase("library guide")) {
				format = "Library Guide";
				if (debug) System.out.println("format:Library Guide due to 653a: library guide.");
			}
		}

/*		i = sf948fs.iterator();
 *		while (i.hasNext())
 *			if (i.next().toLowerCase().equals("ebk")) {
 *				online = true;
 *				if (debug) System.out.println("Online due to 948f: ebk.");
 *			}
 */
		if (format == null) {
			if (record_type.equals("a")) {
				if ((bibliographic_level.equals("a"))
						|| (bibliographic_level.equals("m"))
						|| (bibliographic_level.equals("d"))
						|| (bibliographic_level.equals("c")) ) {
					format = "Book";
					if (debug) System.out.println("Book due to record_type:a and bibliographic_level in: a,m,d,c.");
				} else if ((bibliographic_level.equals("b"))
						|| (bibliographic_level.equals("s"))) {
					format = "Journal";
					if (debug) System.out.println("Journal due to record_type:a and bibliographic_level in: b,s.");
				} else if (bibliographic_level.equals("i")) {
					if (typeOfContinuingResource.equals("w")) {
						format = "Website";
						if (debug) System.out.println("Website due to record_type:a, bibliographic_level:i and typeOfContinuingResource:w.");
					} else if (typeOfContinuingResource.equals("m")) {
						format = "Book";
						if (debug) System.out.println("Book due to record_type:a, bibliographic_level:i and typeOfContinuingResource:m.");
					} else if (typeOfContinuingResource.equals("d")) {
						format = "Database";
						if (debug) System.out.println("Database due to record_type:a, bibliographic_level:i and typeOfContinuingResource:d.");
					} else if (typeOfContinuingResource.equals("n") || typeOfContinuingResource.equals("p")) {
						format = "Journal";
						if (debug) System.out.println("Journal due to record_type:a, bibliographic_level:i and typeOfContinuingResource in:n,p.");
					}
				}
			} else if (record_type.equals("t")) {
				if (bibliographic_level.equals("a")) {
					format = "Book";
					if (debug) System.out.println("Book due to record_type:t and bibliographic_level: a.");
				}
			} else if ((record_type.equals("c"))
					|| (record_type.equals("d"))) {
				format = "Musical Score";
				if (debug) System.out.println("Musical Score due to record_type: c or d.");
			} else if ((record_type.equals("e"))
					|| (record_type.equals("f"))) {
				format = "Map or Globe";
				if (debug) System.out.println("Map or Guide due to record_type: e or f.");
			} else if (record_type.equals("g")) {
				format = "Video";
				if (debug) System.out.println("Video due to record_type: g.");
			} else if (record_type.equals("i")) {
				format = "Non-musical Recording";
				if (debug) System.out.println("Non-musical Recording due to record_type: i.");
			} else if (record_type.equals("j")) {
				format = "Musical Recording";
				if (debug) System.out.println("Musical Recording due to record_type: j.");
			} else if (record_type.equals("k")) {
				format = "Image";
				if (debug) System.out.println("Image due to record_type: k.");
			} else if (record_type.equals("m")) {
				if (sf948fs.contains("evideo")) {
					format = "Video";
					if (debug) System.out.println("Video due to record_type: m and 948f: evideo.");
				} else if (sf948fs.contains("eaudio")) {
					format = "Musical Recording";
					if (debug) System.out.println("Musical Recording due to record_type: m and 948f: eaudio.");
				} else if (sf948fs.contains("escore")) {
					format = "Musical Score";
					if (debug) System.out.println("Musical Score due to record_type: m and 948f: escore.");
				} else {
					format = "Computer File";
					if (debug) System.out.println("Computer File due to record_type: m and 948f not in: eaudio, evideo, escore.");
				}
			} else if (record_type.equals("o")) {
				format = "Kit";
				if (debug) System.out.println("Kit due to record_type: o.");
			} else if (record_type.equals("p")) { // p means "mixed materials", classifying as 
				Iterator<String> iter = loccodes.iterator();   // archival if in rare location
				while (iter.hasNext()) {
					String loccode = iter.next();
					if (rareLocCodes.contains(loccode)) {
						format = "Manuscript/Archive"; //ARCHIVAL
						if (debug) System.out.println("Manuscript/Archive due to record_type: p and loccode on rare location list.");
						break;
					}
				}
				if (debug)
					if ( format == null )
						System.out.println("format:Miscellaneous due to record_type: p and no rare location code.");
			} else if (record_type.equals("r")) {
				format = "Object";
				if (debug) System.out.println("Object due to record_type: r.");
			}
			if (format == null) {
				if (record_type.equals("t")) {
					format = "Manuscript/Archive"; // This includes all bibliographic_levels but 'a',
												   //captured above. MANUSCRIPT
					if (debug) System.out.println("Manuscript/Archive due to record_type: t.");
				} else if (category.equals("h")) {
					format = "Microform";
					if (debug) System.out.println("Microform due to category:h.");
				} else if (category.equals("q")) {
					format = "Musical Score";
					if (debug) System.out.println("Musical Score due to category:q.");
				} else if (category.equals("v")) {
					format = "Video";
					if (debug) System.out.println("Video due to category:v.");
				} else {
					format = "Miscellaneous";
					if (debug) System.out.println("format:Miscellaneous due to no format conditions met.");
				}
			}
		}
		
		
		if (online) {
			System.out.println("Online flag incorrectly applied for reason other than 'serv,remo' flag in mfhd.");
		}
		
		if (loccodes.contains("serv,remo")) {
			online = true;
			if (debug) System.out.println("Online due to loccode: serv,remo.");
		}		

		if (isThesis) {  //Thesis is an "additional" format, and won't override main format entry.
			addField(fields,"format","Thesis");
			if (format.equals("Manuscript/Archive")) {
				format = null;
				if (debug) System.out.println("Not Manuscript/Archive due to collision with format:Thesis.");				
			}
		}
		if (format != null) {
			addField(fields,"format",format);
			addField(fields,"format_main_facet",format);
		} else {
			if (isThesis)
				addField(fields,"format_main_facet","Thesis");
		}
		if (online) {
			addField(fields,"online","Online");
		}
		SolrInputField dbField = new SolrInputField("database_b");
		dbField.setValue(isDatabase, 1.0f);
		fields.put("database_b", dbField);
		return fields;

	}


	private String nodeToString( RDFNode node){
		if( node == null )
			return "";
		else if ( node.canAs( Literal.class )){
			return ((Literal)node).getLexicalForm();			
		}else {
			return node.toString();
		}
	}

}
