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
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class FormatResultSetToFields implements ResultSetToFields {

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
		Collection<String> sf653as = new HashSet<String>();
		Collection<String> sf245hs = new HashSet<String>();
		Collection<String> sf948fs = new HashSet<String>();
		Collection<String> loccodes = new HashSet<String>();
		String format = null;
		Boolean online = false;

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					Iterator<String> names = sol.varNames();
					while(names.hasNext() ){						
						String name = names.next();
						RDFNode node = sol.get(name);
						if (name.equals("cat")) {
							category = nodeToString( node );
						} else if (name.equals("rectype")) {
							record_type = nodeToString( node );
						} else if (name.equals("biblvl")) {
							bibliographic_level = nodeToString( node );
						} else if (name.equals("sf245h")) {
							sf245hs.add(nodeToString( node ));
						} else if (name.equals("sf653a")) {
							sf653as.add(nodeToString( node ));
						} else if (name.equals("sf948f")) {
							sf948fs.add(nodeToString( node ));
						} else if (name.equals("loccode")) {
							loccodes.add(nodeToString( node ));
						}
					}
				}
			}
		}

		Iterator<String> i = sf245hs.iterator();
		while (i.hasNext())
			if (i.next().toLowerCase().contains("[electronic resource]"))
				online = true;

		i = sf948fs.iterator();
		while (i.hasNext())
			if (i.next().toLowerCase().equals("j"))
				if (online) //i.e. If 245h said [electronic resource]
					format = "Journal";

		i = sf948fs.iterator();
		while (i.hasNext()) {
			String val = i.next();
			if ((val.equals("fd")) || (val.equals("webfeatdb"))) {
				format = "Database";
				online = true;
			}
		}

		i = sf653as.iterator();
		while (i.hasNext()) {
			String val = i.next();
			if (val.equalsIgnoreCase("research guide")) {
				format = "Research Guide";
				online = true;
			} else if (val.equalsIgnoreCase("course guide")) {
				format = "Course Guide";
				online = true;
			} else if (val.equalsIgnoreCase("library guide")) {
				format = "Library Guide";
				online = true;
			}
		}

		i = sf948fs.iterator();
		while (i.hasNext())
			if (i.next().toLowerCase().equals("ebk"))
				online = true;

		if (format == null) {
			if (record_type.equals("a")) {
				if ((bibliographic_level.equals("a"))
						|| (bibliographic_level.equals("m"))
						|| (bibliographic_level.equals("d"))
						|| (bibliographic_level.equals("c")) ) {
					format = "Book";
				} else if ((bibliographic_level.equals("b"))
						|| (bibliographic_level.equals("s"))) {
					format = "Journal";
				}
			} else if (record_type.equals("t")) {
				if ((bibliographic_level.equals("a"))
						|| (bibliographic_level.equals("m"))) {
					format = "Book";
				}
			} else if ((record_type.equals("c"))
					|| (record_type.equals("d"))) {
				format = "Musical Score";
			} else if ((record_type.equals("e"))
					|| (record_type.equals("f"))) {
				format = "Map or Globe";
			} else if (record_type.equals("g")) {
				format = "Video";
			} else if (record_type.equals("i")) {
				format = "Non-musical Recording";
			} else if (record_type.equals("j")) {
				format = "Musical Recording";
			} else if (record_type.equals("k")) {
				format = "Image";
			} else if (record_type.equals("m")) {
				format = "Computer File";
			} else if (record_type.equals("o")) {
				format = "Kit";
				//			} else if (record_type.equals("p")) { // p is either mixed-materials
				//				format = "Manuscript";            // or manuscript depending on source
			} else if (record_type.equals("t")) {
				format = "Manuscript";
			} else if (category.equals("h")) {
				format = "Microform";
			} else if (category.equals("q")) {
				format = "Musical Score";
			} else if (category.equals("v")) {
				format = "Video";
			} else {
				format = "Unknown";
			}
		}
		
		if (loccodes.contains("serv,remo")) {
			if (!online) {
				online = true;
				// Suppress warning until this can be logged properly and tabulated.
//				System.out.println("ONLINE STATUS: Resource not online according to Cullr logic, but online according to mfhd 852‡b = \"serv,remo\".");
			}
		} else {
			if (online) {
				// Suppress warning until this can be logged properly and tabulated.
//				System.out.println("ONLINE STATUS: Resource online according to Cullr logic, but not online according to mfhd 852‡b != \"serv,remo\".");
			}
		}

		addField(fields,"format",format);
		if (online) {
			addField(fields,"online","Online");
		}
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
