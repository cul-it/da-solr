package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * Currently, the only record types are "Catalog" and "Shadow", where shadow records are 
 * detected through a 948‡h note. Blacklight searches will be filtered to type:Catalog,
 * so only records that should NOT be returned in Blacklight work-level searches should
 * vary from this.
 */
public class RecordTypeRSTF implements ResultSetToFields {

	protected boolean debug = false;
	
	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {

		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.

		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		Collection<String> sf948hs = new HashSet<String>();
		Collection<String> sf852xs = new HashSet<String>();
		Boolean isShadow = false;
		

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
						if (name.equals("sf948h")) {
							sf948hs.add(nodeToString( node ));
							if (debug) System.out.println("sf948h = "+nodeToString( node ));
						} else if (name.equals("sf852x")) {
							sf852xs.add(nodeToString( node ));
							if (debug) System.out.println("sf852x = "+nodeToString( node ));
						}

					}
				}
			}
		}

		Iterator<String> i = sf948hs.iterator();
		while (i.hasNext())
			if (i.next().toLowerCase().equals("public services shadow record")) {
				isShadow = true;
				if (debug) System.out.println("type:Shadow due to 948‡h = \"PUBLIC SERVICES SHADOW RECORD\".");
			}
		i = sf852xs.iterator();
		while (i.hasNext())
			if (i.next().toLowerCase().equals("public services shadow record")) {
				isShadow = true;
				if (debug) System.out.println("type:Shadow due to mfhd 852‡x = \"PUBLIC SERVICES SHADOW RECORD\".");
			}
		
		if (isShadow) {
			addField(fields,"type","Shadow");
		} else {
			addField(fields,"type","Catalog");
		}

		return fields;

	}
}
