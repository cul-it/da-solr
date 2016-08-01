package edu.cornell.library.integration.indexer.documentPostProcess;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/** Evaluate populated fields for conditions of membership for any collections.
 * (Currently only "Law Library".)
 *  */
public class Collections implements DocumentPostProcess{

	final static Boolean debug = false;
	
	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {

		Collection<String> loccodes = new HashSet<String>();
		Collection<String> lc_alphas = new HashSet<String>();
		SolrInputField collections = new SolrInputField("collection");

		if (document.containsKey("holdings_record_display")) {
			SolrInputField holdingsField = document.getField( "holdings_record_display" );
			ObjectMapper mapper = new ObjectMapper();
			for (Object hold_obj: holdingsField.getValues()) {

				JsonFactory factory = mapper.getJsonFactory(); // since 2.1 use mapper.getFactory() instead
				JsonParser jp = factory.createJsonParser(hold_obj.toString());
				JsonNode rootNode = mapper.readTree(jp);
//				JsonNode rootNode = mapper.readTree(hold_obj.toString());
				Iterator<JsonNode> locs = rootNode.path("locations").getElements();
				while (locs.hasNext()) {
					JsonNode loc = locs.next();
					loccodes.add(loc.path("code").getTextValue());
				}
			}
		}
		
		if (document.containsKey("lc_callnum_facet")) {
			SolrInputField f = document.getField("lc_callnum_facet");
			for (Object v : f.getValues()) {
				String val = v.toString();
				lc_alphas.add(val.substring(0, val.indexOf(' ')));
			}
		}
		
		if (loccodes.isEmpty()) {
			if (debug) System.out.println("No location codes found for record.\n");
			return;
		}
		
		for (String code : loccodes) {
			if (code.startsWith("law"))
				collections.addValue("Law Library",1);
			else if (code.equals("serv,remo")) {
				if (lc_alphas.contains("K"))
					collections.addValue("Law Library",1);
			}
		}

		if ( collections.getValueCount() > 0 ) {
			document.put("collection", collections);
		}
	}
}
