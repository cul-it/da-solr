package edu.cornell.library.integration.indexer.documentPostProcess;

import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/** Evaluate populated fields for conditions of membership for any collections.
 * (Currently only "Law Library".)
 *  */
public class Collections implements DocumentPostProcess{

	final static Boolean debug = false;
	
	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document, Connection conn) throws Exception {

		Collection<String> loccodes = new HashSet<String>();
		Collection<String> lc_alphas = new HashSet<String>();
		Collection<String> collections = new HashSet<String>();

		if (document.containsKey("holdings_record_display")) {
			SolrInputField holdingsField = document.getField( "holdings_record_display" );
			ObjectMapper mapper = new ObjectMapper();
			for (Object hold_obj: holdingsField.getValues()) {

				JsonNode rootNode = mapper.readTree(hold_obj.toString());
				Iterator<JsonNode> locs = rootNode.path("locations").getElements();
				while (locs.hasNext()) {
					JsonNode loc = locs.next();
					loccodes.add(loc.path("code").getTextValue());
				}
			}
		}
		
		if (document.containsKey("lc_alpha_facet")) {
			SolrInputField f = document.getField("lc_alpha_facet");
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
				collections.add("Law Library");
			else if (code.equals("serv,remo")) {
				if (lc_alphas.contains("K"))
					collections.add("Law Library");
			}
		}

		if ( ! collections.isEmpty()) {
			SolrInputField f = new SolrInputField("collection");
			for (String collection : collections)
				f.addValue(collection, 1.0f);
			document.addField("collection", f);
		}
	}
}
