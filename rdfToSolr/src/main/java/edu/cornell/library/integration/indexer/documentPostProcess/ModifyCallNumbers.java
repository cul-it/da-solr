package edu.cornell.library.integration.indexer.documentPostProcess;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/**
 * If a work is on the New & Noteworthy Books shelf at Olin, the holdings call number
 * represents its eventual library of congress filing in the stacks, but not its current
 * filing on the shelf. We can display the actual shelf location to patrons, however.
 */
public class ModifyCallNumbers implements DocumentPostProcess {

	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document, Connection voyager) throws Exception {

		final String holdingsField = "holdings_record_display";
		
		if (! document.containsKey(holdingsField))
			return; // without holdings, we have nothing to modify

		SolrInputField holdings = document.getField(holdingsField);
		SolrInputField processedHoldings = new SolrInputField(holdingsField);
		boolean foundSomethingToChange = false;
		ObjectMapper mapper = new ObjectMapper();

		for (Object o : holdings.getValues()) {
			if (o.toString().contains("New & Noteworthy")) {
				// modify
				@SuppressWarnings("unchecked")
				Map<String,Object> holdRec = mapper.readValue(o.toString(),Map.class);
				@SuppressWarnings("unchecked")
				List<Object> callnos = (List<Object>) holdRec.get("callnos");
				for (int i = 0; i < callnos.size(); i++) {
					String call = callnos.get(i).toString().trim();
					if (call.startsWith("New & Noteworthy Books")) {
						boolean fiction = isFiction(document);
						String author = getAuthorPrefix(document);
						String newCall;
						if (call.endsWith("++"))
							newCall = "New & Noteworthy Books Oversize "+author+" ++";
						else
							newCall = "New & Noteworthy Books "+
								((fiction) ? "Fiction " : "Non-Fiction ")+
								author;
						callnos.set(i, newCall);
						foundSomethingToChange = true;
					}
				}
				String json = mapper.writeValueAsString(holdRec);
				processedHoldings.addValue(json, 1f);
			} else {
				processedHoldings.addValue(o, 1f);
			}
		}
		
		if (foundSomethingToChange) 
			document.put(holdingsField, processedHoldings);

	}
	
	private boolean isFiction (SolrInputDocument d) {
		final String fictionField = "subject_content_facet";
		if ( ! d.containsKey(fictionField))
			return false;
		SolrInputField f = d.getField(fictionField);
		boolean isFiction = false;
		for (Object o : f.getValues()) 
			if (o.toString().startsWith("Fiction"))
				isFiction = true;
		return isFiction;
	}
	
	private String getAuthorPrefix (SolrInputDocument d) {
		final String authorField = "author_display";
		if ( ! d.containsKey(authorField)) 
			return null;
		String author = null;
		for (Object o : d.getFieldValues(authorField)) 
			author = o.toString();
		if (author == null)
			return null;
		// If we have a non-Roman and Roman version, skip to the Romanized
		if (author.contains(" / "))
			author = author.substring(author.indexOf(" / ")+3);
		// Shorten to 4 characters
		if (author.length() > 4)
			author = author.substring(0, 4);
		return author.toUpperCase();
	}
}
