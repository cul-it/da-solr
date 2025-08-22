package edu.cornell.library.integration.authority;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class Solr {

	public static List<String> identifySearchFields(HeadingType fieldType, AuthoritySource vocab, boolean alsoFast) {
		List<String> searchFields = new ArrayList<>();
		switch (fieldType) {
		case PERS:
			searchFields.add("author_pers_roman_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_pers_lcjsh_browse");
			else {
				searchFields.add("subject_pers_lc_browse");
				searchFields.add("subject_pers_unk_browse");
			}
			if ( alsoFast )
				searchFields.add("subject_pers_fast_browse");
			break;
		case CORP:
			searchFields.add("author_corp_roman_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_corp_lcjsh_browse");
			else {
				searchFields.add("subject_corp_lc_browse");
				searchFields.add("subject_corp_unk_browse");
			}
			if ( alsoFast )
				searchFields.add("subject_corp_fast_browse");
			break;
		case MEETING:
		case EVENT:
			searchFields.add("author_event_roman_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_event_lcjsh_browse");
			else {
				searchFields.add("subject_event_lc_browse");
				searchFields.add("subject_event_unk_browse");
			}
			if ( alsoFast )
				searchFields.add("subject_event_fast_browse");
			break;
		case WORK:
			searchFields.add("title_exact");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_work_lcjsh_browse");
			else {
				searchFields.add("subject_work_lc_browse");
				searchFields.add("subject_work_unk_browse");
			}
			break;
		case ERA:
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_era_lcjsh_browse");
			else {
				searchFields.add("subject_era_lc_browse");
				searchFields.add("subject_era_unk_browse");
			}
			break;
		case TOPIC:
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_topic_lcjsh_browse");
			else {
				searchFields.add("subject_topic_lc_browse");
				searchFields.add("subject_topic_unk_browse");
			}
			if ( alsoFast )
				searchFields.add("subject_topic_fast_browse");
			break;
		case PLACE:
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_geo_lcjsh_browse");
			else {
				searchFields.add("subject_geo_lc_browse");
				searchFields.add("subject_geo_unk_browse");
			}
			break;
		case GENRE:
			searchFields.add("subject_genr_lcgft_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_genr_lcjsh_browse");
			else {
				searchFields.add("subject_genr_lc_browse");
				searchFields.add("subject_genr_unk_browse");
			}
			break;
		case INSTRUMENT:
			searchFields = Arrays.asList("notes_t");
			break;
		case SUB_GEN:
		case SUB_GEO:
		case SUB_ERA:
		case SUB_GNR:
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_sub_lcjsh_browse");
			else {
				searchFields.add("subject_sub_lc_browse");
				searchFields.add("subject_sub_unk_browse");
			}
			break;
		default:
			System.out.println("Unexpected field type "+fieldType);
			return null;
		}
		return searchFields;
	}




	public static List<List<String>> querySolrForMatchingBibs(Http2SolrClient solr, String field, String heading)
			throws SolrServerException, IOException {
		SolrQuery q = new SolrQuery(field+":\""+heading.replaceAll("\"","'")+'"');
		q.setRows(99999);
		q.setFields("instance_id","id");
		SolrDocumentList res = solr.query(q).getResults();
		List<List<String>> instances = new ArrayList<>();
		for (SolrDocument doc : res) {
			instances.add(new ArrayList<String>(Arrays.asList(
					(String)doc.getFieldValue("id"),
					(String)doc.getFieldValue("instance_id"))));
		}
		return instances;
	}


	public static int querySolrForMatchingBibCount(Http2SolrClient solr, String field, String heading, boolean aspace) 
			throws SolrServerException, IOException {
		String query = field+":\""+heading.replaceAll("\"","'").replaceAll("\\\\","")+'"';
		SolrQuery q = new SolrQuery(query);
		q.setRows(0);
		q.setFields("instance_id","id");
		if (aspace) q.addFilterQuery("id_t:culaspace");
		SolrDocumentList res = solr.query(q).getResults();
		return (int) res.getNumFound();
	}


	public static Map<String,Integer> tabulateActualUnnormalizedHeadings (
			Http2SolrClient solr, String heading, String field, String facetField, boolean aspace)
					throws SolrServerException, IOException {

		Map<String,Boolean> authors = new HashMap<>();
		Map<String,Integer> displayForms = new HashMap<>();
		String normalizedHeading = getFilingForm( heading );
		System.out.format("tabulating display versions for %s (%s)\n", field, facetField);

		SolrQuery q = new SolrQuery(field+":\""+heading.replaceAll("\"","'").replaceAll("\\\\","")+'"');
		q.setRows(10_000);
		q.setFields(facetField,"id");
		if (aspace) q.addFilterQuery("id_t:culaspace");

		boolean incompleteIndexing = false;
		for (SolrDocument doc : solr.query(q).getResults()) {
			if ( ! doc.containsKey(facetField)) {
				System.out.printf("Needs indexing: %s\n", doc.getFieldValue("id"));
				incompleteIndexing = true;
			} else for (String author : (ArrayList<String>)doc.getFieldValue(facetField)) {
				if ( ! authors.containsKey(author) )
					authors.put(author, normalizedHeading.equals(getFilingForm(author)));
				if ( ! authors.get(author) )
					continue; 
				if ( displayForms.containsKey(author) )
					displayForms.put(author, displayForms.get(author)+1);
				else displayForms.put(author,1);
			}
		}
		if ( incompleteIndexing ) {
			System.out.printf( "query: %s\nfacet %s\n",q.getQuery(),facetField);
		}

		for ( String form : displayForms.keySet() )
			System.out.printf("%s: %d\n", form, displayForms.get(form));
		return displayForms;
	}
}
