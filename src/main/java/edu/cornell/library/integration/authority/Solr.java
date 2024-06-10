package edu.cornell.library.integration.authority;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
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




	public static List<List<String>> querySolrForMatchingBibs(HttpSolrClient solr, String field, String heading)
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


	public static int querySolrForMatchingBibCount(HttpSolrClient solr, String field, String heading) 
			throws SolrServerException, IOException {
		SolrQuery q = new SolrQuery(field+":\""+heading.replaceAll("\"","'")+'"');
		q.setRows(0);
		q.setFields("instance_id","id");
		SolrDocumentList res = solr.query(q).getResults();
		return (int) res.getNumFound();
	}
}
