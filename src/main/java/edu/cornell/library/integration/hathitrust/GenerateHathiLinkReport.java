package edu.cornell.library.integration.hathitrust;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.utilities.Config;

public class GenerateHathiLinkReport {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, SolrServerException {

		List<String> requiredFields = Arrays.asList("blacklightSolrUrl");
		Config config = Config.loadConfig(requiredFields);
		ObjectMapper mapper = new ObjectMapper();

		try ( HttpSolrClient solr = new HttpSolrClient(config.getBlacklightSolrUrl()) ) {

			SolrQuery q = new SolrQuery();
			q.setFields("id","url_access_json","oclc_id_display",
					"author_display","title_display","pub_date_display","type");
			q.setRows(10_000);
			q.setRequestHandler("standard");
			q.setQuery("id:*");
			q.setSort("id", ORDER.asc);
			q.setFilterQueries("online:Online");

			String lastId = null;
			do {
				if ( lastId != null ) {
					q.setQuery("id:{"+lastId+" TO *]");
					lastId = null;
				}
				for (SolrDocument doc : solr.query(q).getResults()) {
					lastId = (String)doc.getFieldValue("id");
					List<String> accessUrlJsons = (ArrayList<String>)doc.getFieldValue("url_access_json");
					List<HathiLink> hathiLinks = new ArrayList<>();
					for (String accessUrlJson : accessUrlJsons) {
						Map<String,String> jsonFields = mapper.readValue(accessUrlJson, HashMap.class);
						if ( ! jsonFields.get("url").contains("//catalog.hathitrust.org/")
								&& ! jsonFields.get("url").contains("//hdl.handle.net/2027/") )
							continue;
						if ( jsonFields.get("description").endsWith("Access limited to authorized subscribers.") )
							hathiLinks.add(new HathiLink("ETAS",jsonFields.get("url")));
						else
							hathiLinks.add(new HathiLink("PD",jsonFields.get("url")));
					}
					if ( hathiLinks.isEmpty() ) continue;
					String recordType = (String)doc.getFieldValue("type");
					String title;
					if (doc.containsKey("title_display"))
						title = (String)doc.getFieldValue("title_display");
					else title = "";
					String author;
					if (doc.containsKey("author_display"))
						author = (String)doc.getFieldValue("author_display");
					else author = "";
					String date;
					if (doc.containsKey("pub_date_display"))
						date = String.join(", ",(ArrayList<String>)doc.getFieldValue("pub_date_display"));
					else date = "";
					String oclcs;
					if (doc.containsKey("oclc_id_display"))
						oclcs = String.join(", ",(ArrayList<String>)doc.getFieldValue("oclc_id_display"));
					else oclcs = "";
					if ( recordType.equals("Catalog") ) recordType = "";
					for (HathiLink hl : hathiLinks )
						System.out.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
								lastId,oclcs,recordType,hl.accessType,date,author,title,hl.url);
				}
			} while ( lastId != null );

		}

	}

	private static class HathiLink {
		final String accessType;
		final String url;
		public HathiLink(String accessType, String url) {
			this.accessType = accessType;
			this.url = url;
		}
	}
}
