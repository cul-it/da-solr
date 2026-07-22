package edu.cornell.library.integration.utilities;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrDocumentList;

import edu.cornell.library.integration.metadata.generator.Language;

/**
  * In order to document our languages, their IANA mappings, and their record counts this generates
  * a series of &lt;tr/&gt; rows to drop into Confluence. The IANA mapping will almost certainly evolve
  * and we will want to regenerate the documentation to remain a correct reflection. 
  */
public class BuildHtmlLanguageTable {

	public static void main(String[] args) throws SQLException, SolrServerException, IOException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		Config config = Config.loadConfig(requiredArgs);

		try( Http2SolrClient solr = new Http2SolrClient
				.Builder(config.getBlacklightSolrUrl())
				.withBasicAuthCredentials(config.getSolrUser(),config.getSolrPassword()).build();
				Connection connection = config.getDatabaseConnection("Current");
				PreparedStatement stmt = connection.prepareStatement(
						"SELECT code, description, preferred FROM language_iana_codes WHERE code = ?")) {
			for (Language.Code code : Language.Code.values()) {
				SolrQuery q = new SolrQuery("language_facet:\""+
						code.getLanguageName().replaceAll("\"","%22").replaceAll("'", "%27")+'"');
				q.setRows(0);
				q.setFields("instance_id","id");
				SolrDocumentList res = solr.query(q).getResults();
				Long recordCount = res.getNumFound();
				String ianaCode = null;
				String description = null;
				String preferred = null;
				stmt.setString(1, code.getIanaCode()); // gets default of marc code if none configured
				try (ResultSet rs = stmt.executeQuery()) {
					while (rs.next()) {
						ianaCode = rs.getString("code");
						description = rs.getString("description");
						preferred = rs.getString("preferred");
					}
				}
				if (ianaCode == null) ianaCode = "NOT MAPPED";
				if (description == null) description = "";
				if (preferred == null) preferred = "";
				
//				System.out.format("%s\t%s\t<a href='%s%s'>%d</a>\t%s\t%s\n",
				System.out.format("<tr><td>%s</td><td>%s</td><td><a href='%s%s'>%d</a></td><td>%s</td><td>%s</td></tr>\n",
						code.name().toLowerCase(),
						code.getLanguageName(),
						"https://catalog.library.cornell.edu/?f%5Blanguage_facet%5D%5B%5D=",
						code.getLanguageName().replaceAll("\"","%22").replaceAll("'", "%27"),
						recordCount,
						ianaCode + ((code.ianaCode != null)?" *":""),
						description);
			}
		}
	}

}
