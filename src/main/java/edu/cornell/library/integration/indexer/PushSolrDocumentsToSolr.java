package edu.cornell.library.integration.indexer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;
import static edu.cornell.library.integration.utilities.IndexingUtilities.xml2SolrInputDocument;

/**
 *  Pushes an existing set of Solr documents into an empty Solr index.
 * 
 */
public class PushSolrDocumentsToSolr {

	static int batchSize = 5_000;
	static int batchesToCommit = 10;

	public static void main(String[] args) throws Exception {

		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Current");
		requiredArgs.add("sorlUrl");
		SolrBuildConfig config = SolrBuildConfig.loadConfig(args, requiredArgs);
		
		try ( SolrClient solr = new HttpSolrClient( config.getSolrUrl() );
			  Connection current = config.getDatabaseConnection("Current")) {
			int maxBib = 0;
			try (Statement stmt = current.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT MAX(bib_id) FROM "+CurrentDBTable.BIB_SOLR)) {
				while (rs.next())
					maxBib = rs.getInt(1);
			}
			int cursor = 0;
			int batchCount = 0;
			try (PreparedStatement pstmt = current.prepareStatement(
					"SELECT solr_document FROM "+CurrentDBTable.BIB_SOLR+
					" WHERE bib_id between ? AND ? AND active = 1")) {
				Collection<SolrInputDocument> docs = new HashSet<>();
				while (cursor < maxBib) {
					pstmt.setInt(1, cursor + 1);
					pstmt.setInt(2, cursor + batchSize);
					try ( ResultSet rs = pstmt.executeQuery() ) {
						while ( rs.next() )
							docs.add( xml2SolrInputDocument(  rs.getString(1) ) );
					}
					cursor += batchSize;
					System.out.println( cursor );
					solr.add(docs);
					docs.clear();
					if ( ++batchCount % batchesToCommit == 0)
						solr.commit(true, true, true);
				}
				solr.add(docs);
				solr.commit(true, true, true);
			}
		}
	}
}
