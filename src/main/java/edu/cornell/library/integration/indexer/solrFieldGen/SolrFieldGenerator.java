package edu.cornell.library.integration.indexer.solrFieldGen;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Interface for classes that convert pre-filtered MARC record subsets into sets of Solr fields.
 *
 */
public interface SolrFieldGenerator {

	public List<String> getHandledFields();

	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException, IOException;
}
