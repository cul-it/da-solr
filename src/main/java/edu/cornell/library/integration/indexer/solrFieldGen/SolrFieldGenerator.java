package edu.cornell.library.integration.indexer.solrFieldGen;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Interface for classes that convert pre-filtered MARC record subsets into sets of Solr fields.
 *
 */
public interface SolrFieldGenerator {

	/**
	 * @return List<String> of expected MARC field tags. Bibliographic field tags should be three digit
	 * numbers; Holdings field tags should be the letter 'h' followed by three digit numbers. For example: 
	 * <pre>return Arrays.asList( "502", "856", "h852" );</pre>
	 */
	public List<String> getHandledFields();

	/**
	 * @return Duration The amount of time generator results can be considered "fresh" while the
	 * related MARC segment goes unchanged.
	 */
	public default Duration resultsShelfLife() { return Duration.ofDays(180); }

	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException, IOException;
}
