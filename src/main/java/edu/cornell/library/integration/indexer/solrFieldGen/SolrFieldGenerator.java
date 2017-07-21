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
	 * numbers; Holdings records aren't segregated by field and can be included by requesting "holdings". 
	 * <pre>return Arrays.asList( "leader", "502", "856", "holdings" );</pre>
	 */
	public List<String> getHandledFields();

	/**
	 * @return Duration The amount of time generator results can be considered "fresh" while the
	 * related MARC segment goes unchanged.
	 */
	public default Duration resultsShelfLife() { return Duration.ofDays(180); }

	/**
	 * Process a subset of a bibliographic MARC (possibly with holdings), into a set of Solr fields.
	 * @param rec A subset of a bibliographic MARC (possibly with holdings), contents determined by getHandledFields().
	 * @param config SolrBuildConfig gives implementations access to databases as needed. 
	 * @return Generated set of Solr fields. If no fields were produced, value should be empty rather than null.
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 */
	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException, IOException;
}
