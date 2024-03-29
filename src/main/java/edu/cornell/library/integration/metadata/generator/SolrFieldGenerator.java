package edu.cornell.library.integration.metadata.generator;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;

/**
 * Interface for classes that convert pre-filtered MARC record subsets into sets of Solr fields.
 *
 */
public interface SolrFieldGenerator {

	/**
	 * The version of a particular SolrFieldGenerator implementation is unparsed by the system, but any
	 * changes to the version string will result in the results generated by that implementation being
	 * considered stale regardless of that implentation's resultsShelfLife() value. 
	 * @return version:	Should be 24 characters at most.
	 */
	public String getVersion();

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
	 * If a SolrFieldGenerator provides data in output that will be used to populate the headings browse
	 * features, a change to its data will trigger an evaluation of the bib's links to headings.
	 */
	public default boolean providesHeadingBrowseData() { return false; }

	/**
	 * Process a subset of a bibliographic MARC (possibly with holdings), into a set of Solr fields.
	 * @param rec A subset of a bibliographic MARC (possibly with holdings), contents determined by getHandledFields().
	 * @param config Config gives implementations access to databases as needed. 
	 * @return Generated set of Solr fields. If no fields were produced, value should be empty rather than null.
	 * @throws SQLException
	 * @throws IOException
	 */
	public SolrFields generateSolrFields( MarcRecord rec, Config config )
			throws SQLException, IOException;

	/**
	 * Process an instance to generate Solr fields similar to those generated through generateSolrFields.
	 * @param instance
	 * @return SolrFields or null for unimplemented or inapplicable generators
	 */
	public default SolrFields generateNonMarcSolrFields( Map<String,Object> instance, Config config )
			throws IOException {
		return null;
	}
}
