package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.utilities.IndexingUtilities.addBibToUpdateQueue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;

import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

public class BatchRecordsForSolrIndex {

    /**
	 * Gets a list of BIB IDs at the top of the queue for indexing, and marks those queue items
	 * as batched. The next request will skip these records.
	 * @param current Open DB Connection to the current records Solr inventory DB.
     * @param solrUrl Base URL for Solr queries
     * @param minCount Minimum number of records to include in batch
     * @param maxCount Maximum number of records to include in batch
	 * @throws Exception 
	 * 
	 */
	public static Set<Integer> getBibsToIndex( Connection current, String solrUrl, int minCount, int maxCount ) throws Exception {

        Set<Integer> addedBibs = new HashSet<>(minCount);

        try (Statement stmt = current.createStatement()) {
        	stmt.executeQuery("LOCK TABLES "+CurrentDBTable.QUEUE+" WRITE"); }
        try (PreparedStatement pstmt = current.prepareStatement(
        		"SELECT * FROM "+CurrentDBTable.QUEUE
        		+" WHERE done_date = 0 AND batched_date = 0"
        		+" ORDER BY priority"
        		+" LIMIT " + Math.round(maxCount*1.125));
        		ResultSet rs = pstmt.executeQuery()) {
        	final String delete = DataChangeUpdateType.DELETE.toString();

        	while (rs.next() && addedBibs.size() < maxCount) {
        		if (rs.getString("cause").equals(delete))
        			continue;
        		int bib_id = rs.getInt("bib_id");
        		addedBibs.add(bib_id);
        	}
        }

        if (addedBibs.size() < minCount) {
            try (HttpSolrClient solr = new HttpSolrClient(solrUrl)) {
            	SolrQuery query = new SolrQuery();
            	query.setRequestHandler("standard");
            	query.setQuery("*:*");
            	query.setSort("timestamp", ORDER.asc);
            	query.setFields("id");
            	if (primeNumbers == null)
            		primeNumbers = generatePrimeNumberList(minCount);
            	query.setRows(primeNumbers.get(minCount-addedBibs.size()-1));
            	int i = 0;
            	for (SolrDocument doc : solr.query(query).getResults()) {
            		if ( ! primeNumbers.contains(++i) ) continue;
            		int bib_id = Integer.valueOf(
            				doc.getFieldValues("id").iterator().next().toString());
            		if ( ! addedBibs.contains(bib_id) ) {
            			addedBibs.add(bib_id);
            			addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.AGE_IN_SOLR);
            		}      		
            	}
            }
        }
        try (PreparedStatement batchStmt = current.prepareStatement(
        		"UPDATE "+CurrentDBTable.QUEUE+" SET batched_date = NOW() WHERE bib_id = ? AND batched_date = 0")) {
        	for (int bib_id : addedBibs) {
        		batchStmt.setInt(1,bib_id);
        		batchStmt.addBatch();
        	}
        	batchStmt.executeBatch();
        }
        try (Statement stmt = current.createStatement()) {
        	stmt.executeQuery("UNLOCK TABLES"); }
        return addedBibs;
    }
	private static ArrayList<Integer> primeNumbers = null;
	/**
	 * Generate an ArrayList&lt;Integer&gt; of the first &lt;number&gt; primes,
	 * including 1. While modern math theory firmly excludes 1 from the set of prime
	 * numbers, this was not originally the case[1][2]. For this particular use case,
	 * it made sense to include it. It may need to be excluded if the code is repurposed.
	 * This is also not an efficient algorithm for large values of &lt;number&gt;, and
	 * a sieve might be preferable at scale.<br/><br/>
	 * 
	 * [1] http://mathworld.wolfram.com/PrimeNumber.html <br/>
	 * [2] http://mathforum.org/library/drmath/view/64874.html
	 */
	private static ArrayList<Integer> generatePrimeNumberList(int number) {
		ArrayList<Integer> primeNumbers = new ArrayList<>(number);
		int i = 0;
		MAIN: while (primeNumbers.size() < number) {
			int halfOfI = ++i/2;
			for (int j=2;j<=halfOfI;j++)
				if (i%j==0)
					continue MAIN;
			primeNumbers.add(i);
		}
		return primeNumbers;
	}

}
