package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.nodeToString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Build Call number search, sort and facet fields.
 */
public class CallNumber implements ResultSetToFields {

	final boolean debug = false;

	// Solr field names
	private final static String sort =   "callnum_sort";
	private final static String search = "lc_callnum_full";
	private final static String facet =  "lc_callnum_facet";

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		Map<String,MarcRecord> holdingRecs = new HashMap<>();

		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ) {
				QuerySolution sol = rs.nextSolution();

				if ( resultKey.startsWith("bib") ) {
					JenaResultsToMarcRecord.addDataFieldQuerySolution(bibRec,sol);
				} else {
					String recordURI = nodeToString(sol.get("mfhd"));
					MarcRecord rec;
					if (holdingRecs.containsKey(recordURI)) {
						rec = holdingRecs.get(recordURI);
					} else {
						rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
						rec.id = recordURI.substring(recordURI.lastIndexOf('/')+1);
						holdingRecs.put(recordURI, rec);
					}
					JenaResultsToMarcRecord.addDataFieldQuerySolution(rec,sol);
				}
			}
		}
		bibRec.holdings.addAll(holdingRecs.values());
		SolrFields vals = generateSolrFields( bibRec, config );
		Map<String,SolrInputField> fields = new HashMap<>();
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);
		return fields;
	}

	public static SolrFields generateSolrFields( MarcRecord bibRec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException {

		List<DataField> allFields = bibRec.matchSortAndFlattenDataFields();
		for (MarcRecord holdingsRec : bibRec.holdings)
			allFields.addAll(holdingsRec.matchSortAndFlattenDataFields());

		Set<Classification> classes = new LinkedHashSet<>();
		List<Sort> sortCandidates = new ArrayList<>();
		SolrFields sfs = new SolrFields();

		for (DataField f : allFields) {

			Boolean isHolding = f.mainTag.equals("852");
			Boolean isLC = true;
			String sortVal = null;

			String callNumber = f.concatenateSpecificSubfields(isHolding?"hi":"ab");
			if (callNumber.equalsIgnoreCase("No Call Number")) continue;

			if ( ! callNumber.isEmpty()) {
				sortVal = callNumber;
				sfs.add(new SolrField(search,callNumber));
			}
			String callNumber2 = callNumber;
			if (callNumber.toLowerCase().startsWith("thesis ")) {
				callNumber2 = callNumber.substring(7);
				if ( ! callNumber2.isEmpty()) {
					sortVal = callNumber2;
					sfs.add(new SolrField(search,callNumber2));
				}
			}
			if (isHolding) {
				String callNumberWithPrefix = f.concatenateSpecificSubfields("khi");
				if ( ! callNumberWithPrefix.isEmpty()
						&& ! callNumberWithPrefix.equals(callNumber)  && ! callNumberWithPrefix.equals(callNumber2) )
					sfs.add(new SolrField(search,callNumberWithPrefix));
			}

			// remaining logic relates to facet values, for which we only want LC call numbers
			if ( isHolding && ! f.ind1.equals('0')) {
				isLC = false;
			}

			int initialLetterCount = 0;
			while ( callNumber2.length() > initialLetterCount) {
				if ( Character.isLetter(callNumber2.charAt(initialLetterCount)) )
					initialLetterCount++;
				else
					break;
			}

			if (initialLetterCount > 3) {
				isLC = false;
			}

			if (sortVal != null)
				sortCandidates.add( new Sort( sortVal, isLC, isHolding ) );

			if ( ! isLC ) continue;

			if (callNumber2.length() > initialLetterCount) {
				int initialNumberOffset = initialLetterCount;
				for ( ; initialNumberOffset < callNumber2.length() ; initialNumberOffset++) {
					Character c = callNumber2.charAt(initialNumberOffset);
					if (! Character.isDigit(c) && ! c.equals('.'))
						break;
				}
				classes.add(new Classification(
						callNumber2.substring(0,initialLetterCount).toUpperCase(),
						callNumber2.substring(initialLetterCount, initialNumberOffset)));
			}
		}

		if ( ! sortCandidates.isEmpty() )
			sfs.add(new SolrField(sort,chooseSortValue(sortCandidates)));

		if ( ! classes.isEmpty() )
			sfs.addAll(buildHierarchicalFacetValues(config,classes));

		return sfs;
	}

	private static SolrFields buildHierarchicalFacetValues(SolrBuildConfig config, Set<Classification> classes)
			throws SQLException, ClassNotFoundException {
		Set<String> facetVals = new LinkedHashSet<>();
		try (   Connection conn = config.getDatabaseConnection("CallNos");
				PreparedStatement pstmt = conn.prepareStatement
						("SELECT label FROM classification"
						+ " WHERE ? BETWEEN low_letters AND high_letters"
						+ "   AND ? BETWEEN low_numbers AND high_numbers"
						+ " ORDER BY high_letters DESC, high_numbers DESC")  ) {

			for ( Classification c : classes ) {
				pstmt.setString(1, c.letters());
				pstmt.setString(2, c.numbers());
				pstmt.execute();
				StringBuilder sb = new StringBuilder();
				try (  java.sql.ResultSet rs = pstmt.getResultSet() ) {

					while (rs.next()) {
						if (sb.length() > 0)
							sb.append(":");
						sb.append(rs.getString("label"));
						facetVals.add(sb.toString());
					}
				}
			}
		}
		SolrFields sfs = new SolrFields();
		for (String s : facetVals)
			sfs.add(new SolrField(facet,s));
		return sfs;
	}

	private static String chooseSortValue(List<Sort> sortCandidates) {
		Optional<Sort> bestSort = sortCandidates.stream().sorted().findFirst();
		if (bestSort.isPresent()) return bestSort.get().sortVal;
		return null;
	}

	private static class Classification {
		public Classification (String letters, String numbers) {
			l = letters;
			n = numbers;
		}
		public String letters() { return l; }
		public String numbers() { return n; }
		private String l;
		private String n;
		public String toString() {
			return "Classification "+l+":"+n;
		}
	}

	public static class Sort implements Comparable<Sort>{
		String sortVal;
		Boolean isLC;
		Boolean isHolding;
		public Sort( String sortVal, Boolean isLC, Boolean isHolding) {
			this.sortVal = sortVal;
			this.isLC = isLC;
			this.isHolding = isHolding;
		}
		@Override
		public int compareTo(Sort o) {
			if (o == null) return 1;
			int holdingCompare = Boolean.compare(o.isHolding, this.isHolding);
			if (holdingCompare != 0) return holdingCompare;
			int lcCompare = Boolean.compare(o.isHolding, this.isHolding);
			if (lcCompare != 0 ) return holdingCompare;
			return this.sortVal.compareTo(o.sortVal);
		}
	}
}
