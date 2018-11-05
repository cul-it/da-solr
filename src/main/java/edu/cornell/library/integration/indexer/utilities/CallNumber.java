package edu.cornell.library.integration.indexer.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;

/**
 * Build Call number search, sort and facet fields.
 */
public class CallNumber {

	// Solr field names
	private final static String sort =   "callnum_sort";
	private final static String search = "lc_callnum_full";
	private final static String facet =  "lc_callnum_facet";

	private List<Sort> sortCandidates = new ArrayList<>();
	private Set<Classification> classes = new LinkedHashSet<>();
	private SolrFields sfs = new SolrFields();

	public CallNumber () {
		
	}

	public void tabulateCallNumber( DataField f ) {

		Boolean isHolding = f.mainTag.equals("852");
		Boolean isLC = true;
		String sortVal = null;

		String callNumber = f.concatenateSpecificSubfields(isHolding?"hi":"ab");
		if (callNumber.equalsIgnoreCase("No Call Number")) return;

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

		if ( ! isLC ) return;

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
		return;
	}

	public SolrFields getCallNumberFields( Config config ) throws ClassNotFoundException, SQLException {

		if ( ! sortCandidates.isEmpty() )
			sfs.add(new SolrField(sort,chooseSortValue(sortCandidates)));

		if ( ! classes.isEmpty() )
			sfs.addAll(buildHierarchicalFacetValues(config,classes));

		return sfs;
	}

	private static SolrFields buildHierarchicalFacetValues(Config config, Set<Classification> classes)
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

	private static class Sort implements Comparable<Sort>{
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
