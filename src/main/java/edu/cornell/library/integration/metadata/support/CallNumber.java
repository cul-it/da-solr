package edu.cornell.library.integration.metadata.support;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Build Call number search, sort and facet fields.
 */
public class CallNumber {

	// Solr field names
	private final static String sort =   "callnum_sort";
	private final static String search = "lc_callnum_full";
	private final static String facet =  "lc_callnum_facet";
	private final static String biblc =  "lc_bib_display";

	private List<Sort> sortCandidates = new ArrayList<>();
	private Set<Classification> classes = new LinkedHashSet<>();
	private SolrFields sfs = new SolrFields();
	private static ReferenceData callNumberTypes = null;

	public CallNumber () {
		
	}
	public CallNumber (OkapiClient okapi) throws IOException {
		if ( callNumberTypes == null )
			callNumberTypes = new ReferenceData(okapi,"/call-number-types","name");
	}
	public CallNumber (String json) throws IOException{
		if ( callNumberTypes == null )
			callNumberTypes = new ReferenceData(json,"name");
	}

	public void tabulateCallNumber( Map<String,Object> holding ) {

		String callNumberPrefix = null, callNumber = null, callNumberSuffix = null, sortVal = null;
		if ( holding.containsKey("callNumberPrefix") )
			callNumberPrefix = ((String)holding.get("callNumberPrefix")).replaceAll("\\s", " ").trim();
		if ( holding.containsKey("callNumber") )
			callNumber = ((String)holding.get("callNumber")).replaceAll("\\s", " ").trim();
		if ( holding.containsKey("callNumberSuffix") )
			callNumberSuffix = ((String)holding.get("callNumberSuffix")).replaceAll("\\s", " ").trim();
		if ( callNumberPrefix == null &&
				( callNumber == null || callNumber.equalsIgnoreCase("No Call Number")))
			return;
		if ( callNumberSuffix != null && ! callNumberSuffix.isEmpty() ) {
			if ( callNumber != null )
				callNumber = callNumber + " " + callNumberSuffix;
			this.sfs.add(new SolrField(search,callNumberSuffix));
		}

		if ( callNumber == null || callNumber.isEmpty() ) return;

		String cn2 = callNumber;
		sortVal = callNumber;
		this.sfs.add(new SolrField(search,callNumber));
		if ( callNumberPrefix != null && ! callNumberPrefix.isEmpty() )
			this.sfs.add(new SolrField(search,callNumberPrefix+" "+callNumber));
		if (callNumber.toLowerCase().startsWith("thesis ")) {
			cn2 = callNumber.substring(7);
			sortVal = cn2;
			this.sfs.add(new SolrField(search,cn2));
		}

		boolean isLC = true;
		if ( holding.containsKey("callNumberTypeId") ) {
			String callNumberType = callNumberTypes.getName((String)holding.get("callNumberTypeId"));
			if ( callNumberType != null && ! callNumberType.equals("Library of Congress classification"))
				isLC = false;
		}

		int initialLetterCount = 0;
		while ( cn2.length() > initialLetterCount) {
			if ( Character.isLetter(cn2.charAt(initialLetterCount)) )
				initialLetterCount++;
			else
				break;
		}

		if (initialLetterCount > 3) {
			isLC = false;
		}

		if (sortVal != null)
			this.sortCandidates.add( new Sort( sortVal, isLC, true ) );

		if ( ! isLC ) return;

		if (cn2.length() > initialLetterCount) {
			int initialNumberOffset = initialLetterCount;
			for ( ; initialNumberOffset < cn2.length() ; initialNumberOffset++) {
				Character c = cn2.charAt(initialNumberOffset);
				if (! Character.isDigit(c) && ! c.equals('.'))
					break;
			}
			this.classes.add(new Classification(
					cn2.substring(0,initialLetterCount).toUpperCase(),
					cn2.substring(initialLetterCount, initialNumberOffset)));
		} else
			this.classes.add(new Classification(cn2.substring(0,initialLetterCount).toUpperCase(),""));
		return;
	}


	public void tabulateCallNumber( DataField f ) {

		Boolean isHolding = f.mainTag.equals("852");
		Boolean isLC = true;
		String sortVal = null;

		String callNumber = f.concatenateSpecificSubfields(isHolding?"hi":"ab");
		if (callNumber.equalsIgnoreCase("No Call Number")) return;

		if ( ! callNumber.isEmpty()) {
			sortVal = callNumber;
			this.sfs.add(new SolrField(search,callNumber));
		}
		String callNumber2 = callNumber;
		if (callNumber.toLowerCase().startsWith("thesis ")) {
			callNumber2 = callNumber.substring(7);
			if ( ! callNumber2.isEmpty()) {
				sortVal = callNumber2;
				this.sfs.add(new SolrField(search,callNumber2));
			}
		}
		if (isHolding) {
			String callNumberWithPrefix = f.concatenateSpecificSubfields("khi");
			if ( ! callNumberWithPrefix.isEmpty()
					&& ! callNumberWithPrefix.equals(callNumber)  && ! callNumberWithPrefix.equals(callNumber2) )
				this.sfs.add(new SolrField(search,callNumberWithPrefix));
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

		if (initialLetterCount > 3 || initialLetterCount == 0) {
			isLC = false;
		}

		if (sortVal != null)
			this.sortCandidates.add( new Sort( sortVal, isLC, isHolding ) );

		if ( ! isLC ) return;

		if ( ! isHolding )
			this.sfs.add(new SolrField(biblc,sortVal));

		if (callNumber2.length() > initialLetterCount) {
			int initialNumberOffset = initialLetterCount;
			for ( ; initialNumberOffset < callNumber2.length() ; initialNumberOffset++) {
				Character c = callNumber2.charAt(initialNumberOffset);
				if (! Character.isDigit(c) && ! c.equals('.'))
					break;
			}
			this.classes.add(new Classification(
					callNumber2.substring(0,initialLetterCount).toUpperCase(),
					callNumber2.substring(initialLetterCount, initialNumberOffset)));
		} else
			this.classes.add(new Classification(callNumber2.substring(0,initialLetterCount).toUpperCase(),""));
		return;
	}

	public SolrFields getCallNumberFields( Config config ) throws SQLException {

		if ( ! this.sortCandidates.isEmpty() )
			this.sfs.add(new SolrField(sort,chooseSortValue(this.sortCandidates)));

		if ( ! this.classes.isEmpty() )
			this.sfs.addAll(buildHierarchicalFacetValues(config,this.classes));

		return this.sfs;
	}

	private static SolrFields buildHierarchicalFacetValues(Config config, Set<Classification> classes)
			throws SQLException {
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
			this.l = letters;
			this.n = numbers;
		}
		public String letters() { return this.l; }
		public String numbers() { return this.n; }
		private String l;
		private String n;
		@Override
		public String toString() {
			return "Classification "+this.l+":"+this.n;
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
