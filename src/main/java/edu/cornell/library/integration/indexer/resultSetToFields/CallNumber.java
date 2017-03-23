package edu.cornell.library.integration.indexer.resultSetToFields;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * Build Call number search, sort and facet fields.
 */
public class CallNumber implements ResultSetToFields {

	final boolean debug = false;

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {

		Collection<FieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results);

		Map<String,SolrInputField> fields = new HashMap<>();
		List<Sort> sortCandidates = new ArrayList<>();
		for( FieldSet fs: sets ) {
			SolrFieldValueSet vals = generateSolrFields ( fs, config );
			for ( String s : vals.facet )
				ResultSetUtilities.addField(fields,"lc_callnum_facet",s);
			for ( String s : vals.search )
				ResultSetUtilities.addField(fields,"lc_callnum_full",s);
			if (vals.sort != null )
				sortCandidates.add(vals.sort);
		}
		String finalSortVal = chooseSortValue( sortCandidates );
		if (finalSortVal != null)
			ResultSetUtilities.addField(fields,"callnum_sort",finalSortVal);
		return fields;
	}

	public static String chooseSortValue(List<Sort> sortCandidates) {
		Optional<Sort> bestSort = sortCandidates.stream().sorted().findFirst();
		if (bestSort.isPresent()) return bestSort.get().sortVal;
		return null;
	}

	public SolrFieldValueSet generateSolrFields( FieldSet fs, SolrBuildConfig config ) throws ClassNotFoundException, SQLException {

		Boolean isHolding = fs.mainTag.equals("852");
		Boolean isLC = true;
		SolrFieldValueSet vals = new SolrFieldValueSet();
		ArrayList<Classification> classes = new ArrayList<>();
		String sort = null;
		for (DataField f : fs.fields) {

			String callNumber = f.concatenateSpecificSubfields(isHolding?"hi":"ab");
			if (callNumber.equalsIgnoreCase("No Call Number")) continue;

			if ( ! callNumber.isEmpty()) {
				sort = callNumber;
				vals.search.add(callNumber);
			}
			if (callNumber.toLowerCase().startsWith("thesis ")) {
				callNumber = callNumber.substring(7);
				if ( ! callNumber.isEmpty()) {
					sort = callNumber;
					vals.search.add(callNumber);
				}
			}
			if (isHolding) {
				String callNumberWithPrefix = f.concatenateSpecificSubfields("khi");
				if ( ! callNumberWithPrefix.isEmpty())
					vals.search.add(callNumberWithPrefix);
			}

			// remaining logic relates to facet values, for which we only want LC call numbers
			if ( isHolding && ! f.ind1.equals('0')) {
				isLC = false;
				continue;
			}

			int initialLetterCount = 0;
			while ( callNumber.length() > initialLetterCount) {
				if ( Character.isLetter(callNumber.charAt(initialLetterCount)) )
					initialLetterCount++;
				else
					break;
			}

			if (initialLetterCount > 3) {
				isLC = false;
				continue;
			}

			if (callNumber.length() > initialLetterCount) {
				int initialNumberOffset = initialLetterCount;
				for ( ; initialNumberOffset < callNumber.length() ; initialNumberOffset++) {
					Character c = callNumber.charAt(initialNumberOffset);
					if (! Character.isDigit(c) && ! c.equals('.'))
						break;
				}
				classes.add(new Classification(
						callNumber.substring(0,initialLetterCount).toUpperCase(),
						callNumber.substring(initialLetterCount, initialNumberOffset)));
			}
		}

		if (sort != null)
			vals.sort = new Sort( sort, isLC, isHolding );

		// new bl5-compatible hierarchical facet
		int classCount = classes.size();
		if ( classCount != 0 ) {
			try (   Connection conn = config.getDatabaseConnection("CallNos");
					PreparedStatement pstmt = conn.prepareStatement
							("SELECT label FROM classification"
							+ " WHERE ? BETWEEN low_letters AND high_letters"
							+ "   AND ? BETWEEN low_numbers AND high_numbers"
							+ " ORDER BY high_letters DESC, high_numbers DESC")  ) {

				for ( int i = 0; i < classCount; i++ ) {
					Classification c = classes.get(i);
					pstmt.setString(1, c.letters());
					pstmt.setString(2, c.numbers());
					pstmt.execute();
					StringBuilder sb = new StringBuilder();
					try (  java.sql.ResultSet rs = pstmt.getResultSet() ) {

						while (rs.next()) {
							if (sb.length() > 0)
								sb.append(":");
							sb.append(rs.getString("label"));
							vals.facet.add(sb.toString());
						}
					}
				}
			}
		}

		return vals;
	}

	private class Classification {
		public Classification (String letters, String numbers) {
			l = letters;
			n = numbers;
		}
		public String letters() { return l; }
		public String numbers() { return n; }
		private String l = null;
		private String n = null;
		public String toString() {
			return "Classification "+l+":"+n;
		}
	}

	public class SolrFieldValueSet {
		public Set<String> search = new TreeSet<>();
		public Set<String> facet = new TreeSet<>();
		Sort sort = null;
	}

	public class Sort implements Comparable<Sort>{
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
