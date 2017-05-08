package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.ControlField;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrFields;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class PubInfo implements ResultSetToFields {

	private final static String INFORMATION =  "pub_info_display";
	private final static String PRODUCTION =   "pub_prod_display";
	private final static String DISTRIBRUTION = "pub_dist_display";
	private final static String MANUFACTURE =  "pub_manu_display";
	private final static String COPYRIGHT =    "pub_copy_display";

	static final Pattern p = Pattern.compile("^[0-9]{4}$");
	static final int current_year = Calendar.getInstance().get(Calendar.YEAR);

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord();
		rec.addControlFieldResultSet( results.get("eight") );
		rec.addDataFieldResultSet( results.get("pub_info_260") );
		rec.addDataFieldResultSet( results.get("pub_info_264") );

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec );
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		return fields;
	}

	public static SolrFields generateSolrFields( MarcRecord rec ) {

		SolrFields sfs = new SolrFields();

		ControlField cf = rec.controlFields.first();
		String eight = cf.value;
		List<String> machineDates = new ArrayList<>();
		machineDates.add(eight.substring(7, 11));
		machineDates.add(eight.substring(11,15));
		// using second 008 date value for sort in some cases (DISCOVERYACCESS-1438)
		String primarySortDate;
		switch(eight.charAt(6)) {
		case 'p':
		case 'r':
			primarySortDate = machineDates.get(1);
			break;
		default:
			primarySortDate = machineDates.get(0);
		}
		Matcher m = p.matcher(primarySortDate);
		if (m.matches()) {
			int year = Integer.valueOf(primarySortDate);
			if (year <= current_year + 1) {
				sfs.fields.add( new SolrField( "pub_date_sort",primarySortDate));
				sfs.fields.add( new SolrField( "pub_date_facet",primarySortDate));
			}
		}

		List<String> humanDates = new ArrayList<>();
		Collection<FieldSet> sets = rec.matchAndSortDataFields();
		for( FieldSet fs: sets ) {

			String relation;
			if (fs.getMainTag().equals("260"))
				relation = INFORMATION;
			else
				switch (fs.getFields().get(0).ind2) {
				case '0': relation = PRODUCTION; break;
				case '1': relation = INFORMATION; break;
				case '2': relation = DISTRIBRUTION; break;
				case '3': relation = MANUFACTURE; break;
				case '4': relation = COPYRIGHT; break;
				default: relation = INFORMATION;
				}
			for (DataField df : fs.getFields()) {
				sfs.fields.add(new SolrField(relation,df.concatenateSpecificSubfields("abc")));
				if (relation.equals(INFORMATION) || relation.equals(COPYRIGHT))
					for (Subfield sf : df.subfields)
						if (sf.code.equals('c'))
							humanDates.add(removeTrailingPunctuation(sf.value,
									sf.value.contains("[") ?  ". " : "]. "));
			}

			String pubplace = fs.getFields().stream()
					.map(p -> removeTrailingPunctuation(p.concatenateSpecificSubfields("a"),": "))
					.filter(s -> ! s.isEmpty())
					.collect(Collectors.joining(" / "));
			if (pubplace != null && ! pubplace.isEmpty()) {
				sfs.fields.add(new SolrField("pubplace_display",pubplace));
				sfs.fields.add(new SolrField("pubplace_t",pubplace));
			}

			String publisher = fs.getFields().stream()
					.map(p -> removeTrailingPunctuation(p.concatenateSpecificSubfields("b"),", "))
					.filter(s -> ! s.isEmpty())
					.collect(Collectors.joining(" / "));
			if (publisher != null && ! publisher.isEmpty()) {
				sfs.fields.add(new SolrField("publisher_display",publisher));
				sfs.fields.add(new SolrField("publisher_t",publisher));
			}

		}

		List<String> displayDates = dedupeDisplayDates( humanDates );
		if (! displayDates.isEmpty())
			sfs.fields.add(new SolrField("pub_date_display",String.join(" ", displayDates)));
		Collection<String> allDates = new TreeSet<>();
		allDates.addAll(machineDates);
		allDates.addAll(humanDates);
		for (String date : allDates)
			sfs.fields.add(new SolrField("pub_date_t",date));

		return sfs;
	}

	/* If there are multiple dates on a work, and it can be reliably determined that they
	 * all represent the same year, then we'll display just the year instead of the duplicates.
	 * DISCOVERYACCESS-1539
	 */
	private static List<String> dedupeDisplayDates( List<String> humanDates ) {
		if (humanDates.size() < 2) return humanDates;
		Collection<String> years = new HashSet<>(); //hashset drops duplicates
		for (String date : humanDates) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0 ; i < date.length() ; i++) {
				char c = date.charAt(i);
				if (Character.isDigit(c))
					sb.append(c);
			}
			String year = sb.toString();
			if (year.length() == 4)
				years.add(year);
			else
				return humanDates;
		}
		if (years.size() == 1)
			return Arrays.asList(years.iterator().next());
		return humanDates;
	}
}
