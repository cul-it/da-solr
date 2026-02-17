package edu.cornell.library.integration.metadata.generator;

import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.CharacterSetUtils;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class PubInfo implements SolrFieldGenerator {

	private final static String INFORMATION =  "pub_info_display";
	private final static String PRODUCTION =   "pub_prod_display";
	private final static String DISTRIBRUTION = "pub_dist_display";
	private final static String MANUFACTURE =  "pub_manu_display";
	private final static String COPYRIGHT =    "pub_copy_display";

	private static final Pattern pYear = Pattern.compile("^([0-9]{2,4})u*$");
	private static final int current_year = Calendar.getInstance().get(Calendar.YEAR);

	@Override
	public String getVersion() { return "1.2"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("008","260","264"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) {

		SolrFields sfs = new SolrFields();

		List<String> machineDates = ( rec.controlFields.isEmpty() ) ?  new ArrayList<>() : process008(rec, sfs);

		Set<String> humanDates = ( rec.dataFields.isEmpty() ) ? new LinkedHashSet<>() : process26X(rec, sfs);

		Set<String> displayDates = dedupeDisplayDates( humanDates );
		if (! displayDates.isEmpty())
			sfs.fields.add(new SolrField("pub_date_display",String.join(" ", displayDates)));
		Collection<String> allDates = new TreeSet<>();
		allDates.addAll(machineDates);
		allDates.addAll(humanDates);
		for (String date : allDates) {
			sfs.fields.add(new SolrField("pub_date_t",date));
		}
		return sfs;
	}

	private static List<String> process008(MarcRecord rec, SolrFields sfs) {
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
			Matcher m = pYear.matcher(primarySortDate);
			if (m.matches())
				break;
		default:
			primarySortDate = machineDates.get(0);
			Matcher m2 = pYear.matcher(primarySortDate);
			if ( ! m2.matches())
				return machineDates;
		}

		primarySortDate = primarySortDate.replaceAll("u","0");
		int year = Integer.valueOf(primarySortDate);
		if (year <= current_year + 1) {
			sfs.fields.add( new SolrField( "pub_date_sort",primarySortDate));
			sfs.fields.add( new SolrField( "pub_date_facet",primarySortDate));
		}

		return machineDates;
	}

	private static Set<String> process26X(MarcRecord rec, SolrFields sfs) {
		Set<String> humanDates = new LinkedHashSet<>();
		Collection<DataFieldSet> sets = rec.matchAndSortDataFields();
		for( DataFieldSet fs: sets ) {

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
				sfs.fields.add(new SolrField(relation,df.concatenateSpecificSubfields("3abc")));
				if (relation.equals(INFORMATION) || relation.equals(COPYRIGHT))
					for (Subfield sf : df.subfields)
						if (sf.code.equals('c'))
							humanDates.add(removeTrailingPunctuation(sf.value,
									sf.value.contains("[") ?  ". " : "]. "));
				if (df.getScript().equals(DataField.Script.CJK)) {
					for (Subfield sf : df.subfields)
						switch (sf.code) {
						case 'a': sfs.fields.add(new SolrField("pubplace_t_cjk",sf.value)); break;
						case 'b': sfs.fields.add(new SolrField("publisher_t_cjk",sf.value)); break;
						case 'c':
							if ( CharacterSetUtils.isCJK(sf.value))
								sfs.fields.add(new SolrField("pub_date_t_cjk",sf.value));
						}
				}
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
		return humanDates;
	}

	/* If there are multiple dates on a work, and it can be reliably determined that they
	 * all represent the same year, then we'll display just the year instead of the duplicates.
	 * DISCOVERYACCESS-1539
	 */
	private static Set<String> dedupeDisplayDates( Set<String> humanDates ) {
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
			return new LinkedHashSet<>(Arrays.asList(years.iterator().next()));
		return humanDates;
	}

	@Override
	public SolrFields generateNonMarcSolrFields(Map<String, Object> instance, Config config) {
		SolrFields sfs = new SolrFields();

		if (instance.containsKey("publication")) {
			Set<String> pubdates = new HashSet<>();
			for (Map<String,Object> publ : (List<Map<String,Object>>) instance.get("publication")) {
				String publisher = null;
				if (publ.containsKey("publisher")) {
					publisher = (String)publ.get("publisher");
					if (publisher.isBlank()) publisher = null;
					else {
						sfs.add("publisher_display", publisher);
						sfs.add("publisher_t", publisher);
					}
				}
				String pubplace = null;
				if (publ.containsKey("place")) {
					pubplace = (String)publ.get("place");
					if (pubplace.isBlank()) pubplace = null;
					else {
						sfs.add("pubplace_display",pubplace);
						sfs.add("pubplace_t",pubplace);
					}
				}
				String pubdate = null;
				if (publ.containsKey("dateOfPublication")) {
					pubdate = (String)publ.get("dateOfPublication");
					if (pubdate.isBlank()) pubdate = null;
					else pubdates.add(pubdate);
				}
				String pubInfoDisplay = null;
				if (publisher != null)
					if (pubdate != null) {
						if (pubplace != null)
							pubInfoDisplay = String.format("%s : %s, %s.", pubplace, publisher, pubdate);
						else 
							pubInfoDisplay = String.format("%s, %s.", publisher, pubdate);
					} else {
						if (pubplace != null)
							pubInfoDisplay = String.format("%s : %s.", pubplace, publisher);
						else 
							pubInfoDisplay = String.format("%s..", publisher);
					}
				if (pubInfoDisplay != null)
					sfs.add("pub_info_display", pubInfoDisplay);
			}
			if ( ! pubdates.isEmpty() )
				for (String date : dedupeDisplayDates(pubdates)) {
					sfs.add("pub_date_display", date);
					sfs.add("pub_date_t", date);
				}
			for (String date : pubdates) {
				Matcher m = pYear.matcher(date);
				if (m.matches()) {
					date = date.replaceAll("u", "0");
					int year = Integer.valueOf(date);
					if (year <= current_year + 1) {
						sfs.fields.add( new SolrField( "pub_date_sort",date));
						sfs.fields.add( new SolrField( "pub_date_facet",date));
						break;
					}
				}
			}
		}

		return sfs;
	}

}
