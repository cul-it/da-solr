package edu.cornell.library.integration.metadata.generator;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.BooleanSolrField;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Look for various factors to identify books on new books shelves and acquisition
 * dates for recently acquired materials. Acquisition dates are less reliable the
 * more time has passed.
 * 
 */
public class NewBooks implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.3"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("007","948","holdings"); }

	@Override
	public SolrFields generateSolrFields ( MarcRecord bib, Config config ) {
		Collection<String> loccodes = new HashSet<>();
		Boolean newBooksZNote = false;

		SolrFields vals = new SolrFields();

		for (MarcRecord hold : bib.holdings)
			for (DataField f : hold.dataFields) if (f.tag.equals("852"))
				for (Subfield sf : f.subfields)
					if (sf.code.equals('k')) {
						if (sf.value.equalsIgnoreCase("new & noteworthy books"))
							vals.add(new SolrField("new_shelf","Olin Library New & Noteworthy Books"));
					} else if (sf.code.equals('b')) {
						loccodes.add(sf.value);
					} else if (sf.code.equals('z')) {
						String val = sf.value.toLowerCase();
						if (val.contains("new book") && val.contains("shelf"))
							newBooksZNote = true;
					}

		if (newBooksZNote)
			for (String loccode : loccodes)
				if (loccode.startsWith("afr"))
					vals.add(new SolrField("new_shelf","Africana Library New Books Shelf"));


		// Begin New Books Logic

		Timestamp acquisitionDate = null;
		for (DataField f : bib.dataFields)   if (f.tag.equals("948") && f.ind1.equals('1'))
			for (Subfield sf : f.subfields)  if (sf.code.equals('a')) {
				if (! yyyymmdd.matcher(sf.value).matches() ) {
					System.out.printf("B%s has invalid acquisition date: %s\n", bib.id, sf.value);
					vals.add(new BooleanSolrField("acquired_date_invalid_b",true));
				} else {
					try {
						Timestamp t = Timestamp.valueOf(sf.value.replaceAll("(\\d{4})(\\d{2})(\\d{2})", "$1-$2-$3 00:00:00"));
						if ( acquisitionDate == null || t.after(acquisitionDate) )
							acquisitionDate = t;
					} catch (@SuppressWarnings("unused") IllegalArgumentException e) {
						System.out.printf("B%s has invalid acquisition date: %s\n", bib.id, sf.value);
						vals.add(new BooleanSolrField("acquired_date_invalid_b",true));
					}
				}
			}

		if ( acquisitionDate == null ) return vals;

		// Stop processing if record is microform
		for (ControlField f : bib.controlFields)
			if (f.tag.equals("007") && ! f.value.isEmpty() && f.value.substring(0,1).equals("h"))
				return vals;

		String formattedDate = String.format("%1$tFT%1$tTZ",acquisitionDate);
		vals.add(new SolrField("acquired_dt",formattedDate));
		vals.add(new SolrField("acquired_month",formattedDate.substring(0,7)));
		return vals;
	}
	private static Pattern yyyymmdd = Pattern.compile("[0-9]{8}");

}
