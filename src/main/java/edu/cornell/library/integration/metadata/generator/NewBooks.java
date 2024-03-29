package edu.cornell.library.integration.metadata.generator;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;
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
	public String getVersion() { return "1.4"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("007","948","holdings","instance"); }

	@SuppressWarnings("unchecked")
	@Override
	public SolrFields generateSolrFields ( MarcRecord bib, Config config ) throws IOException {
		Collection<String> loccodes = new HashSet<>();
		Boolean newBooksZNote = false;

		SolrFields vals = new SolrFields();

		for (MarcRecord hold : bib.marcHoldings)
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

		if ( bib.folioHoldings != null ) {
			if ( folioLocations == null ) {
				folioLocations = SupportReferenceData.locations;
				if (folioLocations == null) {
					OkapiClient folio = config.getOkapi("Folio");
					folioLocations = new ReferenceData( folio,"/locations","code");
				}
			}
			for (Map<String,Object> holding : bib.folioHoldings ) {
				if ( holding.containsKey("permanentLocationId") )
					loccodes.add(folioLocations.getName(holding.get("permanentLocationId").toString()));
				if ( holding.containsKey("temporaryLocationId") )
					loccodes.add(folioLocations.getName(holding.get("temporaryLocationId").toString()));
				if ( holding.containsKey("notes") ) {
					for (Map<String,String> note : (List<Map<String,String>>) holding.get("notes")) {
						String val = note.get("note").toLowerCase();
						if (val.contains("new book") && val.contains("shelf"))
							newBooksZNote = true;
					}
				}
				if ( holding.containsKey("callNumberPrefix") )
					if ( ((String)holding.get("callNumberPrefix")).equalsIgnoreCase("new & noteworthy books") )
						vals.add(new SolrField("new_shelf","Olin Library New & Noteworthy Books"));
			}
		}

		if (newBooksZNote)
			for (String loccode : loccodes)
				if (loccode.startsWith("afr"))
					vals.add(new SolrField("new_shelf","Africana Library New Books Shelf"));


		// Begin New Books Logic

		Timestamp acquisitionDate = null;
		for (DataField f : bib.dataFields) if (f.tag.equals("948") && f.ind1.equals('1'))
			for (Subfield sf : f.subfields) if (sf.code.equals('a')) {
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
		if ( acquisitionDate == null && bib.instance != null ) {
			if ( bib.instance.containsKey("catalogedDate") )
				acquisitionDate = Timestamp.valueOf(((String)bib.instance.get("catalogedDate")+" 00:00:00"));
			else if ( loccodes.contains("serv,remo")
					&& bib.instance.containsKey("statusId")
					&& SupportReferenceData.instanceStatuses.getName(
							(String)bib.instance.get("statusId")).equals("batch") ) {
				if ( bib.instance.containsKey("metadata") ) {
					Map<String,String> meta = Map.class.cast(bib.instance.get("metadata"));
					Timestamp createDate = Timestamp.from(isoDT.parse(meta.get("createdDate"),Instant::from));
					if ( createDate.after(folioGoLive) ) acquisitionDate = createDate;
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
	private static ReferenceData folioLocations = null;
	private static DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME;
	private static Timestamp folioGoLive = Timestamp.valueOf("2021-07-01 00:00:00");

}
