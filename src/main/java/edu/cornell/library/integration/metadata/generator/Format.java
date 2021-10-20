package edu.cornell.library.integration.metadata.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.folio.Locations;
import edu.cornell.library.integration.folio.Locations.Location;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.StatisticalCodes;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.BooleanSolrField;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * process various record values into complicated determination of item format.  
 */
public class Format implements SolrFieldGenerator {

	private static Collection<String> rareLocCodes = Arrays.asList(
			"asia,ranx","asia,rare",
			"ech,rare","ech,ranx",
			"ent,rare","ent,rar2",
			"gnva,rare",
			"hote,rare",
			"ilr,kanx","ilr,lmdc","ilr,lmdr","ilr,rare",
			"lawr","lawr,anx",
			"mann,spec",
			"rmc","rmc,anx","rmc,hsci","rmc,icer","rmc,ref",
			"sasa,ranx","sasa,rare",
			"vet,rare",
			"was,rare","was,ranx");

	@Override
	public String getVersion() { return "2"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList("leader","007","008","245","502","653","948","holdings","instance");
	}

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) throws IOException {

		String record_type =          rec.leader.substring(6,7);
		String bibliographic_level =  rec.leader.substring(7,8);

		String category = "";
		String typeOfContinuingResource = "";
		for (ControlField f : rec.controlFields)
			switch (f.tag) {
			case "007": if (f.value.length() > 0) category = f.value.substring(0,1); break;
			case "008": if (f.value.length() > 21) typeOfContinuingResource = f.value.substring(21,22);
			}

		List<String> sf653as = new ArrayList<>();
		List<String> sf245hs = new ArrayList<>();
		List<String> statCodes = new ArrayList<>();
		Boolean isThesis = false;
		for (DataField f : rec.dataFields)
			switch (f.tag) {
			case "245":	for (Subfield sf : f.subfields)
				if (sf.code.equals('h')) sf245hs.add(sf.value);	break;
			case "502":	isThesis = true; break;
			case "653":	for (Subfield sf : f.subfields)
				if (sf.code.equals('a')) sf653as.add(sf.value); break;
			case "948":	for (Subfield sf : f.subfields) if (sf.code.equals('f')) statCodes.add(sf.value);
			}

		Collection<String> loccodes = new HashSet<>();
		for (MarcRecord hRec : rec.marcHoldings)
			for (DataField f : hRec.dataFields)
				for (Subfield sf : f.subfields)
					if (sf.code.equals('b')) loccodes.add(sf.value);
		if ( rec.folioHoldings != null ) {
			if ( folioLocations == null ) 
				try { folioLocations = new Locations(null); } catch (IOException e) {
					System.out.println(e.getMessage());
					System.out.println("Folio Locations must be instantiated once before this point.");
					e.printStackTrace();
					System.exit(1);
				}
			for (Map<String,Object> holding : rec.folioHoldings)
				if ( holding.containsKey("permanentLocationId") ) {
					Location l = folioLocations.getByUuid((String)holding.get("permanentLocationId"));
					if ( l != null ) loccodes.add(l.code);
				}
		}

		if ( rec.instance != null && rec.instance.containsKey("statisticalCodeIds") )
			statCodes.addAll(StatisticalCodes.dereferenceStatCodes(
					(List<String>)rec.instance.get("statisticalCodeIds")));

		Boolean isDatabase = false;
		Boolean isMicroform = false;
		String format = null;
		
		if (category.equals("h"))
			isMicroform = true;

		Iterator<String> i = sf245hs.iterator();
		while (i.hasNext())
			if (i.next().toLowerCase().contains("[electronic resource]")) {
				Iterator<String> j = statCodes.iterator();
				while (j.hasNext())
					if (j.next().toLowerCase().equals("j"))
						format = "Journal/Periodical";
			}


		i = statCodes.iterator();
		while (i.hasNext()) {
			String val = i.next();
			if ((val.equals("fd")) || (val.equals("webfeatdb"))) {
				format = "Database";
				
				// format:Database differs from database_b flag.
				// The latter is used for Database ERMS.
				if (val.equals("webfeatdb"))
					isDatabase = true;
			}
		}

		i = sf653as.iterator();
		while (i.hasNext()) {
			String val = i.next();
			if (val.equalsIgnoreCase("research guide"))
				format = "Research Guide";
			else if (val.equalsIgnoreCase("course guide"))
				format = "Course Guide";
			else if (val.equalsIgnoreCase("library guide"))
				format = "Library Guide";
		}

		if (format == null) {
			if (record_type.equals("a")) {
				if ((bibliographic_level.equals("a"))
						|| (bibliographic_level.equals("m"))
						|| (bibliographic_level.equals("d"))
						|| (bibliographic_level.equals("c")) )
					format = "Book";
				else if ((bibliographic_level.equals("b"))
						|| (bibliographic_level.equals("s")))
					format = "Journal/Periodical";
				else if (bibliographic_level.equals("i")) {
					if (typeOfContinuingResource.equals("w"))
						format = "Website";
					else if (typeOfContinuingResource.equals("m"))
						format = "Book";
					else if (typeOfContinuingResource.equals("d"))
						format = "Database";
					else if (typeOfContinuingResource.equals("n") || typeOfContinuingResource.equals("p"))
						format = "Journal/Periodical";

				}
			} else if (record_type.equals("t")) {
				if (bibliographic_level.equals("a"))
					format = "Book";
			} else if ((record_type.equals("c")) || (record_type.equals("d"))) {
				format = "Musical Score";
			} else if ((record_type.equals("e")) || (record_type.equals("f"))) {
				format = "Map";
			} else if (record_type.equals("g")) {
				format = "Video";
			} else if (record_type.equals("i")) {
				format = "Non-musical Recording";
			} else if (record_type.equals("j")) {
				format = "Musical Recording";
			} else if (record_type.equals("k")) {
				format = "Image";
			} else if (record_type.equals("m")) {
				if (statCodes.contains("evideo"))
					format = "Video";
				else if (statCodes.contains("eaudio"))
					format = "Musical Recording";
				else if (statCodes.contains("escore"))
					format = "Musical Score";
				else if (statCodes.contains("emap"))
					format = "Map";
				else
					format = "Computer File";
			} else if (record_type.equals("o")) {
				format = "Kit";
			} else if (record_type.equals("p")) { // p means "mixed materials", classifying as 
				Iterator<String> iter = loccodes.iterator();   // archival if in rare location
				while (iter.hasNext()) {
					String loccode = iter.next();
					if (rareLocCodes.contains(loccode)) {
						format = "Manuscript/Archive"; //ARCHIVAL
						break;
					}
				}
			} else if (record_type.equals("r")) {
				format = "Object";
			}
		}
		if (format == null) {
			if (record_type.equals("t"))
				 // This includes all bibliographic_levels but 'a' captured above. MANUSCRIPT
				format = "Manuscript/Archive";
			else if (category.equals("q"))
				format = "Musical Score";
			else if (category.equals("v"))
				format = "Video";
			else
				format = "Miscellaneous";

		}

		SolrFields sfs = new SolrFields();
		if (isThesis) {  //Thesis is an "additional" format, and won't override main format entry.
			sfs.add(new SolrField("format","Thesis"));
			if (format.equals("Manuscript/Archive")) // unless the main format is Manuscript/Archive
				format = null;
		}
		if (isMicroform) {  //Microform is an "additional" format, and won't override main format entry.
			sfs.add(new SolrField("format","Microform"));
		}

		if (format != null) {
			sfs.add(new SolrField("format",format));
			sfs.add(new SolrField("format_main_facet",format));
		} else if (isThesis)
			sfs.add(new SolrField("format_main_facet","Thesis"));
		else if (isMicroform)
			sfs.add(new SolrField("format_main_facet","Microform"));

		sfs.add(new SolrField("bib_format_display", record_type + bibliographic_level));
		sfs.add(new BooleanSolrField("database_b",isDatabase));
		return sfs;
	}

	private static Locations folioLocations = null;

}
