package edu.cornell.library.integration.metadata.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.folio.FolioClient;
import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.StatisticalCodes;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;

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
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) throws IOException {

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
				folioLocations = SupportReferenceData.locations;
				if (folioLocations == null) {
					FolioClient folio = config.getFolio("Folio");
					folioLocations = new ReferenceData( folio,"/locations","code");
				}
			for (Map<String,Object> holding : rec.folioHoldings)
				if ( holding.containsKey("permanentLocationId") ) {
					String locationCode = folioLocations.getName(holding.get("permanentLocationId").toString());
					if ( locationCode != null ) loccodes.add(locationCode);
				}
		}

		if ( rec.instance != null && rec.instance.containsKey("statisticalCodeIds") )
			statCodes.addAll(StatisticalCodes.dereferenceStatCodes(
					(List<String>)rec.instance.get("statisticalCodeIds")));

		Boolean isDatabase = false;
		Boolean isMicroform = false;
		BLFormat format = null;
		
		if (category.equals("h"))
			isMicroform = true;

		Iterator<String> i = sf245hs.iterator();
		while (i.hasNext())
			if (i.next().toLowerCase().contains("[electronic resource]")) {
				Iterator<String> j = statCodes.iterator();
				while (j.hasNext())
					if (j.next().toLowerCase().equals("j"))
						format = BLFormat.SERIAL;
			}


		i = statCodes.iterator();
		while (i.hasNext()) {
			String val = i.next();
			if ((val.equals("fd")) || (val.equals("webfeatdb"))) {
				format = BLFormat.DB;
				
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
				format = BLFormat.RESEARCH;
			else if (val.equalsIgnoreCase("course guide"))
				format = BLFormat.GUIDE;
			else if (val.equalsIgnoreCase("library guide"))
				format = BLFormat.LIBGUIDE;
		}

		if (format == null) {
			if (record_type.equals("a")) {
				if ((bibliographic_level.equals("a"))
						|| (bibliographic_level.equals("m"))
						|| (bibliographic_level.equals("d"))
						|| (bibliographic_level.equals("c")) )
					format = BLFormat.MONO;
				else if ((bibliographic_level.equals("b"))
						|| (bibliographic_level.equals("s")))
					format = BLFormat.SERIAL;
				else if (bibliographic_level.equals("i")) {
					if (typeOfContinuingResource.equals("w"))
						format = BLFormat.SITE;
					else if (typeOfContinuingResource.equals("m"))
						format = BLFormat.MONO;
					else if (typeOfContinuingResource.equals("d"))
						format = BLFormat.DB;
					else if (typeOfContinuingResource.equals("n") || typeOfContinuingResource.equals("p"))
						format = BLFormat.SERIAL;

				}
			} else if (record_type.equals("t")) {
				if (bibliographic_level.equals("a"))
					format = BLFormat.MONO;
			} else if ((record_type.equals("c")) || (record_type.equals("d"))) {
				format = BLFormat.SCORE;
			} else if ((record_type.equals("e")) || (record_type.equals("f"))) {
				format = BLFormat.MAP;
			} else if (record_type.equals("g")) {
				format = BLFormat.VIDEO;
			} else if (record_type.equals("i")) {
				format = BLFormat.NONMUSIC;
			} else if (record_type.equals("j")) {
				format = BLFormat.MUSIC;
			} else if (record_type.equals("k")) {
				format = BLFormat.IMG;
			} else if (record_type.equals("m")) {
				if (statCodes.contains("evideo"))
					format = BLFormat.VIDEO;
				else if (statCodes.contains("eaudio"))
					format = BLFormat.MUSIC;
				else if (statCodes.contains("escore"))
					format = BLFormat.SCORE;
				else if (statCodes.contains("emap"))
					format = BLFormat.MAP;
				else
					format = BLFormat.FILE;
			} else if (record_type.equals("o")) {
				format = BLFormat.KIT;
			} else if (record_type.equals("p")) { // p means "mixed materials", classifying as 
				Iterator<String> iter = loccodes.iterator();   // archival if in rare location
				while (iter.hasNext()) {
					String loccode = iter.next();
					if (rareLocCodes.contains(loccode)) {
						format = BLFormat.MANARCH; //ARCHIVAL
						break;
					}
				}
			} else if (record_type.equals("r")) {
				format = BLFormat.OBJ;
			}
		}
		if (format == null) {
			if (record_type.equals("t"))
				 // This includes all bibliographic_levels but 'a' captured above. MANUSCRIPT
				format = BLFormat.MANARCH;
			else if (category.equals("q"))
				format = BLFormat.SCORE;
			else if (category.equals("v"))
				format = BLFormat.VIDEO;
			else
				format = BLFormat.MISC;

		}

		SolrFields sfs = new SolrFields();
		if (isThesis) {  //Thesis is an "additional" format, and won't override main format entry.
			sfs.add("format",BLFormat.THESIS.display());
			if (format.equals(BLFormat.MANARCH)) // unless the main format is Manuscript/Archive
				format = null;
		}
		if (isMicroform) {  //Microform is an "additional" format, and won't override main format entry.
			sfs.add("format",BLFormat.MICRO.display());
		}

		if (format != null) {
			sfs.add("format",format.display());
			sfs.add("format_main_facet",format.display());
		} else if (isThesis)
			sfs.add("format_main_facet",BLFormat.THESIS.display());
		else if (isMicroform)
			sfs.add("format_main_facet",BLFormat.MICRO.display());

		sfs.add("bib_format_display", record_type + bibliographic_level);
		sfs.add("database_b",isDatabase);
		return sfs;
	}

	@Override
	public SolrFields generateNonMarcSolrFields( Map<String,Object> instance, Config config ) throws IOException {
		BLFormat format = BLFormat.MONO; //default
		if (instance.containsKey("instanceTypeId")) {
			if ( resourceTypes == null )
				resourceTypes = new ReferenceData(config.getFolio("Folio"),"/instance-types","name");
			String resourceType = resourceTypes.getName((String)instance.get("instanceTypeId"));
			if ( resourceType == null )
				// Most likely someone has been editing resource types in the last few hours. Very rare.
				System.out.printf("instanceTypeId %s not known resource type.\n",
						(String)instance.get("instanceTypeId"));
			else
				switch (resourceType) {
				case "borrow direct":
					break;

				case "cartographic dataset":
				case "cartographic image":
				case "cartographic moving image":
				case "cartographic tactile image":
				case "cartographic tactile three-dimensional form":
				case "cartographic three-dimensional form":
					format = BLFormat.MAP;
					break;

				case "computer dataset":
					format = BLFormat.DB;
					break;

				case "computer program":
					format = BLFormat.FILE;
					break;

				case "interlibrary loan":
					break; // not format metadata

				case "notated movement":
				case "notated music":
					format = BLFormat.SCORE;
					break;

				case "other":
					break;

				case "performed music":
					format = BLFormat.MUSIC;
					break;

				case "sounds":
				case "spoken word":
					format = BLFormat.NONMUSIC;
					break;

				case "still image":
				case "tactile image":
					format = BLFormat.IMG;
					break;

				case "tactile notated movement":
				case "tactile notated music":
					format = BLFormat.SCORE;
					break;

				case "tactile text":
					break; //Book

				case "tactile three-dimensional form":
					format = BLFormat.OBJ;
					break;

				case "text":
				case "Text (Check 336$b)":
					break; //Book? Serial?

				case "three-dimensional form":
					format = BLFormat.OBJ;
					break;

				case "three-dimensional moving image":
				case "two-dimensional moving image":
					format = BLFormat.VIDEO;
					break;

				case "unspecified":
					break;

				default:
					System.out.printf("Resource type '%s' not expected.\n",resourceType);
					break;
			}
		}
		SolrFields sfs = new SolrFields();
		sfs.add("format",format.display());
		sfs.add("format_main_facet",format.display());
		// bib_format_display -We lack this data, but may need to fake it if we need Aeon compatibility
		sfs.add("database_b",false);// we currently have no instance flags for this
		return sfs;

	}

	private static ReferenceData folioLocations = null;

	static ReferenceData resourceTypes = null;

	private enum BLFormat {
		MONO    ("Book"),
		SERIAL  ("Journal/Periodical"),
		MICRO   ("Microform"),
		MUSIC   ("Musical Recording"),
		THESIS  ("Thesis"),
		SCORE   ("Musical Score"),
		VIDEO   ("Video"),
		MAP     ("Map"),
		MANARCH ("Manuscript/Archive"),
		SITE    ("Website"),
		NONMUSIC("Non-musical Recording"),
		DB      ("Database"),
		FILE    ("Computer File"),
		IMG     ("Image"),
		MISC    ("Miscellaneous"),
		KIT     ("Kit"),
		OBJ     ("Object"),
		RESEARCH("Research Guide"),
		GUIDE   ("Course Guide"),
		LIBGUIDE("Library Guide");

		private String display;
		BLFormat( String display ) { this.display = display; }

		public String display() { return this.display; }
	}
}
