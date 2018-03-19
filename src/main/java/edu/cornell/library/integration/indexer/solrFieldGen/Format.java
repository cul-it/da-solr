package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.nodeToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.BooleanSolrField;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

/**
 * process various record values into complicated determination of item format.  
 */
public class Format implements ResultSetToFields, SolrFieldGenerator {

	protected boolean debug = false;

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
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, Config config) throws Exception {

		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		JenaResultsToMarcRecord.addControlFieldResultSet(rec,results.get("007"));
		JenaResultsToMarcRecord.addControlFieldResultSet(rec,results.get("008"));
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("245"));
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("502"));
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("652"));
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("948"));
		Map<String,MarcRecord> holdingRecs = new HashMap<>();
		while ( results.get("holdings_852").hasNext() ) {
			QuerySolution sol = results.get("holdings_852").nextSolution();
			String recordURI = nodeToString(sol.get("mfhd"));
			MarcRecord holdingRec;
			if (holdingRecs.containsKey(recordURI)) {
				holdingRec = holdingRecs.get(recordURI);
			} else {
				holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
				holdingRec.id = recordURI.substring(recordURI.lastIndexOf('/')+1);
				holdingRecs.put(recordURI, holdingRec);
			}
			JenaResultsToMarcRecord.addDataFieldQuerySolution(holdingRec,sol);
		}
		rec.holdings.addAll(holdingRecs.values());
		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, config );
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);
		for ( BooleanSolrField f : vals.boolFields ) {
			SolrInputField boolField = new SolrInputField(f.fieldName);
			boolField.setValue(f.fieldValue, 1.0f);
			fields.put(f.fieldName, boolField);
		}
		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList("leader","007","008","245","502","653","948","holdings");
	}

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) {

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
		List<String> sf948fs = new ArrayList<>();
		Boolean isThesis = false;
		for (DataField f : rec.dataFields)
			switch (f.tag) {
			case "245":	for (Subfield sf : f.subfields)	if (sf.code.equals('h')) sf245hs.add(sf.value);	break;
			case "502":	isThesis = true; break;
			case "653":	for (Subfield sf : f.subfields)	if (sf.code.equals('a')) sf653as.add(sf.value); break;
			case "948":	for (Subfield sf : f.subfields) if (sf.code.equals('f')) sf948fs.add(sf.value);
			}

		Collection<String> loccodes = new HashSet<>();
		for (MarcRecord hRec : rec.holdings)
			for (DataField f : hRec.dataFields)
				for (Subfield sf : f.subfields)
					if (sf.code.equals('b')) loccodes.add(sf.value);

		Boolean isDatabase = false;
		Boolean isMicroform = false;
		String format = null;
		
		if (category.equals("h")) {
			isMicroform = true;
			if (debug) System.out.println("Microform due to category:h.");
		}

		Iterator<String> i = sf245hs.iterator();
		while (i.hasNext())
			if (i.next().toLowerCase().contains("[electronic resource]")) {
				Iterator<String> j = sf948fs.iterator();
				while (j.hasNext())
					if (j.next().toLowerCase().equals("j")) {
						format = "Journal/Periodical";
						if (debug) System.out.println("Journal/Periodical format due to 245h: [electronic resource] and 948f: j.");
					}			
			}


		i = sf948fs.iterator();
		while (i.hasNext()) {
			String val = i.next();
			if ((val.equals("fd")) || (val.equals("webfeatdb"))) {
				format = "Database";
				if (debug) System.out.println("format:Database due to 948f: webfeatdb OR fd.");
				
				// format:Database differs from database_b flag.
				// The latter is used for Database ERMS.
				if (val.equals("webfeatdb")) {
					isDatabase = true;
					if (debug) System.out.println("Database_b true due to 948f: webfeatdb.");
				}
			}
		}

		i = sf653as.iterator();
		while (i.hasNext()) {
			String val = i.next();
			if (val.equalsIgnoreCase("research guide")) {
				format = "Research Guide";
				if (debug) System.out.println("format:Research Guide due to 653a: research guide.");
			} else if (val.equalsIgnoreCase("course guide")) {
				format = "Course Guide";
				if (debug) System.out.println("format:Course Guide due to 653a: course guide.");
			} else if (val.equalsIgnoreCase("library guide")) {
				format = "Library Guide";
				if (debug) System.out.println("format:Library Guide due to 653a: library guide.");
			}
		}

		if (format == null) {
			if (record_type.equals("a")) {
				if ((bibliographic_level.equals("a"))
						|| (bibliographic_level.equals("m"))
						|| (bibliographic_level.equals("d"))
						|| (bibliographic_level.equals("c")) ) {
					format = "Book";
					if (debug) System.out.println("Book due to record_type:a and bibliographic_level in: a,m,d,c.");
				} else if ((bibliographic_level.equals("b"))
						|| (bibliographic_level.equals("s"))) {
					format = "Journal/Periodical";
					if (debug) System.out.println("Journal/Periodical due to record_type:a and bibliographic_level in: b,s.");
				} else if (bibliographic_level.equals("i")) {
					if (typeOfContinuingResource.equals("w")) {
						format = "Website";
						if (debug) System.out.println("Website due to record_type:a, bibliographic_level:i and typeOfContinuingResource:w.");
					} else if (typeOfContinuingResource.equals("m")) {
						format = "Book";
						if (debug) System.out.println("Book due to record_type:a, bibliographic_level:i and typeOfContinuingResource:m.");
					} else if (typeOfContinuingResource.equals("d")) {
						format = "Database";
						if (debug) System.out.println("Database due to record_type:a, bibliographic_level:i and typeOfContinuingResource:d.");
					} else if (typeOfContinuingResource.equals("n") || typeOfContinuingResource.equals("p")) {
						format = "Journal/Periodical";
						if (debug) System.out.println("Journal/Periodical due to record_type:a, bibliographic_level:i and typeOfContinuingResource in:n,p.");
					}
				}
			} else if (record_type.equals("t")) {
				if (bibliographic_level.equals("a")) {
					format = "Book";
					if (debug) System.out.println("Book due to record_type:t and bibliographic_level: a.");
				}
			} else if ((record_type.equals("c"))
					|| (record_type.equals("d"))) {
				format = "Musical Score";
				if (debug) System.out.println("Musical Score due to record_type: c or d.");
			} else if ((record_type.equals("e"))
					|| (record_type.equals("f"))) {
				format = "Map or Globe";
				if (debug) System.out.println("Map or Guide due to record_type: e or f.");
			} else if (record_type.equals("g")) {
				format = "Video";
				if (debug) System.out.println("Video due to record_type: g.");
			} else if (record_type.equals("i")) {
				format = "Non-musical Recording";
				if (debug) System.out.println("Non-musical Recording due to record_type: i.");
			} else if (record_type.equals("j")) {
				format = "Musical Recording";
				if (debug) System.out.println("Musical Recording due to record_type: j.");
			} else if (record_type.equals("k")) {
				format = "Image";
				if (debug) System.out.println("Image due to record_type: k.");
			} else if (record_type.equals("m")) {
				if (sf948fs.contains("evideo")) {
					format = "Video";
					if (debug) System.out.println("Video due to record_type: m and 948f: evideo.");
				} else if (sf948fs.contains("eaudio")) {
					format = "Musical Recording";
					if (debug) System.out.println("Musical Recording due to record_type: m and 948f: eaudio.");
				} else if (sf948fs.contains("escore")) {
					format = "Musical Score";
					if (debug) System.out.println("Musical Score due to record_type: m and 948f: escore.");
				} else if (sf948fs.contains("emap")) {
					format = "Map or Globe";
					if (debug) System.out.println("Map or Globe due to record_type: m and 948f: emap.");
				} else {
					format = "Computer File";
					if (debug) System.out.println("Computer File due to record_type: m and 948f not in: eaudio, evideo, escore.");
				}
			} else if (record_type.equals("o")) {
				format = "Kit";
				if (debug) System.out.println("Kit due to record_type: o.");
			} else if (record_type.equals("p")) { // p means "mixed materials", classifying as 
				Iterator<String> iter = loccodes.iterator();   // archival if in rare location
				while (iter.hasNext()) {
					String loccode = iter.next();
					if (rareLocCodes.contains(loccode)) {
						format = "Manuscript/Archive"; //ARCHIVAL
						if (debug) System.out.println("Manuscript/Archive due to record_type: p and loccode on rare location list.");
						break;
					}
				}
				if (debug)
					if ( format == null )
						System.out.println("format:Miscellaneous due to record_type: p and no rare location code.");
			} else if (record_type.equals("r")) {
				format = "Object";
				if (debug) System.out.println("Object due to record_type: r.");
			}
		}
		if (format == null) {
			if (record_type.equals("t")) {
				format = "Manuscript/Archive"; // This includes all bibliographic_levels but 'a',
											   //captured above. MANUSCRIPT
				if (debug) System.out.println("Manuscript/Archive due to record_type: t.");
			} else if (category.equals("q")) {
				format = "Musical Score";
				if (debug) System.out.println("Musical Score due to category:q.");
			} else if (category.equals("v")) {
				format = "Video";
				if (debug) System.out.println("Video due to category:v.");
			} else {
				format = "Miscellaneous";
				if (debug) System.out.println("format:Miscellaneous due to no format conditions met.");
			}
		}

		SolrFields sfs = new SolrFields();
		if (isThesis) {  //Thesis is an "additional" format, and won't override main format entry.
			sfs.add(new SolrField("format","Thesis"));
			if (format.equals("Manuscript/Archive")) {
				format = null;
				if (debug) System.out.println("Not Manuscript/Archive due to collision with format:Thesis.");				
			}
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

		sfs.add(new BooleanSolrField("database_b",isDatabase));
		return sfs;
	}

}
