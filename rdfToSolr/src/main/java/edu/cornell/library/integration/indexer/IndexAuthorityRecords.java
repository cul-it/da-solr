package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.removeTrailingPunctuation;
import static edu.cornell.library.integration.indexer.utilities.BrowseUtils.getSortHeading;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.http.ConnectionClosedException;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.RecordSet;

public class IndexAuthorityRecords {

	private Connection connection = null;
	private DavService davService;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = new HashSet<String>();
		requiredArgs.add("xmlDir");
	            
		VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig(args,requiredArgs);
		try {
  //  	   IndexAuthorityRecords iar =
    			   new IndexAuthorityRecords(config);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public IndexAuthorityRecords(VoyagerToSolrConfiguration config) throws Exception {
        this.davService = DavServiceFactory.getDavService(config);
        		
		connection = config.getDatabaseConnection(1);
		//set up database (including populating description maps)
		setUpDatabaseTypeLists();
		
		String xmlDir = config.getWebdavBaseUrl() + "/" + config.getXmlDir();
		System.out.println("Looking for authority xml in directory: "+xmlDir);
        List<String> authXmlFiles = davService.getFileUrlList(xmlDir);
        System.out.println("Found: "+authXmlFiles.size());
        Iterator<String> i = authXmlFiles.iterator();
        while (i.hasNext()) {
			String srcFile = i.next();
			System.out.println(srcFile);
			InputStream xmlstream = davService.getFileAsInputStream(srcFile);
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			XMLStreamReader r  = 
					input_factory.createXMLStreamReader(xmlstream);
			try {
				processRecords(r);
			} catch (ConnectionClosedException e) {
				System.out.println("Lost access to xml file read. Waiting 2 minutes, and will try again.\n");
				try {
				    Thread.sleep(120 /* s */ * 1000 /* ms/s */);
				} catch(InterruptedException ex) {
				    Thread.currentThread().interrupt();
				}
				System.out.println("Attempting to re-open xml file input.");
				xmlstream = davService.getFileAsInputStream(srcFile);
				input_factory = XMLInputFactory.newInstance();
				r  = input_factory.createXMLStreamReader(xmlstream);
				processRecords(r);
			}
			xmlstream.close();
        }
        connection.close();
	}
	
	private void setUpDatabaseTypeLists() throws SQLException {
		Statement stmt = connection.createStatement();
		stmt.executeUpdate("DELETE FROM record_set");
		
		PreparedStatement insertType = connection.prepareStatement("INSERT INTO record_set (id,name) VALUES (? , ?)");
		for ( RecordSet rs : RecordSet.values()) {
			insertType.setInt(1, rs.ordinal());
			insertType.setString(2, rs.toString());
			insertType.executeUpdate();
		}
		
		stmt.executeUpdate("DELETE FROM type_desc");
		PreparedStatement insertDesc = connection.prepareStatement("INSERT INTO type_desc (id,name) VALUES (? , ?)");
		for ( HeadTypeDesc ht : HeadTypeDesc.values()) {
			insertDesc.setInt(1, ht.ordinal());
			insertDesc.setString(2, ht.toString());
			insertDesc.executeUpdate();
		}
		
		stmt.executeUpdate("DELETE FROM ref_type");
		PreparedStatement insertRefType = connection.prepareStatement("INSERT INTO ref_type (id,name) VALUES (? , ?)");
		for ( ReferenceType rt : ReferenceType.values()) {
			insertRefType.setInt(1, rt.ordinal());
			insertRefType.setString(2, rt.toString());
			insertRefType.executeUpdate();
		}

		stmt.close();
		insertType.close();
		insertDesc.close();
		insertRefType.close();
	}

	private void processRecords (XMLStreamReader r) throws Exception {
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("record")) {
					MarcRecord rec = processRecord(r);
					createHeadingRecordsFromAuthority(rec);
				}
		}
	}

	private void createHeadingRecordsFromAuthority( MarcRecord rec ) throws SQLException  {
		String heading = null;
		String headingSort = null;
		HeadTypeDesc htd = null;
		RecordSet rs = null;
		Collection<Relation> sees = new HashSet<Relation>();
		Collection<Relation> seeAlsos = new HashSet<Relation>();
		Collection<String> expectedNotes = new HashSet<String>();
		Collection<String> foundNotes = new HashSet<String>();
		Collection<String> notes = new HashSet<String>();
		Boolean isUndifferentiated = false;
		
		for (ControlField f : rec.control_fields.values()) {
			if (f.tag.equals("008")) {
				if (f.value.length() >= 10) {
					Character recordType = f.value.charAt(9);
					if (recordType.equals('b')) 
						expectedNotes.add("666");
					else if (recordType.equals('c'))
						expectedNotes.add("664");
					Character undifferentiated = f.value.charAt(32);
					if (undifferentiated.equals('b'))
						isUndifferentiated = true;
				}
			}
		}
		// iterate through fields. Look for main heading and alternate forms.
		Iterator<DataField> i = rec.data_fields.values().iterator();
		while (i.hasNext()) {
			DataField f = i.next();
			if (f.tag.startsWith("1")) {
				// main heading
				heading = f.concatValue("");
				if (f.tag.equals("100") || f.tag.equals("110") || f.tag.equals("111")) {
					rs = RecordSet.NAME;
					Iterator<Subfield> j = f.subfields.values().iterator();
					while (j.hasNext()) {
						Subfield sf = j.next();
						if (sf.code.equals('t')) {
							rs = RecordSet.NAMETITLE;
							break;
						}
					}
					if (f.tag.equals("100")) {
						htd = HeadTypeDesc.PERSNAME;
					} else if (f.tag.equals("110")) {
						htd = HeadTypeDesc.CORPNAME;
					} else {
						htd = HeadTypeDesc.EVENT;
					}
				}
				if (f.tag.equals("130")) {
					htd = HeadTypeDesc.GENHEAD;
					rs = RecordSet.SUBJECT;
				}
				if (f.tag.equals("150")) {
					rs = RecordSet.SUBJECT;
					htd = HeadTypeDesc.TOPIC;
				} else if (f.tag.equals("151")) {
					rs = RecordSet.SUBJECT;
					htd = HeadTypeDesc.GEONAME;
				} else if (f.tag.equals("148")) {
					rs = RecordSet.SUBJECT;
					htd = HeadTypeDesc.CHRONTERM;
				} else if (f.tag.equals("155")) {
					rs = RecordSet.SUBJECT;
					htd = HeadTypeDesc.GENRE;
				} else if (f.tag.equals("162")) {
					rs = RecordSet.SUBJECT;
					htd = HeadTypeDesc.MEDIUM;
				}
				// If the record is for a subdivision (main entry >=180),
				// we won't do anything with it.
			} else if (f.tag.equals("260") || f.tag.equals("360")) {
				notes.add("Search under: "+f.concatValue(""));
			} else if (f.tag.startsWith("4")) {
				// equivalent values
				Relation r = determineRelationship(f);
				if (r != null) {
					expectedNotes.addAll(r.expectedNotes);
					r.heading = buildXRefHeading(f,heading);
					r.headingOrig = f.concatValue("iw");
					r.headingSort = getSortHeading( r.heading );
					for (Relation s : sees) 
						if (s.headingSort.equals(r.headingSort))
							r.display = false;
					sees.add(r);
				}
			} else if (f.tag.startsWith("5")) {
				// see alsos
				Relation r = determineRelationship(f);
				if (r != null) {
					expectedNotes.addAll(r.expectedNotes);
					r.heading = buildXRefHeading(f,heading);
					r.headingOrig = f.concatValue("iw");
					r.headingSort = getSortHeading( r.heading );
					for (Relation s : seeAlsos) 
						if (s.headingSort.equals(r.headingSort))
							r.display = false;
					seeAlsos.add(r);
				}
			} else if (f.tag.equals("663")) {
				foundNotes.add("663");
				if (expectedNotes.contains("663")) {
					notes.add(f.concatValue(""));
				} else {
					System.out.println("Field 663 found, but no matching 4XX or 5XX subfield w. "+rec.id);
				}
			} else if (f.tag.equals("664")) {
				foundNotes.add("664");
				if (expectedNotes.contains("664")) {
					notes.add(f.concatValue(""));
				} else {
					System.out.println("Field 664 found, but no matching 4XX or 5XX subfield w or matching record type: c. "+rec.id);
				}
			} else if (f.tag.equals("665")) {
				foundNotes.add("665");
				if (expectedNotes.contains("665")) {
					notes.add(f.concatValue(""));
				} else {
					System.out.println("Field 665 found, but no matching 4XX or 5XX subfield w. "+rec.id);
				}
			} else if (f.tag.equals("666")) {
				foundNotes.add("666");
				if (expectedNotes.contains("666")) {
					notes.add(f.concatValue(""));
				} else {
					System.out.println("Field 666 found, but no matching record type: b. "+rec.id);
				}
			}
		}
		if (heading == null || rs == null || htd == null) {
			System.out.println("Not deriving heading browse entries from record. "+rec.id);
			return;
		}
		headingSort = getSortHeading(heading);

		// Populate Main Entry Heading Record
		Integer heading_id = getMainHeadingRecordId(heading,headingSort,rs, htd);
		for (String note : notes)
			insertNote(heading_id, note);
		
		// Populate alternate forms as direct references for see "from" tracings. 
		// Records marked as undifferentiated will have see tracings that refer to different
		// people. By not populating them here, we avoid cross populating them into bib records.
		if ( ! isUndifferentiated ) 
			for (Relation r : sees )
				insertAltForm(heading_id, r.headingOrig);

		expectedNotes.removeAll(foundNotes);
		if ( ! expectedNotes.isEmpty())
			System.out.println("Expected notes based on 4XX and/or 5XX subfield ws that didn't appear. "+rec.id);
		
		
		// Populate incoming 4XX cross references
		for (Relation r: sees) {
			if ( ! r.display) continue;
			if ( r.headingSort.equals(headingSort)) continue;
			crossRef(heading_id,rs,r,ReferenceType.FROM4XX);
		}
		
		for (Relation r: seeAlsos) {
			// Populate incoming 5XX cross references
			if ( ! r.display) continue;
			if ( r.headingSort.equals(headingSort)) continue;
			crossRef(heading_id,rs,r,ReferenceType.FROM5XX);

			// Where appropriate, populate outgoing 5XX cross references
			if (r.reciprocalRelationship != null)
				directRef(heading_id,rs,r,ReferenceType.TO5XX);
		}

		return;
	}


	private void insertAltForm(Integer heading_id, String form) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement(
				"INSERT INTO alt_form (heading_id, form) VALUES (?, ?)");
		stmt.setInt(1, heading_id);
		stmt.setString(2, form);
		stmt.executeUpdate();
		stmt.close();		
	}


	private void insertNote(Integer heading_id, String note) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement(
				"INSERT INTO note (heading_id, note) VALUES (? , ?)");
		stmt.setInt(1, heading_id);
		stmt.setString(2, note);
		stmt.executeUpdate();
		stmt.close();
	}

	private Integer getMainHeadingRecordId(String heading, String headingSort,
			RecordSet recordSet, HeadTypeDesc htd) throws SQLException {
		PreparedStatement pstmt = connection.prepareStatement(
				"SELECT id FROM heading " +
				"WHERE record_set = ? AND type_desc = ? AND sort = ?");
		pstmt.setInt(1, recordSet.ordinal());
		pstmt.setInt(2, htd.ordinal());
		pstmt.setString(3, headingSort);
		ResultSet resultSet = pstmt.executeQuery();
		Integer recordId = null;
		while (resultSet.next()) {
			recordId = resultSet.getInt("id");
		}
		pstmt.close();
		if (recordId != null) {
			// update sql record to make sure it's a main entry now; heading overrides
			// possibly different heading form populated from xref
			PreparedStatement pstmt1 = connection.prepareStatement(
					"UPDATE heading SET main_entry = 1, heading = ? WHERE id = ?");
			pstmt1.setString(1, heading); 
			pstmt1.setInt(2, recordId);
			pstmt1.executeUpdate();
			pstmt1.close();
		} else {
			// create new record
			PreparedStatement pstmt1 = connection.prepareStatement(
					"INSERT INTO heading (heading, sort, record_set, type_desc, authority, main_entry) " +
					"VALUES (?, ?, ?, ?, 1, 1)",
                    Statement.RETURN_GENERATED_KEYS);
			pstmt1.setString(1, heading);
			pstmt1.setString(2, headingSort);
			pstmt1.setInt(3, recordSet.ordinal());
			pstmt1.setInt(4, htd.ordinal());
			int affectedCount = pstmt1.executeUpdate();
			if (affectedCount < 1) 
				throw new SQLException("Creating Heading Record Failed.");
			ResultSet generatedKeys = pstmt1.getGeneratedKeys();
			if (generatedKeys.next())
				recordId = generatedKeys.getInt(1);
			pstmt1.close();
		}
 		
		return recordId;
	}

	/* If there are no more than 5 non-period characters in the heading,
	 * and all of those are capital letters, then this is an acronym.
	 */
	private String buildXRefHeading( DataField f , String mainHeading ) {
		String heading = f.concatValue("iw");
		String headingWOPeriods = heading.replaceAll("\\.", "");
		if (headingWOPeriods.length() > 5) return heading;
		boolean upperCase = true;
		for (char c : headingWOPeriods.toCharArray()) {
			if ( ! Character.isUpperCase(c)) {
				upperCase = false;
				break;
			}
		}
		if (upperCase)
			return heading + " (" + mainHeading + ")";
		else
			return heading;

	}
	
	private void crossRef(Integer heading_id, RecordSet rs, Relation r, ReferenceType rt) throws SQLException {
		int from_heading_id = getRelationshipHeadingId( r, rs );
		insertRef(from_heading_id, heading_id, rt, r.relationship);
	}

	private void directRef(int heading_id, RecordSet rs, Relation r, ReferenceType rt) throws SQLException {
		int dest_heading_id = getRelationshipHeadingId( r, rs );
		insertRef(heading_id, dest_heading_id, rt, r.reciprocalRelationship );
	}
	
	private void insertRef(int from_id, int to_id,
			ReferenceType rt, String relationshipDescription) throws SQLException {

		PreparedStatement pstmt = connection.prepareStatement(
				"INSERT INTO reference (from_heading, to_heading, ref_type, ref_desc)"
				+ " VALUES (?, ?, ?, ?)");
		pstmt.setInt(1, from_id);
		pstmt.setInt(2, to_id);
		pstmt.setInt(3, rt.ordinal());
		if (relationshipDescription == null)
			pstmt.setNull(4, java.sql.Types.VARCHAR);
		else
			pstmt.setString(4, relationshipDescription);
		pstmt.executeUpdate();
		pstmt.close();
	}


	private int getRelationshipHeadingId(Relation r, RecordSet recordSet ) throws SQLException {
		PreparedStatement pstmt = connection.prepareStatement(
				"SELECT id FROM heading " +
				"WHERE record_set = ? AND type_desc = ? and sort = ?");
		pstmt.setInt(1, recordSet.ordinal());
		pstmt.setInt(2, r.headingTypeDesc.ordinal());
		pstmt.setString(3, r.headingSort);
		ResultSet resultSet = pstmt.executeQuery();
		Integer recordId = null;
		while (resultSet.next()) {
			recordId = resultSet.getInt("id");
		}
		pstmt.close();
		if (recordId == null) {
			// create new record
			PreparedStatement pstmt1 = connection.prepareStatement(
					"INSERT INTO heading (heading, sort, record_set, type_desc, authority) " +
					"VALUES (?, ?, ?, ?, 1)",
                    Statement.RETURN_GENERATED_KEYS);
			pstmt1.setString(1, r.heading);
			pstmt1.setString(2, r.headingSort);
			pstmt1.setInt(3, recordSet.ordinal());
			pstmt1.setInt(4, r.headingTypeDesc.ordinal());
			int affectedCount = pstmt1.executeUpdate();
			if (affectedCount < 1) 
				throw new SQLException("Creating Heading Record Failed.");
			ResultSet generatedKeys = pstmt1.getGeneratedKeys();
			if (generatedKeys.next())
				recordId = generatedKeys.getInt(1);
			pstmt1.close();
		}
 		
		return recordId;
	}


	private Relation determineRelationship( DataField f ) {
		// Is there a subfield w? The relationship note in subfield w
		// describes the 4XX or 5XX heading, and must be reversed for the
		// from tracing.
		Relation r = new Relation();
		boolean hasW = false;
		
		if (f.tag.endsWith("00"))
			r.headingTypeDesc = HeadTypeDesc.PERSNAME;
		else if (f.tag.endsWith("10")) 
			r.headingTypeDesc = HeadTypeDesc.CORPNAME;
		else if (f.tag.endsWith("11"))
			r.headingTypeDesc = HeadTypeDesc.EVENT;
		else if (f.tag.endsWith("30"))
			r.headingTypeDesc = HeadTypeDesc.GENHEAD;
		else if (f.tag.endsWith("50"))
			r.headingTypeDesc = HeadTypeDesc.TOPIC;
		else if (f.tag.endsWith("48"))
			r.headingTypeDesc = HeadTypeDesc.CHRONTERM;
		else if (f.tag.endsWith("51"))
			r.headingTypeDesc = HeadTypeDesc.GEONAME;
		else if (f.tag.endsWith("55"))
			r.headingTypeDesc = HeadTypeDesc.GENRE;
		else if (f.tag.endsWith("62"))
			r.headingTypeDesc = HeadTypeDesc.MEDIUM;
		else return null;
		
		
		for (Subfield sf : f.subfields.values()) {
			if (sf.code.equals('w')) {
				hasW = true;
				
				if (sf.value.startsWith("a")) {
					//earlier heading
					r.relationship = "Later Heading";
				} else if (sf.value.startsWith("b")) {
					//later heading
					r.relationship = "Earlier Heading";
				} else if (sf.value.startsWith("d")) {
					//acronym
					r.relationship = "Full Heading";
				} else if (sf.value.startsWith("f")) {
					//musical composition
					r.relationship = "Musical Composition Based on this Work";
				} else if (sf.value.startsWith("g")) {
					//broader term
					r.relationship = "Narrower Term";
				} else if (sf.value.startsWith("h")) {
					//narrower term
					r.relationship = "Broader Term";
				} else if (sf.value.startsWith("i")) {
					// get relationship name from subfield i
				} else if (sf.value.startsWith("r")) {
					// get relationship name from subfield i
					//  also something about subfield 4? Haven't seen one.
				} else if (sf.value.startsWith("t")) {
					//parent body
					r.relationship = "Parent Body";
				}
				
				if (sf.value.length() >= 2) {
					Character offset1 = sf.value.charAt(1);
					if (offset1.equals('a')) {
						r.applicableContexts.add(RecordSet.NAME);
					} else if (offset1.equals('b')) {
						r.applicableContexts.add(RecordSet.SUBJECT);
					} else if (offset1.equals('c')) {
						r.applicableContexts.add(RecordSet.SERIES);
					} else if (offset1.equals('d')) {
						r.applicableContexts.add(RecordSet.NAME);
						r.applicableContexts.add(RecordSet.SUBJECT);
					} else if (offset1.equals('e')) {
						r.applicableContexts.add(RecordSet.NAME);
						r.applicableContexts.add(RecordSet.SERIES);
					} else if (offset1.equals('f')) {
						r.applicableContexts.add(RecordSet.SUBJECT);
						r.applicableContexts.add(RecordSet.SERIES);
					} else { // g (or other)
						r.applicableContexts.add(RecordSet.NAME);
						r.applicableContexts.add(RecordSet.SUBJECT);
						r.applicableContexts.add(RecordSet.SERIES);
					}
				} else {
					r.applicableContexts.add(RecordSet.NAME);
					r.applicableContexts.add(RecordSet.SUBJECT);
					r.applicableContexts.add(RecordSet.SERIES);
				}
				
				if (sf.value.length() >= 3) {
					Character offset2 = sf.value.charAt(2);
					if (offset2.equals('a')) {
						r.relationship = "Later Form of Heading";
					}
				}
				
				if (sf.value.length() >= 4) {
					Character offset3 = sf.value.charAt(3);
					if (offset3.equals('a')) {
						r.display = false;
					} else if (offset3.equals('b')) {
						r.display = false;
						r.expectedNotes.add("664");
					} else if (offset3.equals('c')) {
						r.display = false;
						r.expectedNotes.add("663");
					} else if (offset3.equals('d')) {
						r.display = false;
						r.expectedNotes.add("665");
					}
				}
			} else if (sf.code.equals('i')) {
				String rel = removeTrailingPunctuation(sf.value,": ").trim();
				if (rel.equalsIgnoreCase("alternate identity"))
					r.relationship = "Real Identity";
				else if (rel.equalsIgnoreCase("real identity"))
					r.relationship = "Alternate Identity";
				else if (rel.equalsIgnoreCase("family member"))
					r.relationship = "Family";
				else if (rel.equalsIgnoreCase("family"))
					r.relationship = "Family Member";
				else if (rel.equalsIgnoreCase("progenitor"))
					r.relationship = "Descendants";
				else if (rel.equalsIgnoreCase("descendants"))
					r.relationship = "Progenitor";
				else if (rel.equalsIgnoreCase("employee"))
					r.relationship = "Employer";
				else if (rel.equalsIgnoreCase("employer")) {
					r.relationship = "Employee";
					r.reciprocalRelationship = "Employer";
				// The reciprocal relationship to descendant family is missing
				// from the RDA spec (appendix K), so it's unlikely that
				// "Progenitive Family" will actually appear. Of the three
				// adjective forms of "Progenitor" I found in the OED (progenital,
				// progenitive, progenitorial), progenitive has the highest level
				// of actual use according to my Google NGrams search
				} else if (rel.equalsIgnoreCase("Descendant Family")) 
					r.relationship = "Progenitive Family";
				else if (rel.equalsIgnoreCase("Progenitive Family"))
					r.relationship = "Descendant Family";
			}
		}
		if ( ! hasW ) {
			r.applicableContexts.add(RecordSet.NAME);
			r.applicableContexts.add(RecordSet.SUBJECT);
			r.applicableContexts.add(RecordSet.SERIES);
		}
		return r;
	}
	
	private static class Relation {	
		public String relationship = null;
		public String reciprocalRelationship = null;
		public String heading = null;
		public String headingOrig = null; // access to original heading before
		                                  // parenthesized main heading optionally added.
		public String headingSort = null;
		public HeadTypeDesc headingTypeDesc = null;
		public Collection<RecordSet> applicableContexts = new HashSet<RecordSet>();
		public Collection<String> expectedNotes = new HashSet<String>();
		boolean display = true;
	}

	
	public static enum ReferenceType {
		TO4XX("alternateForm"),
		FROM4XX("preferedForm"),
		TO5XX("seeAlso"),
		FROM5XX("seeAlso");
		
		private String string;
		
		private ReferenceType(String name) {
			string = name;
		}

		public String toString() { return string; }
		
	}

	
	// general MARC methods and classes start here
	
	private MarcRecord processRecord( XMLStreamReader r ) throws Exception {
		
		MarcRecord rec = new MarcRecord();
		int id = 0;
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("END_ELEMENT")) {
				if (r.getLocalName().equals("record")) 
					return rec;
			}
			if (event.equals("START_ELEMENT")) {
				if (r.getLocalName().equals("leader")) {
					rec.leader = r.getElementText();
				} else if (r.getLocalName().equals("controlfield")) {
					ControlField f = new ControlField();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag"))
							f.tag = r.getAttributeValue(i);
					f.value = r.getElementText();
					if (f.tag.equals("001"))
						rec.id = f.value;
					rec.control_fields.put(f.id, f);
				} else if (r.getLocalName().equals("datafield")) {
					DataField f = new DataField();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag"))
							f.tag = r.getAttributeValue(i);
						else if (r.getAttributeLocalName(i).equals("ind1"))
							f.ind1 = r.getAttributeValue(i).charAt(0);
						else if (r.getAttributeLocalName(i).equals("ind2"))
							f.ind2 = r.getAttributeValue(i).charAt(0);
					f.subfields = processSubfields(r);
					rec.data_fields.put(f.id, f);
				}
		
			}
		}
		return rec;
	}

	private static Map<Integer,Subfield> processSubfields( XMLStreamReader r ) throws Exception {
		Map<Integer,Subfield> fields = new HashMap<Integer,Subfield>();
		int id = 0;
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("END_ELEMENT"))
				if (r.getLocalName().equals("datafield"))
					return fields;
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("subfield")) {
					Subfield f = new Subfield();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("code"))
							f.code = r.getAttributeValue(i).charAt(0);
					f.value = r.getElementText();
					fields.put(f.id, f);
				}
		}
		return fields; // We should never reach this line.
	}


	private final static String getEventTypeString(int  eventType)
	{
	  switch  (eventType)
	    {
	        case XMLEvent.START_ELEMENT:
	          return "START_ELEMENT";
	        case XMLEvent.END_ELEMENT:
	          return "END_ELEMENT";
	        case XMLEvent.PROCESSING_INSTRUCTION:
	          return "PROCESSING_INSTRUCTION";
	        case XMLEvent.CHARACTERS:
	          return "CHARACTERS";
	        case XMLEvent.COMMENT:
	          return "COMMENT";
	        case XMLEvent.START_DOCUMENT:
	          return "START_DOCUMENT";
	        case XMLEvent.END_DOCUMENT:
	          return "END_DOCUMENT";
	        case XMLEvent.ENTITY_REFERENCE:
	          return "ENTITY_REFERENCE";
	        case XMLEvent.ATTRIBUTE:
	          return "ATTRIBUTE";
	        case XMLEvent.DTD:
	          return "DTD";
	        case XMLEvent.CDATA:
	          return "CDATA";
	        case XMLEvent.SPACE:
	          return "SPACE";
	    }
	  return  "UNKNOWN_EVENT_TYPE ,   "+ eventType;
	}

	
	static class MarcRecord {
		
		public String leader;
		public Map<Integer,ControlField> control_fields 
									= new HashMap<Integer,ControlField>();
		public Map<Integer,DataField> data_fields
									= new HashMap<Integer,DataField>();
		public RecordType type;
		public String id;
		public String bib_id;
		
		public String toString( ) {
			
			StringBuilder sb = new StringBuilder();
			sb.append("000    "+this.leader+"\n");
			int id = 0;
			while( this.control_fields.containsKey(id+1) ) {
				ControlField f = this.control_fields.get(++id);
				sb.append(f.tag + "    " + f.value+"\n");
			}

			while( this.data_fields.containsKey(id+1) ) {
				DataField f = this.data_fields.get(++id);
				sb.append(f.toString());
				sb.append("\n");
			}
			return sb.toString();
		}

	}
	
	static class ControlField {
		
		public int id;
		public String tag;
		public String value;
	}
	
	static class DataField {
		
		public int id;
		public String tag;
		public Character ind1;
		public Character ind2;
		public Map<Integer,Subfield> subfields;

		// Linked field number if field is 880
		public String alttag;
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(this.tag);
			sb.append(" ");
			sb.append(this.ind1);
			sb.append(this.ind2);
			sb.append(" ");
			int sf_id = 0;
			while( this.subfields.containsKey(sf_id+1) ) {
				Subfield sf = this.subfields.get(++sf_id);
				sb.append(sf.toString());
				sb.append(" ");
			}
			return sb.toString();
		}
		public String concatValue(String excludes) {
			StringBuilder sb = new StringBuilder();
			int sf_id = 0;
			while( this.subfields.containsKey(sf_id+1) ) {
				Subfield sf = this.subfields.get(++sf_id);
				if (excludes.contains(sf.code.toString())) continue;
				sb.append(sf.value);
				sb.append(" ");
			}
			return sb.toString().trim();			
		}
	}
	
	static class Subfield {
		
		public int id;
		public Character code;
		public String value;

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("\u2021");
			sb.append(this.code);
			sb.append(" ");
			sb.append(this.value);
			return sb.toString();
		}
	}
	
	static enum RecordType {
		BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
	}

	

}
