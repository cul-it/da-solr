package edu.cornell.library.integration.metadata.generator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Currently, the only record types are "Catalog" and "Shadow", where shadow records are 
 * detected through a 948‡h note. Blacklight searches will be filtered to type:Catalog,
 * so only records that should NOT be returned in Blacklight work-level searches should
 * vary from this.
 */
public class HathiLinks implements SolrFieldGenerator {

	private final String hathiLinkTextVolume = "HathiTrust";
	private final String hathiLinkTextTitle  = "HathiTrust (multiple volumes)";
	private final String hathiLinkTextDeny   = "HathiTrust – Access limited to full-text search";

	@Override
	public String getVersion() { return "1.1"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("035"); }

	@Override
	// This field generator uses currently untracked HathiTrust content data, so should be regenerated more often.
	public Duration resultsShelfLife() { return Duration.ofDays(14); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config )
			throws ClassNotFoundException, SQLException, IOException {

		Collection<String> oclcids = new HashSet<>();

		for (DataField f : rec.dataFields) {
			if (f.mainTag.equals("035")) {
				for (Subfield sf : f.subfields)
					if (sf.code.equals('a'))
						if (sf.value.startsWith("(OCoLC"))
							oclcids.add(sf.value.substring(sf.value.lastIndexOf(')')+1));
			}
		}

		SolrFields sfs = new SolrFields();
		try (  Connection conn = config.getDatabaseConnection("Hathi")  )  {			
			Map<String,Collection<String>> availableHathiMaterials = new HashMap<>();
			Collection<String> denyTitles = new HashSet<>();

	/*		if (oclcids.size() > 0) {
	 			PreparedStatement pstmt = conn.prepareStatement
	 					("SELECT Volume_Identifier, UofM_Record_Number, Access FROM raw_hathi"
	 					+ " WHERE FIND_IN_SET( ? , OCLC_Numbers)");
	 			for (String oclcid : oclcids) {
					pstmt.setString(1, oclcid);
					java.sql.ResultSet rs = pstmt.executeQuery();
					tabulateResults(rs);
				}
				pstmt.close();
			} */
			
			try (  PreparedStatement pstmt = conn.prepareStatement
					("SELECT Volume_Identifier, UofM_Record_Number, Access FROM raw_hathi"
					+ " WHERE Source = 'coo' AND Source_Inst_Record_Number = ?")  ) {
				pstmt.setString(1, rec.id);
				try ( java.sql.ResultSet rs = pstmt.executeQuery() ) {
					tabulateResults(rs,availableHathiMaterials,denyTitles); }
			}
			URL url = new URL();
			
			for ( String title : availableHathiMaterials.keySet() ) {
				Collection<String> volumes = availableHathiMaterials.get(title);
				int count = volumes.size();
				if (count == 1) {
					try ( PreparedStatement pstmt = conn.prepareStatement
							("SELECT COUNT(*) as count FROM raw_hathi"
							+ " WHERE UofM_Record_Number = ?")  ) {
						pstmt.setString(1, title);
						try (  java.sql.ResultSet rs = pstmt.executeQuery()  ) {
							while (rs.next())
								count = rs.getInt("count"); }
					}
				}
				if (count == 1) {
					// volume link
					sfs.addAll(url.generateSolrFields(buildMarcWith856(hathiLinkTextVolume,
							"http://hdl.handle.net/2027/"+volumes.iterator().next(), true),null).fields);
				} else {
					// title link
					sfs.addAll(url.generateSolrFields(buildMarcWith856(hathiLinkTextTitle,
							"http://catalog.hathitrust.org/Record/"+title, true),null).fields);
				}
				sfs.add(new SolrField("hathi_title_data",title));
			}
			for ( String title : denyTitles ) {
				sfs.addAll(url.generateSolrFields(buildMarcWith856(hathiLinkTextDeny,
						"http://catalog.hathitrust.org/Record/"+title, false),null).fields);
				sfs.add(new SolrField("hathi_title_data",title));
			}
		}
		return sfs;
	}

	private static MarcRecord buildMarcWith856( String description, String url, boolean online) {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField( 1, "856");
		f.subfields.add(new Subfield( 1, 'u', url));
		f.subfields.add(new Subfield( 2, 'z', description));
		rec.dataFields.add(f);
		if (!online) return rec;
		MarcRecord h = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		h.id = "1";
		h.dataFields.add(new DataField(1,"852",' ',' ',"‡b serv,remo"));
		rec.holdings.add(h);
		return rec;
	}
	private static void tabulateResults(java.sql.ResultSet rs,
			Map<String,Collection<String>> availableHathiMaterials,
			Collection<String> denyTitles) throws SQLException {
		while (rs.next()) {
			String vol = rs.getString("Volume_Identifier");
			String title = rs.getString("UofM_Record_Number");
			String access = rs.getString("Access");
			if (access.equals("allow")) {
				if ( ! availableHathiMaterials.containsKey(title) )
					availableHathiMaterials.put(title, new HashSet<String>() );
				availableHathiMaterials.get(title).add(vol);
			} else {
				denyTitles.add(title);
			}
		}
	}
}
