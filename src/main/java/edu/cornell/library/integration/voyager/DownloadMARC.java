package edu.cornell.library.integration.voyager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.utilities.Config;

public class DownloadMARC {
	private Config config;
	private static Pattern uPlusHexPattern =  Pattern.compile(".*[Uu]\\+\\p{XDigit}{4}.*");
	private static Pattern copyrightNullPattern = Pattern.compile(".*©Ø.*");
//	private static Pattern htmlEntityPattern = null;

	public DownloadMARC(Config config) {
		this.config = config;
	}

	/**
	 * Retrieve specified MARC record and return MARC21 format as string.
	 * @param type (RecordType.BIBLIOGRAPHIC, RecordType.HOLDINGS, RecordType.AUTHORITY)
	 * @param id
	 * @return String MARC21 encoded MARC file; null if id not found in Voyager
	 * @throws SQLException
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public byte[] downloadMrc(RecordType type, Integer id)
			throws SQLException, IOException, InterruptedException {

		return queryVoyager(type,id);
	}

	public MarcRecord getMarc( RecordType type, Integer id ) throws SQLException, IOException, InterruptedException {
		return new MarcRecord(type,queryVoyager(type,id));
	}

	public List<MarcRecord> retrieveRecordsByIdRange (RecordType type, Integer from, Integer to)
			throws SQLException, IOException {
		List<MarcRecord> recs = new ArrayList<>();

		Map<Integer,ByteArrayOutputStream> bytes = new HashMap<>();
		try ( Connection voyager = this.config.getDatabaseConnection("Voy");
				PreparedStatement pstmt = prepareRangeStatement(voyager, type)) {
			pstmt.setInt(1, from);
			pstmt.setInt(2, to);
			try (ResultSet rs = pstmt.executeQuery()) {
				while ( rs.next() ) {
					int id = rs.getInt(1);
					if ( ! bytes.containsKey(id) )
						bytes.put(id, new ByteArrayOutputStream());
					bytes.get(id).write(rs.getBytes(2));
				}
			}
			for ( ByteArrayOutputStream recordBytes : bytes.values() ) {
				recs.add(new MarcRecord(type,recordBytes.toByteArray()));
			}
		}
		return recs;
	}



	private static void errorChecking(String rec, RecordType type, Integer id) {
		if ( rec.contains("\uFFFD") )
			System.out.println(type.toString().toLowerCase()+
					" MARC contains Unicode Replacement Character (U+FFFD): "+id);
		if ( uPlusHexPattern.matcher(rec).matches() )
			System.out.println(type.toString().toLowerCase()+
					" MARC contains Unicode Character Replacement Sequence (U+XXXX): "+id);
		if ( copyrightNullPattern.matcher(rec).matches() )
			System.out.println(type.toString().toLowerCase()+
					" MARC contains corrupt Unicode sequence (©Ø): "+id);
//		if ( htmlEntityPattern.matcher(rec).matches() )
//			System.out.println(type.toString().toLowerCase()+" MARC contains at least one HTML entity: "+id);
	}

	private static PreparedStatement prepareStatement(Connection voyager , RecordType type) throws SQLException {
		if (type.equals(RecordType.BIBLIOGRAPHIC))
			return voyager.prepareStatement(
					"SELECT * FROM BIB_DATA WHERE BIB_DATA.BIB_ID = ? ORDER BY BIB_DATA.SEQNUM");
		else if (type.equals(RecordType.HOLDINGS))
			return voyager.prepareStatement(
					"SELECT * FROM MFHD_DATA WHERE MFHD_DATA.MFHD_ID = ? ORDER BY MFHD_DATA.SEQNUM");
		else
			return voyager.prepareStatement(
					"SELECT * FROM AUTH_DATA WHERE AUTH_DATA.AUTH_ID = ? ORDER BY AUTH_DATA.SEQNUM");
	}

	private static PreparedStatement prepareRangeStatement(Connection voyager , RecordType type) throws SQLException {
		if (type.equals(RecordType.BIBLIOGRAPHIC))
			return voyager.prepareStatement(
					"SELECT BIB_ID, RECORD_SEGMENT FROM BIB_DATA WHERE BIB_ID BETWEEN ? AND ? ORDER BY BIB_ID, SEQNUM");
		else if (type.equals(RecordType.HOLDINGS))
			return voyager.prepareStatement(
					"SELECT MFHD_ID, RECORD_SEGMENT FROM MFHD_DATA WHERE MFHD_ID BETWEEN ? AND ? ORDER BY MFHD_ID, SEQNUM");
		else
			return voyager.prepareStatement(
					"SELECT AUTH_ID, RECORD_SEGMENT FROM AUTH_DATA WHERE AUTH_ID BETWEEN ? AND ? ORDER BY AUTH_ID, SEQNUM");
	}


	private byte[] queryVoyager(RecordType type, Integer id)
			throws SQLException, IOException, InterruptedException {
		byte[] marcRecord = null;

		int retryLimit = 4;
		boolean succeeded = false;
		while (retryLimit > 0 && ! succeeded)

			try (
					Connection voyager = this.config.getDatabaseConnection("Voy");
					PreparedStatement pstmt = prepareStatement(voyager,type); ) {

				pstmt.setInt(1, id);
				try ( 
						ResultSet rs = pstmt.executeQuery();
						ByteArrayOutputStream bb = new ByteArrayOutputStream();) {

					while (rs.next()) bb.write(rs.getBytes("RECORD_SEGMENT"));
					if (bb.size() == 0)
						return null;
					bb.close();
					marcRecord = bb.toByteArray();
					succeeded = true;
				}
			} catch ( SQLException | IOException e ) {
				System.out.println(e.getClass().getName()+" querying record from Voyager.");
				e.printStackTrace();
				if (retryLimit-- > 0) {
					System.out.println("Will retry in 20 seconds.");
					Thread.sleep(20_000);
				} else {
					System.out.println("Retry limit reached. Failing.");
					throw e;
				}
			}
    	return marcRecord;
	}
}
