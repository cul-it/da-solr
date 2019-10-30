package edu.cornell.library.integration.voyager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

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
	 * Retrieve specified MARC records from Voyager, convert to XML and save.
	 * @param type Record type is necessary for querying Voyager, and is used in file names
	 * @param ids List of identifiers of desired records
	 * @param dir absolute directory for output.
	 * @return Set of identifiers not saved
	 * @throws Exception
	 */
	public Set<Integer> saveXml (RecordType type, Set<Integer> ids, String dir) throws Exception {

		Set<Integer> notFoundIds = new HashSet<>();
		int fileSeqNo = 1;
		int recCount = 0;
		StringBuilder recs = new StringBuilder();

		for( Integer id : ids ) {
			String rec = queryVoyager(type,id);
			if (rec != null) {
				recs.append(rec);
				errorChecking(rec,type,id);
				if (++recCount == 1_000) {
					writeFile(getFilePath(dir,type,fileSeqNo),MarcRecord.marcToXml(recs.toString()));
					recCount = 0;
					fileSeqNo++;
					recs.setLength(0);
				}
			} else
				notFoundIds.add(id);
		}
		if (recCount > 0)
			writeFile(getFilePath(dir,type,fileSeqNo),MarcRecord.marcToXml(recs.toString()));
		return notFoundIds;
	}

	private static String getFilePath( String dir, RecordType type, int fileSeqNo ) {
		return String.format("%s/%s%s.xml", dir,
				(type.equals(RecordType.BIBLIOGRAPHIC)?"bib.":
					(type.equals(RecordType.HOLDINGS))?"mfhd.":"auth."),
				fileSeqNo);
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
	public String downloadMrc(RecordType type, Integer id)
			throws SQLException, IOException, InterruptedException {

		return queryVoyager(type,id);
	}

	/**
	 * Retrieve specified MARC record and return MARC XML format as string.
	 * @param type (RecordType.BIBLIOGRAPHIC, RecordType.HOLDINGS, RecordType.AUTHORITY)
	 * @param id
	 * @return String XML encoded MARC file; null if id not found in Voyager
	 * @throws SQLException
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public String downloadXml(RecordType type, Integer id)
			throws SQLException, IOException, InterruptedException {
		return MarcRecord.marcToXml(downloadMrc(type,id));
	}

	public List<MarcRecord> retrieveRecordsByIdRange (RecordType type, Integer from, Integer to)
			throws SQLException, IOException, XMLStreamException {
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
				recs.add(new MarcRecord(type,new String( recordBytes.toByteArray(), StandardCharsets.UTF_8 )));
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
//        if ( htmlEntityPattern.matcher(rec).matches() )
//        	System.out.println(type.toString().toLowerCase()+" MARC contains at least one HTML entity: "+id);
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


	private static void writeFile(String filename, String recs) throws IOException {
		Files.write(Paths.get(filename), recs.getBytes(),StandardOpenOption.CREATE_NEW);
	}

	private String queryVoyager(RecordType type, Integer id)
			throws SQLException, IOException, InterruptedException {
		String marcRecord = null;

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
					marcRecord = new String( bb.toByteArray(), StandardCharsets.UTF_8 );
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
