package edu.cornell.library.integration.voyager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;

public class DownloadMARC {
	SolrBuildConfig config;
	DavService davService;
	private static Pattern uPlusHexPattern = null;
//	private static Pattern htmlEntityPattern = null;

	public DownloadMARC(SolrBuildConfig config) {
		davService = DavServiceFactory.getDavService(config);
		this.config = config;
		if (uPlusHexPattern == null)
			uPlusHexPattern = Pattern.compile(".*[Uu]\\+\\p{XDigit}{4}.*");
//		if (htmlEntityPattern == null)
//			htmlEntityPattern = Pattern.compile(".*&([a-zA-Z]{2,}|#[0-9]+|#[xX](\\p{XDigit}{2}){1,3});.*");
	}
	/**
	 * Retrieve specified MARC records from Voyager, convert to XML and save.
	 * @param type Record type is necessary for querying Voyager, and is used in file names
	 * @param ids List of identifiers of desired records
	 * @param dir relative directory for output. Will be relative to webdavBaseUrl.
	 * @throws Exception
	 */
	public Set<Integer> saveXml (RecordType type, Set<Integer> ids, String dir) throws Exception {

		Set<Integer> notFoundIds = new HashSet<>();
		int fileSeqNo = 1;
		int recCount = 0;
		StringBuilder recs = new StringBuilder();

		try ( Connection voyager = config.getDatabaseConnection("Voy");
				PreparedStatement pstmt = prepareStatement(voyager,type) ){

			for( Integer id : ids ) {
				String rec = queryVoyager(pstmt,id);
				if (rec != null) {
					recs.append(rec);
					errorChecking(rec,type,id);
					if (++recCount == 10_000) {
						StringBuilder url = new StringBuilder();
						url.append(config.getWebdavBaseUrl()).append('/').append(dir).append('/');
						if (type.equals(RecordType.BIBLIOGRAPHIC))
							url.append("bib.");
						else if (type.equals(RecordType.HOLDINGS))
							url.append("mfhd.");
						else
							url.append("auth.");
						url.append(fileSeqNo).append(".xml");
						writeFile(url.toString(),MarcRecord.marcToXml(recs.toString()));
						recCount = 0;
						fileSeqNo++;
						recs.setLength(0);
					}
				} else
					notFoundIds.add(id);
			}
		}
		if (recCount > 0) {
			StringBuilder url = new StringBuilder();
			url.append(config.getWebdavBaseUrl()).append('/').append(dir).append('/');
			if (type.equals(RecordType.BIBLIOGRAPHIC))
				url.append("bib.");
			else if (type.equals(RecordType.HOLDINGS))
				url.append("mfhd.");
			else
				url.append("auth.");
			url.append(fileSeqNo).append(".xml");
			writeFile(url.toString(),MarcRecord.marcToXml(recs.toString()));
		}
		return notFoundIds;
	}

	/**
	 * Retrieve specified MARC record and return MARC21 format as string.
	 * @param type (RecordType.BIBLIOGRAPHIC, RecordType.HOLDINGS, RecordType.AUTHORITY)
	 * @param id
	 * @return String MARC21 encoded MARC file; null if id not found in Voyager
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public String downloadMrc(RecordType type, Integer id)
			throws SQLException, ClassNotFoundException, IOException, InterruptedException {

		try ( Connection voyager = config.getDatabaseConnection("Voy");
				PreparedStatement pstmt = prepareStatement(voyager,type) ){
			return queryVoyager(pstmt,id);
		}
	}

	/**
	 * Retrieve specified MARC record and return MARC XML format as string.
	 * @param type (RecordType.BIBLIOGRAPHIC, RecordType.HOLDINGS, RecordType.AUTHORITY)
	 * @param id
	 * @return String XML encoded MARC file; null if id not found in Voyager
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public String downloadXml(RecordType type, Integer id)
			throws SQLException, ClassNotFoundException, IOException, InterruptedException {
		return MarcRecord.marcToXml(downloadMrc(type,id));
	}


	private static void errorChecking(String rec, RecordType type, Integer id) {
        if ( rec.contains("\uFFFD") )
        	System.out.println(type.toString().toLowerCase()+
        			" MARC contains Unicode Replacement Character (U+FFFD): "+id);
        if ( uPlusHexPattern.matcher(rec).matches() )
        	System.out.println(type.toString().toLowerCase()+
        			" MARC contains Unicode Character Replacement Sequence (U+XXXX): "+id);
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
	private void writeFile(String filename, String recs) throws Exception {
		try (InputStream is = new ByteArrayInputStream(recs.toString().getBytes(StandardCharsets.UTF_8))) {
			davService.saveFile(filename, is);
		}
	}
	private static String queryVoyager(PreparedStatement pstmt, Integer id)
			throws SQLException, IOException, InterruptedException {
		pstmt.setInt(1, id);
		String marcRecord = null;

		int retryLimit = 4;
		boolean succeeded = false;
		while (retryLimit > 0 && ! succeeded)

			try (   ByteArrayOutputStream bb = new ByteArrayOutputStream();
					ResultSet rs = pstmt.executeQuery()  ) {
				while (rs.next()) bb.write(rs.getBytes("RECORD_SEGMENT"));
				if (bb.size() == 0)
					return null;
				bb.close();
				marcRecord = new String( bb.toByteArray(), StandardCharsets.UTF_8 );
			} catch ( SQLException | IOException e ) {
				System.out.println(e.getClass().getName()+" querying record from Voyager.");
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
