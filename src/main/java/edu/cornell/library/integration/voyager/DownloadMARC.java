package edu.cornell.library.integration.voyager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.marc4j.MarcException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.Record;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.MarcRecord.RecordType;
import edu.cornell.library.integration.util.ConvertUtils;

public class DownloadMARC {
	SolrBuildConfig config;
	Connection voyager;
	PreparedStatement pstmt;
	DavService davService;
	private static Pattern uPlusHexPattern = null;

	public DownloadMARC(SolrBuildConfig config) {
		davService = DavServiceFactory.getDavService(config);
		this.config = config;
		if (uPlusHexPattern == null)
			uPlusHexPattern = Pattern.compile(".*[Uu]\\+\\p{XDigit}{4}.*");
	}
	/**
	 * Retrieve specified MARC records from Voyager, convert to XML and save.
	 * @param type Record type is necessary for querying Voyager, and is used in file names
	 * @param ids List of identifiers of desired records
	 * @param dir relative directory for output. Will be relative to webdavBaseUrl.
	 * @throws Exception
	 */
	public Set<Integer> saveXml (RecordType type, Set<Integer> ids, String dir) throws Exception {
		voyager = config.getDatabaseConnection("Voy");
		Set<Integer> notFoundIds = new HashSet<Integer>();
		prepareStatement(type);
		int fileSeqNo = 1;
		int recCount = 0;
		StringBuilder recs = new StringBuilder();
		for( Integer id : ids ) {
			String rec = queryVoyager(type,id);
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
					writeFile(url.toString(),marcToXml(recs.toString()));
					recCount = 0;
					fileSeqNo++;
					recs.setLength(0);
				}
			} else
				notFoundIds.add(id);
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
			writeFile(url.toString(),marcToXml(recs.toString()));
		}
		pstmt.close();
		voyager.close();
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
	 */
	public String downloadMrc(RecordType type, Integer id)
			throws SQLException, ClassNotFoundException, IOException {
		voyager = config.getDatabaseConnection("Voy");
		prepareStatement(type);
		String rec = queryVoyager(type,id);
		pstmt.close();
		voyager.close();
		return rec;
	}

	/**
	 * Retrieve specified MARC record and return MARC XML format as string.
	 * @param type (RecordType.BIBLIOGRAPHIC, RecordType.HOLDINGS, RecordType.AUTHORITY)
	 * @param id
	 * @return String XML encoded MARC file; null if id not found in Voyager
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException 
	 */
	public String downloadXml(RecordType type, Integer id)
			throws SQLException, ClassNotFoundException, IOException {
		voyager = config.getDatabaseConnection("Voy");
		prepareStatement(type);
		String rec = queryVoyager(type,id);
		pstmt.close();
		voyager.close();
		return marcToXml(rec);
	}


	private void errorChecking(String rec, RecordType type, Integer id) {
        if ( rec.contains("\uFFFD") )
        	System.out.println(type.toString()+" MARC contains Unicode Replacement Character (U+FFFD): "+id);
        if ( uPlusHexPattern.matcher(rec).matches() )
        	System.out.println(type.toString()+" MARC contains Unicode Character Replacement Sequence (U+XXXX): "+id);
	}
	private String marcToXml( String marc21 ) {
		InputStream is = new ByteArrayInputStream(marc21.getBytes(StandardCharsets.UTF_8));
		OutputStream out = new ByteArrayOutputStream();
		MarcPermissiveStreamReader reader = new MarcPermissiveStreamReader(is,true,true);
		MarcXmlWriter writer = new MarcXmlWriter(out, "UTF8", true);
		writer.setUnicodeNormalization(true);
		Record record = null;
        while (reader.hasNext()) {
            try {
            	record = reader.next();
            } catch (MarcException me) {
            	me.printStackTrace();
            	continue;
            } catch (Exception e) {
            	e.printStackTrace();
            	continue;
            }
            boolean hasInvalidChars = ConvertUtils.dealWithBadCharacters(record);
            if (! hasInvalidChars)
            	writer.write(record);
        }
        writer.close();
        return out.toString();
	}
	private void prepareStatement(RecordType type) throws SQLException {
		if (type.equals(RecordType.BIBLIOGRAPHIC))
			pstmt = voyager.prepareStatement(
					"SELECT * FROM BIB_DATA WHERE BIB_DATA.BIB_ID = ? ORDER BY BIB_DATA.SEQNUM");
		else if (type.equals(RecordType.HOLDINGS))
			pstmt = voyager.prepareStatement(
					"SELECT * FROM MFHD_DATA WHERE MFHD_DATA.MFHD_ID = ? ORDER BY MFHD_DATA.SEQNUM");
		else
			pstmt = voyager.prepareStatement(
					"SELECT * FROM AUTH_DATA WHERE AUTH_DATA.AUTH_ID = ? ORDER BY AUTH_DATA.SEQNUM");
	}
	private void writeFile(String filename, String recs) throws Exception {
		InputStream is = new ByteArrayInputStream(recs.toString().getBytes(StandardCharsets.UTF_8));
		davService.saveFile(filename, is);
	}
	private String queryVoyager(RecordType type, Integer id)
			throws SQLException, ClassNotFoundException, IOException {
		pstmt.setInt(1, id);
    	ByteArrayOutputStream bb = new ByteArrayOutputStream();
    	ResultSet rs = pstmt.executeQuery();
    	while (rs.next()) bb.write(rs.getBytes("RECORD_SEGMENT"));
    	rs.close();
    	if (bb.size() == 0)
    		return null;
    	String marcRecord = new String( bb.toByteArray(), StandardCharsets.UTF_8 );
    	return marcRecord;
	}
}
