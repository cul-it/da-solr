package edu.cornell.library.integration.authority;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.converter.impl.AnselToUnicode;
import org.marc4j.marc.Record;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class LCAuthorityUpdateFile {

	public static Map<MarcRecord,String> readFile(Path inputPath) throws IOException {

		byte[] bytes = Files.readAllBytes(inputPath);

		Map<MarcRecord,String> records = new LinkedHashMap<>();
		while ( bytes != null ) {

			// Extract the first record, convert to utf8
			byte[] recordArray;
			try {
				recordArray = getTopRecordFromArray( bytes );
			} catch (NumberFormatException e) {
				e.printStackTrace();
				System.out.printf("Error parsing %s. Skipping the rest.\n", inputPath);
				return records;
			}
			bytes = removeTopRecordFromArray( bytes, recordArray.length );
			byte[] recordArrayUTF8 = convertMarc8RecordToUtf8( recordArray );
			MarcRecord r = new MarcRecord(MarcRecord.RecordType.AUTHORITY,recordArrayUTF8);

			records.put(r,new String( recordArrayUTF8, StandardCharsets.UTF_8));

		}
		return records;
	}

	public static void pushRecordsToDatabase(
			Connection authorityDB, Map<MarcRecord,String> records, String inputFile) throws SQLException {

		int count = 0;
		try (PreparedStatement insertStmt = authorityDB.prepareStatement(
				"INSERT INTO authorityUpdate"+
				"       (id,vocabulary,updateFile,positionInFile,changeType,"+
				"        heading,headingType,linkedSubdivision,undifferentiated,"+
				"        marc21,human,moddate) "+
				"VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")) {
			insertStmt.setString(3, inputFile);
			for (MarcRecord r : records.keySet()) {

				// Extract data points needed for database
				String                    id                = extractId(r);
				AuthoritySource           vocab             = extractVocab(id);
				ChangeType                changeType        = extractChangeType(r);
				Entry<String,HeadingType> heading           = extractHeading(r);
				String                    linkedSubdivision = extractLinkedSubdivision(r);
				boolean                   undifferentiated  = extractUndifferentiated(r);
				Date                      moddate           = extractModdate(r);
				int                       positionInFile    = ++count;

				// Build insert
				insertStmt.setString(1, id);
				insertStmt.setInt(2, vocab.ordinal());
				insertStmt.setInt(4, positionInFile);
				insertStmt.setInt(5, changeType.ordinal());
				insertStmt.setString(6, (heading == null)?null:heading.getKey());
				insertStmt.setInt(7, (heading == null)?-1:heading.getValue().ordinal());
				insertStmt.setString(8, linkedSubdivision);
				insertStmt.setBoolean(9, undifferentiated);
				insertStmt.setString(10, records.get(r));
				insertStmt.setString(11, r.toString());
				insertStmt.setDate(12, moddate);
				insertStmt.addBatch();

				if ( 0 == count % 100 ) insertStmt.executeBatch();
			}
			if ( 0 != count % 100 ) insertStmt.executeBatch();
		}
	}


	private static String extractLinkedSubdivision(MarcRecord r) {
		for (DataField f : r.dataFields)
			if (linkedSubdivisionFields.contains(f.tag))
				return f.concatenateSpecificSubfields(" > ", "avxyz");
		return null;
	}
	private static List<String> linkedSubdivisionFields = Arrays.asList("780","781","782","785");

	private static Date extractModdate(MarcRecord r) {
		for (ControlField f : r.controlFields) if (f.tag.equals("005")) {
			Matcher m = dateMatcher.matcher(f.value);
			if ( m.matches())
				return Date.valueOf(String.format("%s-%s-%s",m.group(1),m.group(2),m.group(3)));
		}
		return null;
	}
	static Pattern dateMatcher = Pattern.compile("(\\d{4})(\\d{2})(\\d{2}).*");

	private static boolean extractUndifferentiated(MarcRecord r) {
		for (ControlField f : r.controlFields) if (f.tag.equals("008"))
			if (f.value.length() >= 32) return f.value.charAt(32) == 'b';
		return false;
	}

	private static AuthoritySource extractVocab(String id) {
		if (id.startsWith("gf")) return AuthoritySource.LCGFT;
		if (id.startsWith("sj")) return AuthoritySource.LCJSH;
		if (id.startsWith("sh")) return AuthoritySource.LCSH;
		if (id.startsWith("n"))  return AuthoritySource.NAF;
		return AuthoritySource.UNK;
	}

	private static Entry<String, HeadingType> extractHeading( MarcRecord r ) {
		String heading = null;
		for ( DataField f : r.dataFields ) if ( f.tag.startsWith("1") ) {

			if (f.tag.startsWith("18"))
				heading = f.concatenateSpecificSubfields(" > ", "vxyz");
			else {
				heading = f.concatenateSpecificSubfields("abcdefghjklmnopqrstu");
				String dashed_terms = f.concatenateSpecificSubfields(" > ", "vxyz");
				if ( ! heading.isEmpty() && ! dashed_terms.isEmpty() )
					heading += " > "+dashed_terms;
			}

			HeadingType ht = HeadingType.byAuthField(f.tag);

			return new AbstractMap.SimpleEntry<>(heading,ht);
		}
		return null;
	}

	private static String extractId(MarcRecord r) {
		for (ControlField f : r.controlFields)
			if ( f.tag.equals("001") ) return f.value.trim();
		return null;
	}

	private static ChangeType extractChangeType(MarcRecord r) {
		ChangeType changeType = null;
		switch ( r.leader.substring(5, 6)) {
		case "c" :  changeType = ChangeType.UPDATE; break;
		case "d" :  changeType = ChangeType.DELETE; break;
		case "n" :  changeType = ChangeType.NEW;    break;
		default: System.out.println(r.toString()); System.exit(0);
		}
		return changeType;
	}

	private static byte[] removeTopRecordFromArray(byte[] bytes, int recordLength) {
		if (bytes.length > recordLength)
			return Arrays.copyOfRange(bytes, recordLength, bytes.length);
		return null;
	}

	private static byte[] getTopRecordFromArray(byte[] bytes) {
		int recordLength = Integer.valueOf( new String (
				Arrays.copyOfRange(bytes, 0, 5), StandardCharsets.UTF_8 ) );
		return Arrays.copyOfRange(bytes, 0, recordLength);
	}

	private static byte[] convertMarc8RecordToUtf8(byte[] recordArray) {
		MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(recordArray));
		OutputStream output = new ByteArrayOutputStream();
		MarcWriter writer = new MarcStreamWriter(output, "UTF8"); 
		AnselToUnicode converter = new AnselToUnicode();
		writer.setConverter(converter);
		while (reader.hasNext()) {
			Record record = reader.next();
			writer.write(record);
		}
		writer.close();
		return output.toString().getBytes();
	}

}
