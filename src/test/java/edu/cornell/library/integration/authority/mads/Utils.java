package edu.cornell.library.integration.authority.mads;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.xml.stream.XMLStreamException;

import edu.cornell.library.integration.authority.IndexAuthorityRecords;
import edu.cornell.library.integration.authority.IndexAuthorityRecords.AuthorityData;
import edu.cornell.library.integration.marc.MarcRecord;

public class Utils {
	public static AuthorityData getData(Path filePath) throws IOException, XMLStreamException {
		String content = Files.readString(filePath);
		var rec = new MarcRecord(MarcRecord.RecordType.AUTHORITY, content, true);
		return IndexAuthorityRecords.parseMarcRecord(rec);
	}
}
