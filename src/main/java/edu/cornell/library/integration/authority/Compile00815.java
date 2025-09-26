package edu.cornell.library.integration.authority;


import static edu.cornell.library.integration.processing.IndexAuthorityRecords.getAllIdentifiers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;

public class Compile00815 {

	public static void main(String[] args)
			throws FileNotFoundException, IOException, SQLException {

		Collection<String> requiredArgs =  Config.getRequiredArgsForDB("Authority");
		Config config = Config.loadConfig(requiredArgs);

		try ( Connection authority = config.getDatabaseConnection("Authority") ){
			Set<String> identifiers = getAllIdentifiers(authority);

			for (String identifier : identifiers) {
				String heading = null;
				MarcRecord rec = null;
				String recSource = null;
				try ( PreparedStatement getAuthStmt = authority.prepareStatement(
						"SELECT marc21, heading, updateFile FROM authorityUpdate WHERE id = ? ORDER BY moddate DESC LIMIT 1")) {
					getAuthStmt.setString(1, identifier);
					try (ResultSet rs = getAuthStmt.executeQuery()) {
						while (rs.next()) {
							rec = new MarcRecord(MarcRecord.RecordType.AUTHORITY,rs.getBytes("marc21"));
							heading = rs.getString("heading");
							recSource = rs.getString("updateFile");
						}
					}
					
				}
				if (heading == null)
					try ( PreparedStatement getAuthStmt = authority.prepareStatement(
							"SELECT marcxml FROM voyagerAuthority WHERE id = ? ORDER BY moddate DESC LIMIT 1")) {
						getAuthStmt.setString(1, identifier);
						try (ResultSet rs = getAuthStmt.executeQuery()) {
							while (rs.next()) {
								rec = new MarcRecord(MarcRecord.RecordType.AUTHORITY,rs.getString("marcxml"),false);
								heading = mainHeading(rec);
								recSource = "voyager";
							}
						} catch (IllegalArgumentException e) {
							System.out.format("ERROR: IllegalArgumentException %s (%s)\n", e.getMessage(), identifier);
						} catch (XMLStreamException e) {
							System.out.format("ERROR: XML Error %s (%s)\n",e.getMessage(),identifier);
						}
						
					}

				if (rec == null) continue;

				Character recordStatus = rec.leader.charAt(5);
				if ( recordStatus.equals('d') || recordStatus.equals('o')) continue;

				Character eightFifteen = null;
				for (ControlField f : rec.controlFields) if (f.tag.equals("008")) eightFifteen = f.value.charAt(15);

				System.out.format("%s\t%s\t%s\t%s\n", identifier, recSource, heading, eightFifteen);
			}

		}
	}

	private static String mainHeading( MarcRecord rec ) {
		for (DataField f : rec.dataFields) if (f.tag.startsWith("1")) {
			String main = f.concatenateSpecificSubfields("abcdefghijklmnopqrstu");
			String dashedTerms = f.concatenateSpecificSubfields(" > ", "vxyz");
			if ( ! main.isEmpty() && ! dashedTerms.isEmpty() )
				main += " > "+dashedTerms;
			return main;
		}
		return null;
	}

}
