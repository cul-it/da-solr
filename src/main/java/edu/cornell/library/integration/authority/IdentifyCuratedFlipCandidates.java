package edu.cornell.library.integration.authority;

import static edu.cornell.library.integration.authority.Solr.identifySearchFields;
import static edu.cornell.library.integration.authority.Solr.querySolrForMatchingBibs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;

public class IdentifyCuratedFlipCandidates {

	public static void main(String[] args) throws SQLException, IOException, SolrServerException {

		Flip michael = new Flip(
				new DataField(1,"150",' ',' ',"‡a Michael (Archangel)"),
				new DataField(1,"100",'0',' ',"‡a Michael ‡c (Archangel)"),
				"n 2001096929",
				EnumSet.of(AuthoritySource.LC, AuthoritySource.FAST));

		List<String> requiredArgs = Config.getRequiredArgsForDB("Authority");
		requiredArgs.add("blacklightSolrUrl");
		Config config = Config.loadConfig(requiredArgs);


		if ( michael.authorityId != null ) {
			boolean isCurrent = confirmAfterFormIsCurrent(config, michael);
			if ( ! isCurrent ) {
				System.out.println("The flip's 'after' form doesn't match its current authority.");
				System.out.println("TODO: some workflow for addressing outdated flips.");
				System.exit(1);
			}
		}

		List<String> blacklightFields = identifyBlacklightFields( michael );

		String heading = headingOf(michael.before);

		try( HttpSolrClient solr = new HttpSolrClient(config.getBlacklightSolrUrl())) {
			
			Map<String,Object> autoFlip = new HashMap<>();
			for (String field : blacklightFields) {
				List<List<String>> instances = querySolrForMatchingBibs( solr, field, heading );
				if (instances.isEmpty()) continue;
				autoFlip.put(field, instances);
			}
			if (autoFlip.isEmpty()) return;
			autoFlip.put("oldHeading", michael.before);
			autoFlip.put("newHeading", michael.after);
			autoFlip.put("name", "Replace");
			System.out.println(mapper.writeValueAsString(autoFlip));
		}
	}

	private static String headingOf(DataField f) {
		String heading = f.concatenateSpecificSubfields("abcdefghjklmnopqrstu");
		String dashed = f.concatenateSpecificSubfields(" > ", "xvyz");
		if ( ! dashed.isEmpty() ) heading = String.format("%s > %s", heading, dashed);
		return heading;
	}


	private static List<String> identifyBlacklightFields(Flip flip) {

		HeadingType before_ht = HeadingType.byAuthField( flip.before.tag );
		HeadingType after_ht = HeadingType.byAuthField( flip.after.tag );

		boolean includeAuthorFields = authorHeadingTypes.contains(before_ht) && authorHeadingTypes.contains(after_ht);
		List<String> searchFields = identifySearchFields(
				before_ht, AuthoritySource.LC, flip.vocabs.contains(AuthoritySource.FAST));
		if ( ! includeAuthorFields ) searchFields.removeIf(p -> p.contains("author"));
		searchFields.removeIf(p -> p.contains("_unk_"));
		return searchFields;
	}



	private static EnumSet<HeadingType> authorHeadingTypes = EnumSet.of(HeadingType.PERS, HeadingType.CORP, HeadingType.MEETING);

	private static boolean confirmAfterFormIsCurrent(Config config, Flip michael) throws SQLException {
		try ( Connection authority = config.getDatabaseConnection("Authority");
				PreparedStatement mostRecentAuth = authority.prepareStatement(
						"SELECT marc21 FROM authorityUpdate WHERE id = ? ORDER BY moddate DESC LIMIT 1") ){
			mostRecentAuth.setString(1, michael.authorityId);
			try (ResultSet rs = mostRecentAuth.executeQuery()) {
				while (rs.next()) {
					MarcRecord r = new MarcRecord(MarcRecord.RecordType.AUTHORITY, rs.getBytes("marc21"));
					DataField f = getHeadField(r);
					if ( ! f.tag.equals(michael.after.tag) || f.subfields.size() != michael.after.subfields.size()) {
						System.out.printf("Heading has changed from %s to %s.\n",michael.after.toString(), f.toString());
						return false;
					}
					List<Subfield> after = new ArrayList<>(michael.after.subfields);
					List<Subfield> current = new ArrayList<>(f.subfields);
					for (int i = 0; i < after.size(); i++) {
						Subfield a = after.get(i);
						Subfield b = current.get(i);
						if (a.code.equals(b.code) && a.value.equals(b.value)) continue;
						System.out.printf("Heading has changed from %s to %s.\n",michael.after.toString(), f.toString());
						return false;
					}
				}
			}
		}
		return true;
	}

	private static DataField getHeadField(MarcRecord r) {
		for (DataField f : r.dataFields)
			if (f.tag.startsWith("1"))
				return f;
		return null;
	}

	private static class Flip {
		final DataField before;
		final DataField after;
		final String authorityId;
		final EnumSet<AuthoritySource> vocabs;
//		Flip( DataField before, DataField after, String authorityId) {
//			this.before = before;
//			this.after = after;
//			this.authorityId = authorityId;
//			this.vocabs =EnumSet.of(AuthoritySource.LC);
//		}
		Flip( DataField before, DataField after, String authorityId, EnumSet<AuthoritySource> vocabs) {
			this.before = before;
			this.after = after;
			this.authorityId = authorityId;
			this.vocabs = vocabs;
		}
	}

	static ObjectMapper mapper = new ObjectMapper();
	static { mapper.enable(SerializationFeature.INDENT_OUTPUT); }

}
