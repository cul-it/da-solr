package edu.cornell.library.integration.authority;

import static edu.cornell.library.integration.authority.Solr.identifySearchFields;
import static edu.cornell.library.integration.authority.Solr.querySolrForMatchingBibs;
import static edu.cornell.library.integration.utilities.Excel.readExcel;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;

public class IdentifyCuratedFlipCandidates {

	
	public static void main(String[] args) throws SQLException, IOException, SolrServerException {

		List<String> requiredArgs = Config.getRequiredArgsForDB("Authority");
		requiredArgs.add("blacklightSolrUrl");
		Config config = Config.loadConfig(requiredArgs);


		List<Map<String,String>> data = readExcel("C:\\Users\\fbw4\\Documents\\authUpdates\\names-that-may-be-fictitious_full (1).xlsx");
		List<String> errors = new ArrayList<>();
		for( Map<String,String> row: data) {
			try {
				DataField before = parseDataField(row.get("Heading - BEFORE"), row.get("LC Authority - BEFORE"));
				DataField after = parseDataField(row.get("Heading - AFTER"), row.get("LC Authority - AFTER"));
				String lcIdBefore = row.containsKey("LC Authority - BEFORE")?row.get("LC Authority - BEFORE")
						:row.containsKey("LC Authority")?row.get("LC Authority"):null;
				String lcIdAfter = row.containsKey("LC Authority - AFTER")?row.get("LC Authority - AFTER")
						:row.containsKey("LC Authority")?row.get("LC Authority"):null;
				Flip flip = new Flip(before, after, lcIdBefore, lcIdAfter, getVocabsToFlip(after));
				boolean isCurrent = confirmAfterFormIsCurrent(config, flip);
				if ( ! isCurrent ) {
					errors.add("AFTER NOT CURRENT ERROR: "+ row.get("Heading - AFTER"));
					continue;
				}
			} catch (UnsupportedEncodingException e) {
				errors.add(e.getMessage());
			}
		}
		for (String error : errors)
			System.out.println(error);

		Flip michael = new Flip(
				new DataField(1,"150",' ',' ',"‡a Michael (Archangel)"),
				new DataField(1,"100",'0',' ',"‡a Michael ‡c (Archangel)"),
				"sh 85061506",
				"n 2001096929",
				EnumSet.of(AuthoritySource.LC, AuthoritySource.FAST));



		if ( michael.authorityIdAfter != null ) {
			boolean isCurrent = confirmAfterFormIsCurrent(config, michael);
			if ( ! isCurrent ) {
				System.out.println("The flip's 'after' form doesn't match its current authority.");
				System.out.println("TODO: some workflow for addressing outdated flips.");
				System.exit(1);
			}
		}

//		Map<String,Object> autoFlip = buildAutoFlip(config, michael);
//		if (autoFlip != null)
//			System.out.println(mapper.writeValueAsString(autoFlip));
	}

	private static EnumSet<AuthoritySource> getVocabsToFlip(DataField f) {
		boolean isName = (f.tag.equals("100") || f.tag.equals("110") || f.tag.equals("111"));
		if (!isName ) return EnumSet.of(AuthoritySource.LC);
		for (Subfield sf : f.subfields)
			if (sf.code.equals('v') || sf.code.equals('x') || sf.code.equals('y') || sf.code.equals('z'))
				return EnumSet.of(AuthoritySource.LC);
		return EnumSet.of(AuthoritySource.LC, AuthoritySource.FAST);
	}

	private static DataField parseDataField( String s, String lcId ) throws UnsupportedEncodingException {
		String[] parts = s.split("[$‡ǂ]");
		TreeSet<Subfield> subfields = new TreeSet<>();
		for (int i = 1; i<parts.length; i++)
			subfields.add(new Subfield(i,parts[i].charAt(0),parts[i].substring(1).trim()));
		String prefix = parts[0];
		if (prefix.length() > 8) throw new UnsupportedEncodingException(String.format("PARSE ERROR: %s", s));
		String tag = "1"+prefix.substring(1, 3);
		HeadingType ht = HeadingType.byAuthField(tag);
		if (ht == null) throw new UnsupportedEncodingException(String.format("PARSE ERROR: %s", s));
		if ( lcId == null ) return new DataField(1, tag,' ',' ',subfields);

		boolean isName = ht.equals(HeadingType.PERS) || ht.equals(HeadingType.CORP) || ht.equals(HeadingType.MEETING);
		if (lcId.startsWith("n")) {
			if (!isName) throw new UnsupportedEncodingException(String.format("VOCABULARY ERROR (%s): %s", lcId, s));
		} else {
			if (isName) throw new UnsupportedEncodingException(String.format("VOCABULARY ERROR (%s): %s", lcId, s));
		}
		return new DataField(1, tag,' ',' ',subfields);
	}


	private static Map<String,Object> buildAutoFlip(Config config, Flip flip) throws IOException, SolrServerException {
		List<String> blacklightFields = identifyBlacklightFields( flip );

		String heading = headingOf(flip.before);

		try( HttpSolrClient solr = new HttpSolrClient(config.getBlacklightSolrUrl())) {
			
			Map<String,Object> autoFlip = new HashMap<>();
			for (String field : blacklightFields) {
				List<List<String>> instances = querySolrForMatchingBibs( solr, field, heading );
				if (instances.isEmpty()) continue;
				autoFlip.put(field, instances);
			}
			if (autoFlip.isEmpty()) return null;
			autoFlip.put("oldHeading", flip.before);
			autoFlip.put("newHeading", flip.after);
			autoFlip.put("name", "Replace");
			return autoFlip;
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

	private static boolean confirmAfterFormIsCurrent(Config config, Flip flip) throws SQLException, UnsupportedEncodingException {
		try ( Connection authority = config.getDatabaseConnection("Authority");
				PreparedStatement mostRecentAuth = authority.prepareStatement(
						"SELECT marc21 FROM authorityUpdate WHERE id = ? ORDER BY moddate DESC LIMIT 1") ){
			mostRecentAuth.setString(1, flip.authorityIdAfter);
			try (ResultSet rs = mostRecentAuth.executeQuery()) {
				while (rs.next()) {
					MarcRecord r = new MarcRecord(MarcRecord.RecordType.AUTHORITY, rs.getBytes("marc21"));
					DataField f = getHeadField(r);
					flip.after.ind1 = f.ind1;
					flip.after.ind2 = f.ind2;
					if ( ! f.tag.equals(flip.after.tag) || f.subfields.size() != flip.after.subfields.size())
						throw new UnsupportedEncodingException(String.format(
								"NOT CURRENT ERROR: AFTER: %s CURRENT: %s", flip.after.toString(), f.toString()));
					List<Subfield> after = new ArrayList<>(flip.after.subfields);
					List<Subfield> current = new ArrayList<>(f.subfields);
					for (int i = 0; i < after.size(); i++) {
						Subfield a = after.get(i);
						a.value = Normalizer.normalize(a.value,Normalizer.Form.NFC);
						Subfield b = current.get(i);
						if (a.code.equals(b.code) && a.value.equals(b.value)) continue;
						throw new UnsupportedEncodingException(String.format(
								"NOT CURRENT ERROR: AFTER: %s CURRENT: %s", flip.after.toString(), f.toString()));
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
		final String authorityIdBefore;
		final String authorityIdAfter;
		final EnumSet<AuthoritySource> vocabs;
//		Flip( DataField before, DataField after, String authorityId) {
//			this.before = before;
//			this.after = after;
//			this.authorityId = authorityId;
//			this.vocabs =EnumSet.of(AuthoritySource.LC);
//		}
		Flip( DataField before, DataField after, String authorityIdBefore, String authorityIdAfter, EnumSet<AuthoritySource> vocabs) {
			this.before = before;
			this.after = after;
			this.authorityIdAfter = authorityIdAfter;
			this.authorityIdBefore = authorityIdBefore;
			this.vocabs = vocabs;
		}
	}

	static ObjectMapper mapper = new ObjectMapper();

}
