package edu.cornell.library.integration.processing;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource;
import edu.cornell.library.integration.metadata.support.HeadingType;
import edu.cornell.library.integration.utilities.CSVReader;
import edu.cornell.library.integration.utilities.Config;

public class MatchHeadingsCSVToAuthority {
	private static HeadingType[] headingTypes = HeadingType.values();
	private static AuthoritySource[] sources = AuthoritySource.values();
	private static String headingQuery = 
			"SELECT authority.nativeId AS identifier,"+
			"       authority.source, authority.nativeHeading, heading_type, authority2heading.main_entry"+
			"  FROM heading, authority2heading, authority"+
			" WHERE sort LIKE ? AND sort REGEXP ?"+
			"   AND heading.id = authority2heading.heading_id"+
			"   AND authority2heading.authority_id = authority.id"+
			"   AND authority.undifferentiated = 0";
	private static String referenceTypeQuery = 
			"SELECT ref_type"+
			"  FROM authority, heading `from`, heading `to`, authority2reference, reference"+
			" WHERE authority.source = ?"+
			"   AND authority.nativeId = ?"+
			"   AND authority.undifferentiated = 0"+
			"   AND authority.id = authority2reference.authority_id"+
			"   AND authority2reference.reference_id = reference.id"+
			"   AND reference.from_heading = from.id"+
			"   AND from.sort = ?"+
			"   AND reference.to_heading = to.id"+
			"   AND to.sort = ?";

	public static void main(String[] args) throws IOException, SQLException {

		Config config = Config.loadConfig(Config.getRequiredArgsForDB("Headings"));
		String inputFile = System.getenv("CSV_FILE");
		CSVReader reader = new CSVReader( System.getenv("CSV_FILE") );
		try (
		PrintWriter matchesFile = new PrintWriter(
				inputFile.substring(0, inputFile.lastIndexOf('.'))+"-matches.tdf", "UTF-8");
		PrintWriter potentialsFile = new PrintWriter(
				inputFile.substring(0, inputFile.lastIndexOf('.'))+"-potentials.tdf", "UTF-8");
		PrintWriter nonmatchesFile = new PrintWriter(
				inputFile.substring(0, inputFile.lastIndexOf('.'))+"-nonmatches.tdf", "UTF-8")){
		Map<String,String> line;
		int matched = 0, potentiallyMatched = 0, unmatched = 0;
		while ((line = reader.readLine()) != null) {
			ASpaceTerm term = new ASpaceTerm();
			for (String column : line.keySet())
				switch (column) {
				case "display name":
				case "title":  term.title  = (line.get(column).startsWith(">"))
						?line.get(column).substring(1):line.get(column); break;
				case "primary name":
				case "term 1": term.term1  = line.get(column); break;
				case "term 2": term.term2  = line.get(column); break;
				case "term 3": term.term3  = line.get(column); break;
				case "term 4": term.term4  = line.get(column); break;
				case "source": term.source = line.get(column); break;
				case "aspace_id": term.id  = line.get(column); break;
				case "rest of name": term.term2 = line.get(column); break;
				case "dates":
					if (term.term2.isEmpty()) term.term2 = line.get(column);
					else                      term.term3 = line.get(column);
					break;
				}
			if (term.title.equals("???")) continue;
			if ( term.term2.isEmpty() && (term.title.contains("--") || term.title.contains(">")) ) {
				String[] parts = term.title.split("\\s*(--|>)\\s*");
				switch (parts.length) {
				case 6: case 5:
				case 4: term.term4 = parts[3];
				case 3: term.term3 = parts[2];
				case 2: term.term2 = parts[1];
				case 1: term.term1 = parts[0];
				}
			}
			List<Match> matches = new ArrayList<>();
			List<Match> potentials = new ArrayList<>();
			getMatches(config, matches, potentials, getFilingForm(term.title));
			getMatches(config, matches, potentials, getFilingForm(term.title)+"s");
			Match m = tryToIdentifyUnambiguousMatch( matches );
			if ( m != null ) {
				writeToMatchesFile(matchesFile,term,m);
				matched++;
				continue;
			}
			String placeNameNormalized = normalizePlaceNames( getFilingForm(term.term1) );
			if ( placeNameNormalized != null )
				getMatches(config, matches, potentials, placeNameNormalized);
			if ( ! term.term2.isEmpty() ) {
				getMatches(config, matches, potentials, getFilingForm(term.term1));
				getMatches(config, matches, potentials, getFilingForm(term.term1)+"s");
			}
			if ( ! term.term3.isEmpty() ) {
				getMatches(config, matches, potentials, getFilingForm(term.term1+" > "+term.term2));
				getMatches(config, matches, potentials, getFilingForm(term.term1)+"s > "+term.term2);
				if ( placeNameNormalized != null )
					getMatches(config, matches, potentials, placeNameNormalized+" > "+term.term2);
			}
			if ( ! term.term4.isEmpty() ) {
				getMatches(config, matches, potentials, getFilingForm(term.term1+" > "+term.term2+" > "+term.term3));
				getMatches(config, matches, potentials, getFilingForm(term.term1)+"s > "+term.term2+" > "+term.term3);
				if ( placeNameNormalized != null )
					getMatches(config, matches, potentials, placeNameNormalized+" > "+term.term2+" > "+term.term3);
			}
			potentials.addAll(matches);
			if ( potentials.isEmpty() ) {
				writeToNonmatchesFile(nonmatchesFile,term);
				unmatched++;
				continue;
			}
			writeToPotentialsFile(potentialsFile,term,potentials);
			potentiallyMatched++;
		}
		System.out.printf("matched: %d\npotentially matched: %d\nunmatched: %d\n",matched,potentiallyMatched,unmatched);
	}}

	private static void writeToPotentialsFile(PrintWriter potentialsFile, ASpaceTerm term, List<Match> potentials) {
//		System.out.printf("%s (%s)\n" , term.id, term.title);
//		for ( Match pm : potentials )
//			System.out.printf("\t--> %s (%s) %s - %s\n",pm.id,pm.term,pm.type,pm.source);
		boolean first = true;
		for ( Match match : potentials ) {
			List<String> fields = new ArrayList<>();
			if (first) {
				fields.add(term.id.replaceAll("\t", " "));
				fields.add(term.title.replaceAll("\t", " "));
				first = false;
			} else {
				fields.add(" ");
				fields.add(" ");
			}
			fields.add(match.id.replaceAll("\t", " "));
			fields.add(match.term.replaceAll("\t", " "));
			fields.add(match.type.toString());
			fields.add(match.source.toString());
			potentialsFile.println(String.join("\t", fields));
		}
	}

	private static void writeToNonmatchesFile(PrintWriter nonmatchesFile, ASpaceTerm term) {
		System.out.printf("%s (%s)\n" , term.id, term.title);
		nonmatchesFile.printf("%s\t%s\n",term.id.replaceAll("\t", " "),term.title.replaceAll("\t", " "));
	}

	private static void writeToMatchesFile(PrintWriter matchesFile, ASpaceTerm term, Match match) {
//		System.out.printf("%s (%s): (%s) %s - %s - %s\n" , term.id, term.title, m.term, m.id, m.type, m.source);
		List<String> fields = new ArrayList<>();
		fields.add(term.id);
		fields.add(term.title);
		fields.add(match.id);
		fields.add(match.term);
		fields.add(match.type.toString());
		fields.add(match.source.toString());
		for ( int i = 0; i < fields.size(); i++ )
			if (fields.get(i).contains("\t")) fields.set(i, fields.get(i).replaceAll("\t", " "));
		matchesFile.println(String.join("\t", fields));
	}

	private static String normalizePlaceNames(String term) {

		if (term.endsWith(" west virginia")) return term.replaceAll(" west virginia$", " w va");
		if (term.endsWith(" wv")) return term.replaceAll(" wv$", " w va");

		if (term.endsWith(" alabama")) return term.replaceAll(" alabama$", " ala");

		if (term.endsWith(" arizona")) return term.replaceAll(" arizona$", " ariz");
		if (term.endsWith(" az")) return term.replaceAll(" az$", " ariz");

		if (term.endsWith(" california")) return term.replaceAll(" california$", " calif");
		if (term.endsWith(" ca")) return term.replaceAll(" ca$", " calif");

		if (term.endsWith(" colorado")) return term.replaceAll(" colorado$", " colo");
		if (term.endsWith(" co")) return term.replaceAll(" co$", " colo");

		if (term.endsWith(" connecticut")) return term.replaceAll(" connecticut$", " conn");
		if (term.endsWith(" ct")) return term.replaceAll(" ct$", " conn");

		if (term.endsWith(" delaware")) return term.replaceAll(" delaware$", " del");
		if (term.endsWith(" de")) return term.replaceAll(" de$", " del");

		if (term.endsWith(" florida")) return term.replaceAll(" florida$", " fla");
		if (term.endsWith(" fl")) return term.replaceAll(" fl$", " fla");

		if (term.endsWith(" illinois")) return term.replaceAll(" illinois$", " ill");
		if (term.endsWith(" il")) return term.replaceAll(" il$", " ill");

		if (term.endsWith(" indiana")) return term.replaceAll(" indiana$", " ind");
		if (term.endsWith(" in")) return term.replaceAll(" in$", " ind");

		if (term.endsWith(" kansas")) return term.replaceAll(" kansas$", " kan");
		if (term.endsWith(" ks")) return term.replaceAll(" ks$", " kan");

		if (term.endsWith(" kentucky")) return term.replaceAll(" kentucky$", " ky");

		if (term.endsWith(" maine")) return term.replaceAll(" maine$", " me");

		if (term.endsWith(" louisiana")) return term.replaceAll(" louisianna$", " la");

		if (term.endsWith(" maryland")) return term.replaceAll(" maryland$", " md");

		if (term.endsWith(" massachusetts")) return term.replaceAll(" massachusetts$", " mass");
		if (term.endsWith(" ma")) return term.replaceAll(" ma$", " mass");

		if (term.endsWith(" michigan")) return term.replaceAll(" michigan$", " mich");
		if (term.endsWith(" mi")) return term.replaceAll(" mi$", " mich");

		if (term.endsWith(" mississippi")) return term.replaceAll(" mississippi$", " miss");
		if (term.endsWith(" ms")) return term.replaceAll(" ms$", " miss");

		if (term.endsWith(" minnesota")) return term.replaceAll(" minnesota$", " minn");

		if (term.endsWith(" missouri")) return term.replaceAll(" missouri$", " mo");

		if (term.endsWith(" montana")) return term.replaceAll(" montana$", " mont");
		if (term.endsWith(" mt")) return term.replaceAll(" mont$", " mont");

		if (term.endsWith(" nebraska")) return term.replaceAll(" nebraska$", " neb");

		if (term.endsWith(" nevada")) return term.replaceAll(" nevada$", " nev");
		if (term.endsWith(" nv")) return term.replaceAll(" nv$", " nev");

		if (term.endsWith(" new jersey")) return term.replaceAll(" new jersey$", " nj");

		if (term.endsWith(" new mexico")) return term.replaceAll(" new mexico$", " nm");

		if (term.endsWith(" new york")) return term.replaceAll(" new york$", " ny");

		if (term.endsWith(" north carolina")) return term.replaceAll(" north carolina$", " nc");

		if (term.endsWith(" oklahoma")) return term.replaceAll(" oklahoma$", " okla");
		if (term.endsWith(" ok")) return term.replaceAll(" ok$", " okla");

		if (term.endsWith(" oregon")) return term.replaceAll(" oregon$", " or");

		if (term.endsWith(" pennsylvania")) return term.replaceAll(" pennsylvania$", " pa");
		if (term.endsWith(" penn")) return term.replaceAll(" penn$", " pa");

		if (term.endsWith(" rhode island")) return term.replaceAll(" rhode island$", " ri");

		if (term.endsWith(" south carolina")) return term.replaceAll(" south carolina$", " sc");

		if (term.endsWith(" south dakota")) return term.replaceAll(" south dakota$", " sd");

		if (term.endsWith(" tennessee")) return term.replaceAll(" tennessee$", " tenn");
		if (term.endsWith(" tn")) return term.replaceAll(" tn$", " tenn");

		if (term.endsWith(" texas")) return term.replaceAll(" texas$", " tex");
		if (term.endsWith(" tx")) return term.replaceAll(" tx$", " tex");

		if (term.endsWith(" vermont")) return term.replaceAll(" vermont$", " vt");

		if (term.endsWith(" virginia")) return term.replaceAll(" virginia$", " va");

		if (term.endsWith(" washington")) return term.replaceAll(" washington$", " wash");
		if (term.endsWith(" wa")) return term.replaceAll(" wa$", " wash");

		if (term.endsWith(" wisconsin")) return term.replaceAll(" wisconsin$", " wis");

		return null;
	}

	private static Match tryToIdentifyUnambiguousMatch(List<Match> matches) {
		if ( matches.isEmpty() ) return null;
		if ( matches.size() == 1 ) return matches.get(0);
		Map<Integer,List<Match>> prioritizedMatches = new HashMap<>();
		for ( Match match : matches ) {
			int priority = ( match.source.equals(AuthoritySource.LOCAL) )?2:1;
			if ( ! prioritizedMatches.containsKey(priority) )
				prioritizedMatches.put(priority, new ArrayList<>());
			prioritizedMatches.get(priority).add(match);
		}
		int topPriority = Collections.min(prioritizedMatches.keySet());
		if ( prioritizedMatches.get(topPriority).size() == 1)
			return prioritizedMatches.get(topPriority).get(0);
		return null;
	}

	private static void getMatches( Config config, List<Match> matches, List<Match> xrefs, String term )
			throws SQLException {
		if ( term.isEmpty() ) return;
		try ( Connection headings = config.getDatabaseConnection("Headings");
				PreparedStatement authorityQuery = headings.prepareStatement(headingQuery);
				PreparedStatement refTypeQuery = headings.prepareStatement(referenceTypeQuery)) {
			authorityQuery.setString(1, term+'%');
			authorityQuery.setString(2, "^"+term+"[0-9 ]*$");
			try ( ResultSet rs = authorityQuery.executeQuery() ) {
				while (rs.next()) {
					Match match = new Match();
					match.id = rs.getString("identifier");
					match.source = sources[rs.getInt("source")];
					match.term = rs.getString("nativeHeading");
					match.type = headingTypes[rs.getInt("heading_type")];
					if ( rs.getBoolean("main_entry") ) {
						if (getFilingForm(match.term).equals(term)) {
							for ( Match m : matches ) if (m.id.equals(match.id) )
								continue;
							matches.add(match);
						} else if (match.type.equals(HeadingType.PERSNAME))
							xrefs.add(match);
					} else {
						refTypeQuery.setInt(1, match.source.ordinal());
						refTypeQuery.setString(2, match.id);
						refTypeQuery.setString(3, term);
						refTypeQuery.setString(4, getFilingForm(match.term));
						try ( ResultSet rs1 = refTypeQuery.executeQuery() ) {
							while (rs1.next()) if (rs1.getInt(1) == 1) {
								xrefs.add(match);
								break;
							}
						}
					}
				}
			}
		}
	}

	private static class ASpaceTerm {
		String title = null;
		String term1 = "";
		String term2 = "";
		String term3 = "";
		String term4 = "";
		String source = "";
		String id = "";
	}
	private static class Match {
		String term = null;
		HeadingType type = null;
		String id = null;
		AuthoritySource source = null;
	}
}
