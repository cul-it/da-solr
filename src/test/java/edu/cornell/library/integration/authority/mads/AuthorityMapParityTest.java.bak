package edu.cornell.library.integration.authority.mads;

import java.sql.Connection;

import static org.junit.Assert.assertNull;
import org.junit.Test;

public class AuthorityMapParityTest {

	@Test
	public void skipsJuvenileLccnRecords() throws Exception {
		String source = """
				{
				  "@graph": [
				    {
				      "@id": "http://id.loc.gov/authorities/subjects/sj00000001",
				      "identifiers:lccn": "sj 00000001",
				      "identifiers:local": "local-sj"
				    }
				  ]
				}
				""";

		MadsAuthority map = MadsAuthority.fromMadsJsonld(
				(Connection) null,
				"http://id.loc.gov/authorities/subjects/sj00000001",
				source,
				false);

		assertNull(map);
	}

	@Test
	public void skipsSubdivisionRecords() throws Exception {
		String source = """
				{
				  "@graph": [
				    {
				      "@id": "http://id.loc.gov/authorities/subjects/sh00000001",
				      "identifiers:lccn": "sh 00000001",
				      "identifiers:local": "local-subdiv",
				      "madsrdf:isMemberOfMADSCollection": {
				        "@id": "http://id.loc.gov/authorities/subjects/collection_Subdivisions"
				      }
				    }
				  ]
				}
				""";

		MadsAuthority map = MadsAuthority.fromMadsJsonld(
				(Connection) null,
				"http://id.loc.gov/authorities/subjects/sh00000001",
				source,
				false);

		assertNull(map);
	}

	// @Test
	// public void buildsRefDescriptionsReciprocalAndExtractsNotes() {
	// 	AuthorityMap.HeadingMap main = new AuthorityMap.HeadingMap(
	// 			null, "Alpha", "alpha", MadsHeadingType.TOPIC);
	// 	AuthorityMap.HeadingMap related = new AuthorityMap.HeadingMap(
	// 			null, "Beta", "beta", MadsHeadingType.TOPIC);

	// 	List<AuthorityMap.HeadingMap> headings = new ArrayList<>();
	// 	headings.add(main);
	// 	headings.add(related);

	// 	List<AuthorityMap.ReferenceMap> refs = AuthorityMap.madsReferencesPopulate(null, headings);
	// 	assertEquals(2, refs.size());
	// 	assertEquals(ReferenceType.FROM5XX, refs.get(0).refType());
	// 	assertEquals("Broader Term", refs.get(0).refDesc());
	// 	assertEquals(ReferenceType.TO5XX, refs.get(1).refType());
	// 	assertEquals("Narrower Term", refs.get(1).refDesc());

	// 	JsonObject mainEntry = Json.createObjectBuilder()
	// 			.add("@id", "http://id.loc.gov/authorities/subjects/sh11111111")
	// 			.add("madsrdf:note", "Top-level note")
	// 			.add("madsrdf:scopeNote", Json.createObjectBuilder().add("@value", "Scope note"))
	// 			.add("madsrdf:hasRelatedAuthority", Json.createObjectBuilder().add("@id", "http://id.loc.gov/authorities/subjects/sh22222222"))
	// 			.build();

	// 	JsonObject relatedEntry = Json.createObjectBuilder()
	// 			.add("@id", "http://id.loc.gov/authorities/subjects/sh22222222")
	// 			.add("madsrdf:note", "Related note")
	// 			.build();

	// 	JsonArray graph = Json.createArrayBuilder().add(mainEntry).add(relatedEntry).build();
	// 	List<String> notes = AuthorityMap.madsNotesPopulate(mainEntry, graph);

	// 	assertNotNull(notes);
	// 	assertTrue(notes.contains("Top-level note"));
	// 	assertTrue(notes.contains("Scope note"));
	// 	assertTrue(notes.contains("Related note"));
	// }
}
