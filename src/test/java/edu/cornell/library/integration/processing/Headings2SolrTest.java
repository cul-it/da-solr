package edu.cornell.library.integration.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.db_test.DbBaseTest;

public class Headings2SolrTest extends DbBaseTest {

	@BeforeClass
	public static void setup() throws IOException, SQLException {
		setup("Headings");
	}

	@Test
	public void testGetNotes() throws SQLException {
		try ( Connection conn = config.getDatabaseConnection("Headings") ){
			// Corporate Name: Freemasons
			Collection<String> notes = Headings2Solr.getNotes(conn, 296895);
			assertEquals(1,notes.size());
			assertEquals("[\"Search under: subdivision Freemasonry under names of persons\"]",notes.iterator().next());

			// Topic: Mines and mineral resources
			notes = Headings2Solr.getNotes(conn, 1537075);
			Iterator<String> i = notes.iterator();
			assertEquals(2,notes.size());
			assertEquals("[\"Search under: headings beginning with the words Mine and Mining\"]",i.next());
			assertEquals("[\"Search under: subdivision Effect of mining on under individual animals"
					+ " and groups of animals, e.g. Fishes--Effect of mining on\"]",i.next());

			// Person : Waugh, Hillary
			notes = Headings2Solr.getNotes(conn, 4496);
			assertEquals(1,notes.size());
			assertEquals("[\"For works of this author entered under other names, search also under\","
					+ "{\"header\":\"Grandower, Elissa\"},{\"header\":\"Taylor, H. Baldwin\"}]",notes.iterator().next());
		}
	}
}
