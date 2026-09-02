package edu.cornell.library.integration.metadata.support;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.metadata.support.ReplacementHeadings.ReplaceResults;

public class ReplacementHeadingsTest extends DbBaseTest {


	@BeforeClass
	public static void setup() throws IOException, SQLException {
		setup("Headings");
	}


	@Test
	public void replacementHeadingsTest() throws SQLException {
		try (Connection headings = config.getDatabaseConnection("Headings");) {
			ReplacementHeadings.initialize(headings);
			System.out.println(ReplacementHeadings.toString(null));

			// apply subdivision mapping for 20th century
			List<String> parts = Arrays.asList("Tulips","20th century","In the rain");
			ReplaceResults replaceData = ReplacementHeadings.checkForHeadingReplacements(parts);
			assertEquals("Tulips > Subdivision map > In the rain", String.join(" > ",replaceData.afterHeading()));

			// apply mapping of two subdivisions to only one
			parts = Arrays.asList("Tulips","20th century","History");
			replaceData = ReplacementHeadings.checkForHeadingReplacements(parts);
			assertEquals("Tulips > Two subdivisions mapped", String.join(" > ", replaceData.afterHeading()));

			// apply a different mapping to main heading than to a similar subdivision
			parts = Arrays.asList( "20th Century", "History", "20th CENTURY");
			replaceData = ReplacementHeadings.checkForHeadingReplacements(parts);
			assertEquals("Main heading map > History > Subdivision map", String.join(" > ", replaceData.afterHeading()));


			// apply mapping of Roses to Daisies
			replaceData = ReplacementHeadings.checkForHeadingReplacements(Arrays.asList( "Roses"));
			assertEquals("Daisies", String.join(" > ", replaceData.afterHeading()));
			// and Lilies to Roses
			replaceData = ReplacementHeadings.checkForHeadingReplacements(Arrays.asList( "Lilies"));
			assertEquals("Roses", String.join(" > ", replaceData.afterHeading()));
			// overlay is not transitory, which would have resulted in Lilies mapped to Daisies

			// do not apply main heading overlay to subdivision
			replaceData = ReplacementHeadings.checkForHeadingReplacements(Arrays.asList( "Tulips"+"Lilies"));
			assertNull(replaceData);

			// main heading with one subdivision overlaid to just a main heading
			replaceData = ReplacementHeadings.checkForHeadingReplacements(Arrays.asList(
					"Main Heading","Subdivision"));
			assertEquals("Grapes", String.join(" > ", replaceData.afterHeading()));
		}
	}

}
