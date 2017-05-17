package edu.cornell.library.integration.indexer;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldValues;

@RunWith(Parameterized.class)
public class MarcRecordAuthorTitleSegregationTest {

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{
				new DataField
				(1,"776",'0','8',"776 08 ‡i Print version: ‡a Rosengarten, Frank, 1927- ‡t Revolutionary Marxism of "
						+ "Antonio Gramsci. ‡d Leiden, Netherlands : Brill, c2013 ‡h viii, 197 pages ‡k Historical "
						+ "materialism book series ; Volume 62. ‡x 1570-1522 ‡z 9789004265745 ‡w 2013041807"),
				new FieldValues(
						"Rosengarten, Frank, 1927-",
						"Revolutionary Marxism of Antonio Gramsci. Leiden, Netherlands : Brill, c2013 Historical "
								+ "materialism book series ; Volume 62.")},
			{
				new DataField(1,"760",'1',' ',"‡a FAO statistics series"),
				new FieldValues( null, "FAO statistics series" )
			},
			{
				new DataField(1,"780",'0','0',"‡7 clas ‡a California. Dept. of Human Resources Development. Rural "
						+ "manpower report ‡g 1972"),
				new FieldValues(null,"California. Dept. of Human Resources Development. Rural manpower report 1972")
			},
			{
				new DataField
				(1,"780",'0','0',"‡7 c2as ‡a International Printing and Graphic Communications Union. Convention. "
						+ "‡t Convention proceedings of the International Printing & Graphic Communications Union"),
				new FieldValues(
						"International Printing and Graphic Communications Union. Convention.",
						"Convention proceedings of the International Printing & Graphic Communications Union")
			},
			{
				new DataField(1,"785",'0','0',"‡7 un ‡a Citizenship bulletin (1953)"),
				new FieldValues(null,"Citizenship bulletin (1953)")
			}

		});
	}

	private DataField inputField;
	private FieldValues expectedResult;


	public MarcRecordAuthorTitleSegregationTest( DataField inputField, FieldValues expectedResult) {
		this.inputField = inputField;
		this.expectedResult = expectedResult;
	}

	@Test
	public void testAuthorTitleSegregation() {

		FieldValues generated = inputField.getFieldValuesForNameAndOrTitleField("abcdegkqrst");
		assertEquals(expectedResult.type,generated.type);
		assertEquals(expectedResult.author,generated.author);
		assertEquals(expectedResult.title,generated.title);

	}
}
