package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

@SuppressWarnings("static-method")
public class CallNumberTest {

	static SolrBuildConfig config = null;
	static CallNumber callno = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("CallNos");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
		callno = new CallNumber();
	}

	@Test
	public void testHoldingCallNo() throws ClassNotFoundException, SQLException {
		MarcRecord.DataField f = new MarcRecord.DataField(3,"852");
		f.ind1 = '0';
		f.subfields.add(new MarcRecord.Subfield(1, 'h', "QA611"));
		f.subfields.add(new MarcRecord.Subfield(2, 'i', ".R123.6"));
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(f);
		for (FieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			assertEquals(1,              vals.search.size());
			assertEquals("QA611 .R123.6",vals.search.iterator().next());
			assertEquals("QA611 .R123.6",vals.sort.sortVal);
			System.out.println(String.join("\n", vals.facet));
		}
	}
}
