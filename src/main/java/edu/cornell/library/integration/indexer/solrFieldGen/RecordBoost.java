package edu.cornell.library.integration.indexer.solrFieldGen;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.MarcRecord;

public class RecordBoost implements ResultSetToFields, SolrFieldGenerator {

	private static Map<String,Integer> boostedIds = null;
	private static final Integer DEFAULT_BOOST = 100;

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		JenaResultsToMarcRecord.addControlFieldResultSet(rec,results.get("001"));

		SolrFields vals = generateSolrFields( rec, null );

		Map<String,SolrInputField> fields = new HashMap<>();
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);
		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	// This SolrFieldGenerator is not currently using MARC data
	public List<String> getHandledFields() { return Arrays.asList(); }

	@Override
	public SolrFields generateSolrFields(MarcRecord rec, SolrBuildConfig config)
			throws ClassNotFoundException, SQLException, IOException {
		if (boostedIds == null)
			boostedIds = importBoostedIds();
		SolrFields sfs = new SolrFields();
		if (boostedIds.containsKey(rec.id))
			sfs.setRecordBoost(boostedIds.get(rec.id));
		return sfs;
	}

	private static Map<String,Integer> importBoostedIds() {
		URL url = ClassLoader.getSystemResource("boosted_records.txt");
		Map<String,Integer> boostedIds = new HashMap<>();
		try {
			Path p = Paths.get(url.toURI());
			List<String> records = Files.readAllLines(p, StandardCharsets.UTF_8);
			for (String record : records) {
				String [] parts = record.split("\\s+",2);
				if (parts.length == 1)
					boostedIds.put(parts[0], DEFAULT_BOOST);
				else if (parts.length == 2) {
					try {
						boostedIds.put(parts[0],Integer.valueOf(parts[1]));
					} catch (@SuppressWarnings("unused") NumberFormatException e) {
						boostedIds.put(parts[0], DEFAULT_BOOST);
					}
				}
			}
		} catch (URISyntaxException e) {
			// This should never happen since the URI syntax is machine generated.
			e.printStackTrace();
		} catch (@SuppressWarnings("unused") IOException e) {
			System.out.println("Couldn't read list of boosted record IDs Not boosting records.");
			return boostedIds;
		}
		return boostedIds;
	}

}
