package edu.cornell.library.integration.indexTesting;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

public class BuildDisplayFieldTestRecordSuite {

	public static void main(String[] args) {
		Collection<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("TestSuite");
		requiredArgs.add("blacklightSolrUrl");
		try {
			new BuildDisplayFieldTestRecordSuite( SolrBuildConfig.loadConfig(args, requiredArgs));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public BuildDisplayFieldTestRecordSuite(SolrBuildConfig config) throws Exception {
		Connection db = config.getDatabaseConnection("TestSuite");
		Statement stmt = db.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT doc_id) FROM value");
		Integer count = null;
		while (rs.next())
			count = rs.getInt(1);
		if (count == null)
			throw new SQLException("Failed to retrieve unique document count from test suite database.");
		System.out.println(count);
		HttpSolrClient solr = new HttpSolrClient(config.getBlacklightSolrUrl());
		SolrQuery query = new SolrQuery();
		query.setQuery("id:*");
		query.setFields("*");
		query.set("defType", "lucene");
		query.setRows(1);

		while (count < 10 && knownFieldsNeedBetterCoverage(db)) {
			//http://da-stg-ssolr.library.cornell.edu/solr/blacklight/select?
			//qt=standard&rows=1&q=id:*&sort=random123658431547%20desc&fl=id
			int seed = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
			query.setSort("random"+seed,SolrQuery.ORDER.desc);
			for (SolrDocument doc : solr.query(query).getResults()) {
				int doc_id = Integer.valueOf((String)doc.getFieldValue("id"));
				System.out.println(doc_id);
				boolean containsNeededFields = false;
				for (String field : doc.getFieldNames())
					if (5 > getDocCountForField(field,db)) {
						containsNeededFields = true;
						break;
					}
				if (! containsNeededFields) {
					System.out.println("Dropping document #"+doc_id+" as not fresh enough.");
					continue;
				}
				System.out.println("Using document #"+doc_id+".");
				for (String field : doc.getFieldNames()) {
					Integer field_id = findExistingFieldInDB(field,db);
					if (field_id == null)
						field_id = registerField(field,db);
					registerFieldContents(doc_id, field_id, doc.getFieldValue(field), db);
				}
				count++;
			}
		}
		solr.close();
	}

	PreparedStatement knownFieldsWhichNeedCoverageStmt = null;
	private boolean knownFieldsNeedBetterCoverage(Connection db) throws SQLException {
		if (knownFieldsWhichNeedCoverageStmt == null)
			knownFieldsWhichNeedCoverageStmt = db.prepareStatement(
					"SELECT field.name, COUNT(DISTINCT doc_id) AS doc_count "
					+ "FROM field, value  "
					+ "WHERE field.id = value.field_id "
					+ "GROUP BY field.id "
					+ "HAVING doc_count < 5");
		StringBuilder sb = new StringBuilder();
		ResultSet rs = knownFieldsWhichNeedCoverageStmt.executeQuery();
		while (rs.next()) {
			sb.append(rs.getString(1)).append(" (").append(rs.getString(2)).append(") ");
		}
		if (sb.length() > 0) {
			System.out.println(sb.toString());
			return true;
		}
		return false;
	}

	PreparedStatement docCountForFieldStmt = null;
	private int getDocCountForField(String field, Connection db) throws SQLException {
		if (docCountForFieldStmt == null)
			docCountForFieldStmt = db.prepareStatement(
					"SELECT COUNT(DISTINCT doc_id) "
					+ "FROM field, value "
					+ "WHERE field.id = value.field_id "
					+ "AND  field.name = ?");
		docCountForFieldStmt.setString(1, field);
		int docs = 0;
		ResultSet rs = docCountForFieldStmt.executeQuery();
		while (rs.next())
			docs = rs.getInt(1);
		rs.close();
		return docs;
	}

	PreparedStatement insertFieldValueStmt = null;
	@SuppressWarnings("unchecked")
	private void registerFieldContents(int doc_id, Integer field_id,
			Object fieldValue, Connection db) throws SQLException {
		if (insertFieldValueStmt == null)
			insertFieldValueStmt = db.prepareStatement(
					"INSERT INTO value (field_id, doc_id, value) VALUES (?, ?, ?)");
		insertFieldValueStmt.setInt(1, field_id);
		insertFieldValueStmt.setInt(2, doc_id);
		if (fieldValue.getClass().equals(String.class)) {
			insertFieldValueStmt.setString(3, (String)fieldValue);
			insertFieldValueStmt.executeUpdate();
		} else if (fieldValue.getClass().equals(ArrayList.class)) {
			for (Object value : (ArrayList<Object>) fieldValue) {
				insertFieldValueStmt.setString(3, (String)value);
				insertFieldValueStmt.executeUpdate();
			}
		} else {
			System.out.println("Unexpected object type for field "+field_id+": "
					+fieldValue.getClass().getName());
		}
	}

	PreparedStatement insertFieldStmt = null;
	private Integer registerField(String field, Connection db) throws SQLException {
		if (insertFieldStmt == null)
			insertFieldStmt = db.prepareStatement("INSERT INTO field (name) VALUES (?)",
                    Statement.RETURN_GENERATED_KEYS);
		insertFieldStmt.setString(1, field);
		int affectedCount = insertFieldStmt.executeUpdate();
		if (affectedCount < 1)
			throw new SQLException("Registering new field in DB failed.");
		ResultSet generatedKeys = insertFieldStmt.getGeneratedKeys();
		Integer field_id = null;
		if (generatedKeys.next())
			field_id = generatedKeys.getInt(1);
		generatedKeys.close();
		return field_id;
		
	}

	PreparedStatement isNewFieldStmt = null;
	private Integer findExistingFieldInDB(String field, Connection db) throws SQLException {
		if (isNewFieldStmt == null) 
			isNewFieldStmt = db.prepareStatement("SELECT * FROM field WHERE name = ?");
		isNewFieldStmt.setString(1, field);
		ResultSet rs = isNewFieldStmt.executeQuery();
		Integer field_id = null;
		while (rs.next())
			field_id = rs.getInt("id");
		rs.close();
		return field_id;
	}

}
