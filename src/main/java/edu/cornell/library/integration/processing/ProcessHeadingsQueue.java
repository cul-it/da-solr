package edu.cornell.library.integration.processing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.utilities.BlacklightHeadingField;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.FilingNormalization;

public class ProcessHeadingsQueue {

	public static void main(String[] args) throws SQLException, InterruptedException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Headings"));
		Config config = Config.loadConfig(requiredArgs);

		new ProcessHeadingsQueue ( config );
	}

	public ProcessHeadingsQueue(Config config) throws SQLException, InterruptedException {

		try (	Connection current = config.getDatabaseConnection("Current");
				Connection headings = config.getDatabaseConnection("Headings");
				Statement stmt = current.createStatement();
				PreparedStatement nextBibStmt = current.prepareStatement
						("SELECT bib_id, priority FROM headingsQueue ORDER BY priority LIMIT 1");
				PreparedStatement allForBibStmt = current.prepareStatement
						("SELECT id, cause, record_date FROM headingsQueue WHERE bib_id = ?");
				PreparedStatement deprioritizeStmt = current.prepareStatement
						("UPDATE headingsQueue SET priority = 9 WHERE id = ?");
				PreparedStatement previousHeadingsStmt = headings.prepareStatement(
						"SELECT heading_id, heading FROM bib2heading WHERE bib_id = ? AND generator = ?");
						) {
			SolrHeadingBlock.SUBJECT.setBlockQuery( current.prepareStatement(
					"SELECT subject_solr_fields FROM solrFieldsData WHERE bib_id = ?"));
			SolrHeadingBlock.AUTHORTITLE.setBlockQuery( current.prepareStatement(
					"SELECT authortitle_solr_fields FROM solrFieldsData WHERE bib_id = ?"));
			SolrHeadingBlock.SERIES.setBlockQuery( current.prepareStatement(
					"SELECT series_solr_fields FROM solrFieldsData WHERE bib_id = ?"));

			while(true) {
				// Identify Bib and Generator blocks to update headings for
				Integer bib = null;
				Integer priority = null;
				stmt.execute("LOCK TABLES headingsQueue WRITE");
				try (ResultSet rs = nextBibStmt.executeQuery()){
					while (rs.next()) { bib = rs.getInt(1); priority = rs.getInt(2); }
				}

				if (bib == null || priority == null) {
					stmt.execute("UNLOCK TABLES");
					Thread.sleep(5000);
					continue;
				}

				allForBibStmt.setInt(1,bib);
				Set<Integer> queueIds = new HashSet<>();
				Timestamp minChangeDate = null;
				EnumSet<SolrHeadingBlock> changes = EnumSet.noneOf(SolrHeadingBlock.class);
				try ( ResultSet rs = allForBibStmt.executeQuery() ) {
					while (rs.next()) {
						Integer id = rs.getInt("id");
						Timestamp recordDate = rs.getTimestamp("record_date");
						String cause = rs.getString("cause");
						if ( cause.equals("Delete all") )
							changes = EnumSet.allOf(SolrHeadingBlock.class);
						else for ( SolrHeadingBlock c : SolrHeadingBlock.values() )
							if ( cause.contains(c.name()) ) changes.add(c);
						deprioritizeStmt.setInt(1,id);
						deprioritizeStmt.addBatch();
						if (minChangeDate == null || minChangeDate.after(recordDate))
							minChangeDate = recordDate;
						queueIds.add(id);
					}
					deprioritizeStmt.executeBatch();
				}
				stmt.execute("UNLOCK TABLES");
				System.out.println("** "+bib+": "+changes.toString());

				for ( SolrHeadingBlock b : changes)
					lookForChangesToHeadings( bib, b, previousHeadingsStmt );
				System.exit(0);
			}
		}
	}

	private static void lookForChangesToHeadings(Integer bib, SolrHeadingBlock b, PreparedStatement previousHeadingsStmt)
			throws SQLException {

		System.out.println( b.name() );
		b.blockQuery.setInt(1, bib);
		previousHeadingsStmt.setInt(1,bib);
		previousHeadingsStmt.setString(2,b.name());
		try ( ResultSet newFields = b.blockQuery.executeQuery();
				ResultSet oldHeadings = previousHeadingsStmt.executeQuery() ) {

			System.out.println("Previous Headings");
			while ( oldHeadings.next() )
				System.out.printf("\t%d: %s\n", oldHeadings.getInt("heading_id"),oldHeadings.getString("heading"));

			System.out.println("\nCurrent Headings");
			while (newFields.next()) {
				String[] blockFields = newFields.getString(1).split("\n");
				for ( String lookedForFilingField : b.filingFields.keySet() )
					for ( String actualField : blockFields )
						if ( actualField.startsWith(lookedForFilingField) ) {
							String sortForm = actualField.substring(lookedForFilingField.length()+2);
							String displayForm = findDisplayForm(blockFields,sortForm,b.filingFields.get(lookedForFilingField));
							System.out.printf("%s (%s): %s, %s\n", displayForm, sortForm,
									b.filingFields.get(lookedForFilingField).headingCategory(),
									b.filingFields.get(lookedForFilingField).headingTypeDesc());
						}
			}
		}
		
	}

	private static String findDisplayForm(String[] blockFields, String sortForm, BlacklightHeadingField bhf) {
		String displayFormFieldName = bhf.facetField();
		for ( String blockField : blockFields )
			if ( blockField.startsWith(displayFormFieldName) ) {
				String fieldValue = blockField.substring(displayFormFieldName.length()+2);
				if ( FilingNormalization.getFilingForm(fieldValue).equals(sortForm) )
					return fieldValue;
			}
		return null;
	}

	private enum SolrHeadingBlock {
		SUBJECT(EnumSet.of(
				BlacklightHeadingField.SUBJECT_PERSON,
				BlacklightHeadingField.SUBJECT_CORPORATE,
				BlacklightHeadingField.SUBJECT_EVENT,
				BlacklightHeadingField.SUBJECT_WORK,
				BlacklightHeadingField.SUBJECT_TOPIC,
				BlacklightHeadingField.SUBJECT_PLACE,
				BlacklightHeadingField.SUBJECT_CHRON,
				BlacklightHeadingField.SUBJECT_GENRE)),
		AUTHORTITLE(EnumSet.of(
				BlacklightHeadingField.AUTHOR_PERSON,
				BlacklightHeadingField.AUTHOR_CORPORATE,
				BlacklightHeadingField.AUTHOR_EVENT,
				BlacklightHeadingField.AUTHORTITLE_WORK)),
		SERIES(EnumSet.of(
				BlacklightHeadingField.AUTHORTITLE_WORK));

		Map<String,BlacklightHeadingField> filingFields = new HashMap<>();
		PreparedStatement blockQuery = null;
		public void setBlockQuery( PreparedStatement blockQuery ) {
			this.blockQuery = blockQuery;
		}
		SolrHeadingBlock( EnumSet<BlacklightHeadingField> fields ) {
			for ( BlacklightHeadingField bhf : fields )
				this.filingFields.put(bhf.fieldName(),bhf);
		}
	}
}
