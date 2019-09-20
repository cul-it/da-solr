package edu.cornell.library.integration.processing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.cornell.library.integration.utilities.BlacklightHeadingField;
import edu.cornell.library.integration.utilities.Config;

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
						("UPDATE generationQueue SET priority = 9 WHERE id = ?");
				PreparedStatement previousHeadingsStmt = headings.prepareStatement(
						"SELECT heading_id, heading FROM bib2heading WHERE bib_id = ? AND generator = ?");
						) {
			SolrHeadingBlock.SUBJECT.addBlockQuery( current.prepareStatement(
					"SELECT subject_solr_fields FROM solrFieldsData WHERE bib_id = ?"));
			SolrHeadingBlock.AUTHORTITLE.addBlockQuery( current.prepareStatement(
					"SELECT authortitle_solr_fields FROM solrFieldsData WHERE bib_id = ?"));
			SolrHeadingBlock.SERIES.addBlockQuery( current.prepareStatement(
					"SELECT series_solr_fields FROM solrFieldsData WHERE bib_id = ?"));

			while(true) {
				// Identify Bib to generate data for
				Integer bib = null;
				Integer priority = null;
				stmt.execute("LOCK TABLES generationQueue WRITE");
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
			}
		}
		
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

		EnumSet<BlacklightHeadingField> fields = null;
		PreparedStatement blockQuery = null;
		public void addBlockQuery( PreparedStatement blockQuery ) {
			this.blockQuery = blockQuery;
		}
		SolrHeadingBlock( EnumSet<BlacklightHeadingField> fields ) {
			this.fields = fields;
		}
	}
}
