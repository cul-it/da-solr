package edu.cornell.library.integration.processing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.Normalizer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Objects;

import edu.cornell.library.integration.metadata.support.Heading;
import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.metadata.support.HeadingType;
import edu.cornell.library.integration.utilities.BlacklightHeadingField;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.FilingNormalization;

public class ProcessHeadingsQueue {

	public static void main(String[] args) throws SQLException, InterruptedException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Headings"));
		Config config = Config.loadConfig(requiredArgs);

		try (	Connection current = config.getDatabaseConnection("Current");
				Connection headings = config.getDatabaseConnection("Headings");
				Statement stmt = current.createStatement();
				PreparedStatement nextBibStmt = current.prepareStatement
						("SELECT bib_id, priority FROM headingsQueue ORDER BY priority LIMIT 1");
				PreparedStatement allForBibStmt = current.prepareStatement
						("SELECT id, cause, record_date FROM headingsQueue WHERE bib_id = ? ORDER BY record_date");
				PreparedStatement deprioritizeStmt = current.prepareStatement
						("UPDATE headingsQueue SET priority = 9 WHERE id = ?");
				PreparedStatement browseQueueStmt = current.prepareStatement
						("INSERT INTO browseQueue (heading_id, priority, record_date) VALUES (?, ?, ?)");
				PreparedStatement dequeueStmt = current.prepareStatement
						("DELETE FROM headingsQueue WHERE id = ?");
						) {
			SolrHeadingBlock.SUBJECT.setBlockQuery( current.prepareStatement(
					"SELECT subject_solr_fields FROM processedMarcData WHERE bib_id = ?"));
			SolrHeadingBlock.AUTHORTITLE.setBlockQuery( current.prepareStatement(
					"SELECT authortitle_solr_fields FROM processedMarcData WHERE bib_id = ?"));
			SolrHeadingBlock.SERIES.setBlockQuery( current.prepareStatement(
					"SELECT series_solr_fields FROM processedMarcData WHERE bib_id = ?"));
			SolrHeadingBlock.TITLECHANGE.setBlockQuery( current.prepareStatement(
					"SELECT titlechange_solr_fields FROM processedMarcData WHERE bib_id = ?"));

			while(true) {
				// Identify Bib and Generator blocks to update headings for
				Integer bib = null;
				Integer priority = null;
				stmt.execute("LOCK TABLES headingsQueue WRITE");
				try (ResultSet rs = nextBibStmt.executeQuery()){
					while (rs.next()) {
						bib = rs.getInt(1);
						priority = rs.getInt(2);
					}
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
						if (minChangeDate == null)
							minChangeDate = recordDate;
						queueIds.add(id);
					}
					deprioritizeStmt.executeBatch();
				}
				stmt.execute("UNLOCK TABLES");
				System.out.println("** "+bib+": "+changes.toString());

				for ( SolrHeadingBlock b : changes) {
					Set<Heading> changedHeadings = lookForChangesToHeadings( headings, bib, b );
					if ( ! changedHeadings.isEmpty() ) {
						browseQueueStmt.setInt(2, priority);
						browseQueueStmt.setTimestamp(3, minChangeDate);
						for ( Heading h : changedHeadings ) {
							browseQueueStmt.setInt(1, h.id());
							browseQueueStmt.addBatch();
						}
						browseQueueStmt.executeBatch();
					}
				}
				for ( Integer id : queueIds ) {
					dequeueStmt.setInt(1, id);
					dequeueStmt.addBatch();
				}
				dequeueStmt.executeBatch();
			}
		}
	}

	private static HeadingType[] headingTypes = HeadingType.values();
	private static HeadingCategory[] headingCategories = HeadingCategory.values();
	private static Set<Heading> lookForChangesToHeadings(
			Connection headings, Integer bib, SolrHeadingBlock b) throws SQLException {

		Map<Heading,EnumSet<HeadingCategory>> prev = new HashMap<>();
		Map<Heading,EnumSet<HeadingCategory>> curr = new HashMap<>();

		try (PreparedStatement previousHeadingsStmt = headings.prepareStatement(
				"SELECT heading_id, bh.heading, sort, heading_type, heading_category FROM bib2heading bh, heading "+
				" WHERE bib_id = ? AND generator = ? AND bh.heading_id = heading.id") ){
			b.blockQuery.setInt(1, bib);
			previousHeadingsStmt.setInt(1,bib);
			previousHeadingsStmt.setString(2,b.name());
			try ( ResultSet newFields = b.blockQuery.executeQuery();
					ResultSet oldHeadings = previousHeadingsStmt.executeQuery() ) {

				while ( oldHeadings.next() ) {
					Heading h = new Heading(
							oldHeadings.getInt("heading_id"),oldHeadings.getString("heading"),
							oldHeadings.getString("sort"), headingTypes[oldHeadings.getInt("heading_type")]);
					HeadingCategory hc = headingCategories[oldHeadings.getInt("heading_category")];

					if ( prev.containsKey(h) ) prev.get(h).add(hc);
					else                       prev.put(h,EnumSet.of(hc));
				}

				while (newFields.next()) {
					String[] blockFields = newFields.getString(1).split("\n");
					for ( String lookedForFilingField : b.filingFields.keySet() )
						for ( String actualField : blockFields )
							if ( actualField.startsWith(lookedForFilingField) ) {
								BlacklightHeadingField blField = b.filingFields.get(lookedForFilingField);
								String sortForm = actualField.substring(lookedForFilingField.length()+2);
								String displayForm = ( sortForm.equals("electronic books"))?
										"Electronic books":findDisplayForm(blockFields,sortForm,blField);
								Heading h = getHeading( headings, displayForm, sortForm,blField);

								if ( curr.containsKey(h) ) curr.get(h).add(blField.headingCategory());
								else                       curr.put(h,EnumSet.of(blField.headingCategory()));
							}
				}

			}
		}

		Set<Heading> changedHeadings = new HashSet<>();
		for ( Entry<Heading,EnumSet<HeadingCategory>> e : prev.entrySet() ) {
			Heading h = e.getKey();
			EnumSet<HeadingCategory> hcs = e.getValue();
			if ( curr.containsKey(h) ) {
				EnumSet<HeadingCategory> newHcs = curr.get(h);
				curr.remove(h);
				if ( Objects.equal(hcs, newHcs) )
					continue;
				changedHeadings.add(h);
				EnumSet<HeadingCategory> droppedCategories = EnumSet.noneOf(HeadingCategory.class);
				for ( HeadingCategory hc : hcs )
					if ( newHcs.contains(hc) ) newHcs.remove(hc);
					else droppedCategories.add(hc);
				EnumSet<HeadingCategory> addedCategories = newHcs;
				if ( ! droppedCategories.isEmpty() )
					dropBibHeading( headings, bib, b.name(), h, droppedCategories );
				if ( ! addedCategories.isEmpty() )
					addBibHeading( headings, bib, b.name(), h, addedCategories );
				continue;
			}
			addBibHeading( headings, bib, b.name(), h, hcs );
		}
		for ( Entry<Heading,EnumSet<HeadingCategory>> e : curr.entrySet() ) {
			addBibHeading( headings,bib,b.name(),e.getKey(),e.getValue());
			changedHeadings.add(e.getKey());
		}

		return changedHeadings;
	}

	private static void dropBibHeading(
			Connection headings, Integer bib, String generatorBlock, Heading h, EnumSet<HeadingCategory> categories)
					throws SQLException {
		try( PreparedStatement pstmt = headings.prepareStatement(
				"DELETE FROM bib2heading WHERE bib_id = ? AND generator = ? AND heading_id = ? AND heading_category = ?") ) {
			for ( HeadingCategory hc : categories ) {
				pstmt.setInt(   1, bib);
				pstmt.setString(2, generatorBlock);
				pstmt.setInt(   3, h.id());
				pstmt.setInt(   4, hc.ordinal());
				pstmt.addBatch();
				System.out.printf("-%s: \"%s\"\t(%s)\n", hc,h.displayForm(),h.headingType());
			}
			pstmt.executeBatch();
		}
	}

	private static void addBibHeading(Connection headings, int bib, String generatorBlock, Heading h, EnumSet<HeadingCategory> categories )
			throws SQLException {
		try( PreparedStatement pstmt = headings.prepareStatement(
				"INSERT INTO bib2heading ( bib_id, generator, heading_id, heading, heading_category ) values (?,?,?,?,?)") ) {
			for ( HeadingCategory hc : categories ) {
				pstmt.setInt(   1, bib);
				pstmt.setString(2, generatorBlock);
				pstmt.setInt(   3, h.id());
				pstmt.setString(4, h.displayForm());
				pstmt.setInt(   5, hc.ordinal());
				pstmt.addBatch();
				System.out.printf("+%s: \"%s\"\t(%s)\n", hc,h.displayForm(),h.headingType());
			}
			pstmt.executeBatch();
		}
	}

	private static Heading getHeading(
			Connection headings, String displayForm, String sortForm, BlacklightHeadingField blField) throws SQLException {

		// get heading id if already exists
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"SELECT id FROM heading " +
				"WHERE heading_type = ? AND sort = ?") ){
			pstmt.setInt(1,blField.headingType().ordinal());
			pstmt.setString(2,sortForm);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next()) return new Heading(resultSet.getInt(1),displayForm,sortForm,blField.headingType());
			}
		}

		// Create record and return id, otherwise
		try (PreparedStatement pstmt = headings.prepareStatement(
				"INSERT INTO heading (heading, sort, heading_type, parent_id) VALUES (?, ?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS) ) {

			pstmt.setString(1, Normalizer.normalize(displayForm, Normalizer.Form.NFC));
			pstmt.setString(2, sortForm);
			pstmt.setInt(3,    blField.headingType().ordinal());
			pstmt.setInt(4,    0);//TODO Drop parentId or fully populate. (Intended for canceled issue DISCOVERYACCESS-3069)
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) 
				throw new SQLException("Creating Heading Record Failed.");
			try ( ResultSet generatedKeys = pstmt.getGeneratedKeys() ) {
				if (generatedKeys.next())  return new Heading(generatedKeys.getInt(1),displayForm,sortForm,blField.headingType());
			}
		}

		throw new SQLException("Creating Heading Record Failed.");
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
				BlacklightHeadingField.AUTHORTITLE_WORK)),
		TITLECHANGE(EnumSet.of(
				BlacklightHeadingField.AUTHOR_PERSON,
				BlacklightHeadingField.AUTHOR_CORPORATE,
				BlacklightHeadingField.AUTHOR_EVENT));

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
