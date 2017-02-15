package edu.cornell.library.integration.indexer.updates;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.googlecode.sardine.DavResource;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;
import edu.cornell.library.integration.utilities.IndexingUtilities.IndexQueuePriority;

public class RecordBatchProcessingData {

	public static void main(String[] args) throws Exception {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Current");
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForWebdav());
		requiredArgs.add("batchInfoDir");
		SolrBuildConfig config = SolrBuildConfig.loadConfig(null, requiredArgs);
		new RecordBatchProcessingData( config );
	}

	public RecordBatchProcessingData(SolrBuildConfig config) throws Exception {

		DavService davService = DavServiceFactory.getDavService( config );
		String batchDirUrl = config.getWebdavBaseUrl()+"/"+config.getBatchInfoDir();
		List<DavResource> resources = davService.getResourceList(batchDirUrl);
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd HH:mm:ss");
		try ( Connection current = config.getDatabaseConnection("Current") ) {

			
			for (DavResource resource : resources) {
				String filename = resource.getName();
				if (! filename.startsWith("replkeys"))
					continue;
				if (batchAlreadyProcessed(current,filename))
					continue;
				Date startDate = new Date( 1000 * Long.valueOf(
						filename.substring(filename.lastIndexOf('.')+1)));
				Date endDate = resource.getModified();
				JobType jt;
				Set<Integer> bibsToUpdate = new HashSet<>();
				try ( InputStream is = davService.getFileAsInputStream(batchDirUrl+'/'+filename);
					BufferedReader reader = new BufferedReader(new InputStreamReader(is)) ){
					reader.readLine(); // discard first line
					String secondLine = reader.readLine();
					String jobCode = StringUtils.split(
							StringUtils.split(secondLine, '\t')[0] , '=')[1];
					jt = getJobType(current,jobCode);
					String line;
					int bibCount = 0;
					while ((line = reader.readLine()) != null) {
						bibCount++;
						Integer bib = Integer.valueOf(line);
						if (doesBibHaveMatchigUpdateDate(current,bib,startDate,endDate)) {
							bibsToUpdate.add(bib);
						}
					}
					System.out.println(filename+'\t'+
							formatter.format(startDate)+" --> "+
							formatter.format(endDate)+" ("+jt.description+") "+
							bibsToUpdate.size()+'/'+bibCount);
				}
				updateQueueItems(current,bibsToUpdate,jt);
				insertRecordOfBatch(current,jt,startDate,endDate,filename,bibsToUpdate.size());
			}
		}
	}

	PreparedStatement batchAlreadyProcessedPstmt = null;
	private boolean batchAlreadyProcessed(Connection current, String filename) throws SQLException {

		if (batchAlreadyProcessedPstmt == null)
			batchAlreadyProcessedPstmt = current.prepareStatement(
					"SELECT * FROM processedBatch WHERE batch_file = ?");
		batchAlreadyProcessedPstmt.setString(1,filename);
		boolean processed = false;
		try ( ResultSet rs = batchAlreadyProcessedPstmt.executeQuery() ){
			while (rs.next())
				processed = true;
		}
		return processed;
	}

	private static void insertRecordOfBatch(Connection current, JobType jt,
			Date startDate, Date endDate, String filename, int count) throws SQLException {

		try ( PreparedStatement pstmt = current.prepareStatement(
				"INSERT INTO processedBatch (batch_file, job_type, bib_count, execution_start, execution_finish)"
					+ "VALUES (?, ?, ?, ?, ?)") ){

			pstmt.setString(1, filename);
			pstmt.setString(2, jt.code);
			pstmt.setInt(3, count);
			pstmt.setTimestamp(4, new Timestamp(startDate.getTime()));
			pstmt.setTimestamp(5, new Timestamp(endDate.getTime()));
			pstmt.executeUpdate();
		}
	}

	private static void updateQueueItems(Connection current, Set<Integer> bibsToUpdate,
			JobType jt) throws SQLException {
		if (bibsToUpdate == null || bibsToUpdate.isEmpty())
			return;

		try ( PreparedStatement pstmt = current.prepareStatement(
				"UPDATE "+CurrentDBTable.QUEUE+" SET cause = ? AND priority = ? "+
				"WHERE bib_id = ? AND cause = '"+DataChangeUpdateType.BIB_UPDATE+"'") ){

			pstmt.setString(1,"Batch: "+((jt.description.isEmpty())?jt.code:jt.description));
			pstmt.setInt(1, jt.priority.ordinal());
			for (Integer bib : bibsToUpdate) {
				pstmt.setInt(3, bib);
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		}
	}

	private static boolean doesBibHaveMatchigUpdateDate(Connection current, Integer bib,
			Date startDate, Date endDate) throws SQLException {

		Boolean matching = false;

		try ( PreparedStatement pstmt = current.prepareStatement(
				"SELECT record_date FROM "+CurrentDBTable.BIB_VOY+" WHERE bib_id = ?") ){

			pstmt.setInt(1, bib);
			try ( ResultSet rs = pstmt.executeQuery() ){

				while (rs.next()) {
					Timestamp recordDate = rs.getTimestamp(1);
					if (recordDate.before(startDate) || recordDate.after(endDate))
						matching = false;
					else
						matching = true;
				}
			}
		}
		return matching;
	}

	PreparedStatement identifyBatchPstmt = null;
	private JobType getJobType( Connection current, String typeCode ) throws SQLException {
		if (identifyBatchPstmt == null)
			identifyBatchPstmt = current.prepareStatement(
				"SELECT * FROM batchType WHERE job_type = ?");
		identifyBatchPstmt.setString(1, typeCode);
		JobType jt = null;
		try ( ResultSet rs = identifyBatchPstmt.executeQuery() ){

			while (rs.next()) {
				jt = new JobType(typeCode,rs.getString("job_description"),
						IndexQueuePriority.values()[rs.getInt("processing_priority")]);
			}
		}
		if (jt == null)
			jt = new JobType(typeCode,null,IndexQueuePriority.DATACHANGE_SECONDARY);
		return jt;
	}

	private class JobType {
		String code = null;
		String description = null;
		IndexQueuePriority priority = null;
		public JobType(String code, String description, IndexQueuePriority priority) {
			this.code = code;
			this.description = description;
			this.priority = priority;
		}
	}

}
