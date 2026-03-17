package edu.cornell.library.integration.metadb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import edu.cornell.library.integration.metadb.Utils.Cause;
import edu.cornell.library.integration.metadb.Utils.FolioType;
import edu.cornell.library.integration.utilities.Config;
import static edu.cornell.library.integration.metadb.Utils.queueUpdateToCache;

public class RecordCurrencySurvey {

	public static void main(String[] args) throws SQLException {

		List<String> requiredArgs = Config.getRequiredArgsForDB("MetaDB");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Current"));
		Config config = Config.loadConfig(requiredArgs);

		try (Connection inventory = config.getDatabaseConnection("Current");
			 Connection metadb    = config.getDatabaseConnection("MetaDB");) {
			UUID lastCursor = null;


			UUID instanceCursor = UUID.fromString("00000000-0000-0000-0000-000000000000");
			while ( ! instanceCursor.equals(lastCursor) ) { 
				lastCursor = instanceCursor;
				instanceCursor = checkBatch(inventory, FolioType.INSTANCE,  metadb, instanceCursor);
			}


			UUID holdingCursor = UUID.fromString("00000000-0000-0000-0000-000000000000");
			while ( ! holdingCursor.equals(lastCursor) ) { 
				lastCursor = holdingCursor;
				holdingCursor = checkBatch(inventory, FolioType.HOLDING,  metadb, holdingCursor);
			}


			UUID itemCursor = UUID.fromString("00000000-0000-0000-0000-000000000000");
			while ( ! itemCursor.equals(lastCursor) ) { 
				lastCursor = itemCursor;
				itemCursor = checkBatch(inventory, FolioType.ITEM,  metadb, itemCursor);
			}
		}
	}

	private static UUID checkBatch(Connection inventory, FolioType type, Connection metadb, UUID cursor)
			throws SQLException {
		int batchSize = 10000;
		try (PreparedStatement cacheStmt = inventory.prepareStatement(
				"SELECT id," +
				"       TRIM(BOTH '\"' FROM JSON_EXTRACT(content,'$.metadata.updatedDate')) AS moddate" +
				"  FROM " + type.cacheTableName +
				" WHERE id > ?" +
				" ORDER BY id" +
				" LIMIT "+batchSize);
			 PreparedStatement metadbStmt = metadb.prepareStatement(
				"SELECT id," +
				"       JSONB_EXTRACT_PATH_TEXT(jsonb,'metadata','updatedDate') AS moddate" +
				"  FROM "+ type.metadbTableName +
				" WHERE id > ?" +
				" ORDER BY id" +
				" LIMIT "+batchSize)) {

			String folioGoLiveDate = "2021-07-01";

			cacheStmt.setString(1, cursor.toString());
			metadbStmt.setObject(1, cursor);

			try (ResultSet cacheRs = cacheStmt.executeQuery(); ResultSet metadbRs = metadbStmt.executeQuery()) {
				boolean endOfCache = !cacheRs.next();
				boolean endOfMetadb = !metadbRs.next();
				if ( endOfMetadb || endOfCache ) {
					if ( ! endOfMetadb )
						queueUpdateToCache(inventory, (UUID)metadbRs.getObject("id"), type, Cause.ADDED);
					if ( ! endOfCache )
						queueUpdateToCache(inventory, UUID.fromString(cacheRs.getString("id")), type, Cause.DELETED);
					return cursor;
				}

				while (true) {
					UUID mId = (UUID)metadbRs.getObject("id");
					UUID cId = UUID.fromString(cacheRs.getString("id"));

					switch ( mId.compareTo(cId) ) {
					case 0: // match

						cursor = mId;
						String mDate = metadbRs.getString("moddate");
						String cDate = cacheRs.getString("moddate");
						if (cDate == null) {
							// pre go-live records were loaded to cache without timestamps
							// undated record in cache may be outdated if moddate is after go-live
							if (mDate.substring(0,10).compareTo(folioGoLiveDate) > 0) {
								queueUpdateToCache(inventory, mId, type, Cause.UPDATED);
							}
						} else if ((mDate.substring(0,23).compareTo(cDate.substring(0,23)) > 0)
								&& (mDate.substring(0,10).compareTo(folioGoLiveDate) > 0))
							queueUpdateToCache(inventory, mId, type, Cause.UPDATED);
						if (!cacheRs.next() || ! metadbRs.next()) return cursor;
						break;

					case -1: // mId < cId, so mId appears to be missing from cache

						queueUpdateToCache(inventory, mId, type, Cause.ADDED);
						cursor = mId;
						if (! metadbRs.next()) return cursor;
						break;

					case 1: // mId > cId, so cId not in metadb - deleted from folio?

						queueUpdateToCache(inventory, cId, type, Cause.DELETED);
						cursor = cId;
						if (! cacheRs.next()) return cursor;
						break;
	
					}
				}
			}
		}
	}
}
