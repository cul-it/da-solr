package edu.cornell.library.integration.metadb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import edu.cornell.library.integration.metadb.Utils.Cause;
import edu.cornell.library.integration.metadb.Utils.FolioType;
import edu.cornell.library.integration.utilities.Config;
import static edu.cornell.library.integration.metadb.Utils.getUpdateCursor;
import static edu.cornell.library.integration.metadb.Utils.queueUpdateToCache;



public class UpdateDetection {

	private final static String CURSOR_NAME = "metadb_upd";

	public static void main(String[] args) throws SQLException, IOException, InterruptedException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("MetaDB");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Current"));
		Config config = Config.loadConfig(requiredArgs);
		try (Connection metadb = config.getDatabaseConnection("MetaDB");
			 Connection inventory = config.getDatabaseConnection("Current");) {

			Timestamp cursor = getUpdateCursor(inventory, CURSOR_NAME);

			while (true) {
				Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-60_000);//now minus 60 seconds
				for (FolioType t : EnumSet.of(FolioType.INSTANCE, FolioType.HOLDING, FolioType.ITEM)) {
					System.out.println(t.name());
					List<UUID> ids = getUpdated(metadb, t,  cursor);
					System.out.println(ids.size());
					for (UUID id : ids)
						queueUpdateToCache(inventory, id, t, Cause.UPDATED);
				}
				cursor = newTime;
				putNewCursorToDB( inventory, cursor );
				Thread.sleep(30_000); //30 seconds
			}
		}

	}

	private static void putNewCursorToDB(Connection inventory, Timestamp cursor) throws SQLException {
		try (PreparedStatement stmt = inventory.prepareStatement(
				"UPDATE updateCursor SET current_to_date = ? WHERE cursor_name = ?")) {
			stmt.setTimestamp(1, cursor);
			stmt.setString(2, CURSOR_NAME);
			stmt.executeUpdate();
		}
	}

	private static List<UUID> getUpdated(Connection metadb, FolioType t, Timestamp since) throws SQLException {
		List<UUID> ids = new ArrayList<>();
		try (PreparedStatement stmt = metadb.prepareStatement(
				"SELECT id"+
				"  FROM " +t.metadbTableName +
				" WHERE __start > ?")) {
			stmt.setTimestamp(1, since);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next())
					ids.add((UUID)rs.getObject("id"));
			}
			
		}
		return ids;
	}


}
