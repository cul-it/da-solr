package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class VoyagerUtilities {

	public static Timestamp confirmBibRecordActive( Connection voyager, Integer bibId ) throws SQLException {
		try ( PreparedStatement pstmt = voyager.prepareStatement
				("SELECT create_date, update_date, suppress_in_opac"
				+ " FROM bib_master "
				+ "WHERE bib_id = ?")) {
			pstmt.setInt(1, bibId);
			try ( ResultSet rs = pstmt.executeQuery()) {
				if ( ! rs.next())	               return null; // deleted
				if ( ! rs.getString(3).equals("N")) return null; // suppressed

				Timestamp mod_date = rs.getTimestamp(2);
				if (mod_date == null)
					mod_date = rs.getTimestamp(1);
				return mod_date;
			}
		}
	}


	public static Map<Integer,Timestamp> confirmActiveMfhdRecords( Connection voyager, Integer bibId ) throws SQLException {
		try ( PreparedStatement pstmt = voyager.prepareStatement
				("SELECT mfhd_master.mfhd_id, create_date, update_date"
				+ " FROM mfhd_master, bib_mfhd "
				+ "WHERE BIB_MFHD.MFHD_ID = mfhd_master.mfhd_id"
				+ "  AND bib_id = ?"
				+ "  AND suppress_in_opac = 'N'")) {
			pstmt.setInt(1, bibId);
			try ( ResultSet rs = pstmt.executeQuery()) {
				Map<Integer,Timestamp> mfhds = new HashMap<>();
				while (rs.next()) {
					Timestamp mod_date = rs.getTimestamp(3);
					if (mod_date == null)
						mod_date = rs.getTimestamp(2);
					mfhds.put(rs.getInt(1), mod_date);
				}
				return mfhds;
			}
		}
	}

}
