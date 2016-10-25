package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.utilities.IndexingUtilities;

public final class Locations {
	SolrBuildConfig config;
	private static final Map<String,Location> _byCode = new HashMap<>();
	private static final Map<Integer,Location> _byNumber = new HashMap<>();

	@SuppressWarnings("static-method")
	public final Location getByCode( final String code ) {
		if (_byCode.containsKey(code))
			return _byCode.get(code);
		return null;
	}
	@SuppressWarnings("static-method")
	public final Location getByNumber( final int number ) {
		if (_byNumber.containsKey(number))
			return _byNumber.get(number);
		return null;
	}

	public static class Location implements Comparable<Location>{
		public final String code;
		public final Integer number;
		public final String name;
		public final String library;
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("code: ").append(this.code);
			sb.append("; number: ").append(this.number);
			sb.append("; name: ").append(this.name);
			sb.append("; library: ").append(this.library);
			return sb.toString();
		}
		public boolean equals( final Location other ) {
			if (other == null) return false;
			if (other.number == this.number) return true;
			return false;
		}
		@Override
		public int compareTo( final Location other ) {
			return this.number.compareTo(other.number);
		}
		public Location (String code, Integer number, String name, String library) {
			this.code = code;
			this.number = number;
			this.name = name;
			this.library = library;
		}
	}
	public Locations(final SolrBuildConfig config) throws ClassNotFoundException, SQLException {
		this.config = config;
		if (_byCode.isEmpty())
			populateLocationMaps();
	}

// PRIVATE METHODS

	private static final String getLocationsQuery =
			"SELECT "
			+ "LOCATION.LOCATION_CODE, "
			+ "LOCATION.LOCATION_ID, "
			+ "LOCATION.LOCATION_DISPLAY_NAME, "
			+ "LOCATION.LOCATION_NAME "
			+"FROM LOCATION ";
	private void populateLocationMaps() throws ClassNotFoundException, SQLException {
		System.out.println("Retrieving location data from Voyager");
		try ( Connection voyager = config.getDatabaseConnection("Voy");
				Statement stmt = voyager.createStatement();
				ResultSet rs = stmt.executeQuery(getLocationsQuery)) {

			while (rs.next()) {

				String name = rs.getString(3);
				if (name == null)
					name = rs.getString(4);
				Location l = new Location(rs.getString(1),rs.getInt(2),name,getLibrary(name));

				_byCode.put(l.code, l);
				_byNumber.put(l.number, l);
			}
			
		}
	}
	private static String getLibrary(String name) {
		if (name == null)
			return null;
		if (libraryPatterns == null)
			libraryPatterns = IndexingUtilities.loadPatternMap("library_names.txt");
		Iterator<String> i = libraryPatterns.keySet().iterator();
		while (i.hasNext()) {
			String pattern = i.next();
			if (name.contains(pattern))
				return libraryPatterns.get(pattern);
		}
		return null;
	}
	static Map<String,String> libraryPatterns = null;
}
