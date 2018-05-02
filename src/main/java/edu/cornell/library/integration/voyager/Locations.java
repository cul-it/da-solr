package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.utilities.IndexingUtilities;

/**
 * Give access to location data by code or number. Will load loaded into memory from the Voyager
 * database on the first instantiation of the class.
 */
public final class Locations {

	/**
	 * Retrieve Location object based on <b>code</b>. The value will already have been loaded
	 * into memory. The method cannot be called statically to ensure that the instantiation has
	 * been able to load the data.
	 * @param code
	 * @return Location
	 */
	@SuppressWarnings("static-method")
	public final Location getByCode( final String code ) {
		if (_byCode.containsKey(code))
			return _byCode.get(code);
		return null;
	}
	/**
	 * Retrieve Location object based on <b>number</b>. The value will already have been loaded
	 * into memory. The method cannot be called statically to ensure that the instantiation has
	 * been able to load the data.
	 * @param number
	 * @return Location
	 */
	@SuppressWarnings("static-method")
	public final Location getByNumber( final int number ) {
		if (_byNumber.containsKey(number))
			return _byNumber.get(number);
		return null;
	}

	/**
	 * Structure containing values relating to holdings location. <b>Name</b> and <b>library</b>
	 * may potentially be null.<br/><br/>
	 * <b>Fields</b><hr/>
	 * <dl>
	 *  <dt>code</dt><dd>Location code, as appears in holdings 852$b, e.g. "fine,res"</dd>
	 *  <dt>number</dt><dd>Location number, as appears in item record, e.g. 33</dd>
	 *  <dt>name</dt><dd>Location name, e.g. "Fine Arts Library Reserve"</dd>
	 *  <dt>library</dt><dd>Unit Library name, e.g. "Fine Arts Library"</dd>
	 * </dl>
	 * <b>Methods</b><hr/>
	 * <dl>
	 *  <dt>toString()</dt><dd>Returns display value of Location, primarily for diagnostic use,
	 *     e.g. "code: fine,res; number: 33; name: Fine Arts Library Reserve; library: Fine Arts Library"</dd>
	 *  <dt>equals( Location other )</dt><dd>returns <b>true</b> if this.number == other.number; else <b>false</b></dd>
	 *  <dt>compareTo( Location other )</dt><dd>returns this.number.compareTo(other.number)</dd>
	 * </dl>
	 */
	public static class Location implements Comparable<Location>{
		public final String code;
		public final Integer number;
		public final String name;
		public final String library;
		/**
		 * @return
		 *  Display value of Location, primarily for diagnostic use,
		 *     e.g. "code: fine,res; number: 33; name: Fine Arts Library Reserve; library: Fine Arts Library"
		 */
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("code: ").append(this.code);
			sb.append("; number: ").append(this.number);
			sb.append("; name: ").append(this.name);
			sb.append("; library: ").append(this.library);
			return sb.toString();
		}
		/**
		 * @param other
		 * @return
		 *    <b>true</b> if this.number == other.number; else <b>false</b>
		 */
		@Override
		public boolean equals( final Object other ) {
			if (other == null) return false;
			if (! this.getClass().equals(other.getClass())) return false;
			return (((Location)other).number.equals(this.number));
		}
		@Override
		public int hashCode() {
			return this.number.hashCode();
		}

		/**
		 * @param other
		 * @return
		 *    this.number.compareTo(other.number)
		 */
		@Override
		public int compareTo( final Location other ) {
			return this.number.compareTo(other.number);
		}
		private Location (String code, Integer number, String name, String library) {
			this.code = code;
			this.number = number;
			this.name = name;
			this.library = library;
		}
	}

	/**
	 * Give access to location data by code or number. If this is the first time Locations has
	 * been instantiated in the current process, instantiation will attempt to connect to Voyager
	 * to retrieve and index the data. Otherwise, the instance will simply give access to the
	 * already loaded data.
	 */
	public Locations(final SolrBuildConfig config) throws ClassNotFoundException, SQLException {

		if (_byCode.isEmpty())
			populateLocationMaps(config);
	}

// PRIVATE RESOURCES

	private static final Map<String,Location> _byCode = new HashMap<>();
	private static final Map<Integer,Location> _byNumber = new HashMap<>();

	private static final String getLocationsQuery =
			"SELECT "
			+ "LOCATION.LOCATION_CODE, "
			+ "LOCATION.LOCATION_ID, "
			+ "LOCATION.LOCATION_DISPLAY_NAME, "
			+ "LOCATION.LOCATION_NAME "
			+"FROM LOCATION ";

	private static void populateLocationMaps(final SolrBuildConfig config)
			throws ClassNotFoundException, SQLException {
		System.out.println("Retrieving location data from Voyager");
		libraryPatterns = IndexingUtilities.loadPatternMap("library_names.txt");

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
		String lcName = name.toLowerCase();
		Iterator<String> i = libraryPatterns.keySet().iterator();
		while (i.hasNext()) {
			String pattern = i.next();
			if (lcName.contains(pattern))
				return libraryPatterns.get(pattern);
		}
		return null;
	}
	static Map<String,String> libraryPatterns = null;

	public static void main( String[] args ) throws Exception {

		// Execution synopsis
		Collection<String> requiredFields = SolrBuildConfig.getRequiredArgsForDB("Voy");
		Locations locations = new Locations( SolrBuildConfig.loadConfig(args,requiredFields) );
		Location l = locations.getByCode("fine,res");
		System.out.println(l.toString());
		l = locations.getByNumber(33);
		System.out.println(l.toString());

		// Data dump
		System.out.println(libraryPatterns.size()+" patterns");
		for (String pattern : libraryPatterns.keySet()) {
			System.out.println(pattern+" => "+libraryPatterns.get(pattern));
		}
		System.out.println(_byNumber.size()+" locations");
		List<Integer> locNumbers = new ArrayList<>(_byNumber.keySet());
		Collections.sort(locNumbers);
		for (Integer locNumber : locNumbers)
			System.out.println(locations.getByNumber(locNumber).toString());
	}
}
