package edu.cornell.library.integration.catalog;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.utilities.Config;

public class Catalog {

	public static DownloadMARC getMarcDownloader( Config config ) {
		try {
			Class c = Class.forName(config.getCatalogClass()+".DownloadMARC");
			DownloadMARC downloader = (DownloadMARC) c.newInstance();
			downloader.setConfig(config);
			return downloader;
		} catch ( ReflectiveOperationException e ) {
			System.out.printf("ERROR: Configured catalog class %s.DownloadMARC invalid.\n",config.getCatalogClass());
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	public static Locations getLocations( Config config ) throws SQLException {
		try {
			Class c = Class.forName(config.getCatalogClass()+".Locations");
			Locations locations = (Locations) c.newInstance();
			locations.loadLocations(config);
			return locations;
		} catch ( ReflectiveOperationException e ) {
			System.out.printf("ERROR: Configured catalog class %s.Locations invalid.\n",config.getCatalogClass());
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	public interface DownloadMARC {
		public MarcRecord getMarc( RecordType type, Integer id ) throws SQLException, IOException, InterruptedException;
		public void setConfig( Config config );
		public List<MarcRecord> retrieveRecordsByIdRange (RecordType type, Integer from, Integer to)
				throws SQLException, IOException;
	}

	public interface Locations {
		public void loadLocations( final Config config ) throws SQLException;
		public Location getByCode( final String code );
		public Location getById ( final Object id );
	}

	public interface Location {
		@Override public String toString();
		public String getLibrary();
		public String getName();
		public String getCode();
	}

}
