package edu.cornell.library.integration.indexer;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.marc.MarcRecord;

public class ConvertMarcToXml {

	/** Logger for this class and subclasses */
	protected final Log logger = LogFactory.getLog(getClass()); 

	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		Collection<String> requiredFields = Arrays.asList("marc2XmlDirs");
		Config config = Config.loadConfig(requiredFields);

		new ConvertMarcToXml(config);
	}

	/**
	 * default constructor
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public ConvertMarcToXml(Config config) throws FileNotFoundException, IOException { 

		String[] dirs = config.getMarc2XmlDirs();
		if (dirs == null) return;
		if (dirs.length % 2 != 0) { 
			System.out.println("marc2XmlDirs must be configured with an even number of paths in src/dest pairs.");
			return;
		}
		System.out.println("\nConvert MARC to MARC XML");
		for (int i = 0; i < dirs.length; i += 2) {
			convertDir(dirs[i], dirs[i+1]);
		}
	}

	private static void convertDir( String srcDir, String destDir) throws FileNotFoundException, IOException {

		// get list of daily mrc files
		File[] srcList = (new File(srcDir)).listFiles();
		if ( srcList == null ) {
			System.out.printf( "'%s' is not a valid directory.\n",srcDir);
			return;
		}
		if (srcList.length == 0) {
			System.out.printf("No files available to process in '%s'.\n",srcDir);
			return;
		}

		// iterate over mrc files
		for (File srcFile  : srcList) {
			if ( ! srcFile.getName().endsWith(".mrc") ) continue;
			System.out.println("Converting file: "+ srcFile);
			convertMrcToXml(srcFile, destDir);
		}

	}

	private static void convertMrcToXml(File srcFile, String destDir)
			throws FileNotFoundException, IOException {

		File destFile = new File ( destDir, srcFile.getName().replaceAll(".mrc$", ".xml") );

		try ( FileInputStream is = new FileInputStream(srcFile);
				FileOutputStream out = new FileOutputStream(destFile) ){

			MarcRecord.marcToXml(is, out);
		}
   }

}
