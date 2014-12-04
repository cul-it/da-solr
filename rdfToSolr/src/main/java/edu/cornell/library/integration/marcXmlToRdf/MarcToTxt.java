package edu.cornell.library.integration.marcXmlToRdf;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;

import org.apache.commons.io.FileUtils;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Mode;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.OutputFormat;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Report;

public class NonVoyagerToTxt {
	
	DavService davService;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {		
	    new NonVoyagerToTxt(args);			
	}
	
	public NonVoyagerToTxt(String[] args) throws Exception {
		
		Collection<String> requiredFields = new HashSet<String>();
	//	requiredFields.add("nonVoyIdPrefix"); only needed with reportlist
		requiredFields.add("xmlDir");
		requiredFields.add("txtDir");
		// optionalField : reportList
		VoyagerToSolrConfiguration config =
				VoyagerToSolrConfiguration.loadConfig( args, requiredFields );
		
		davService = DavServiceFactory.getDavService(config);

		MarcXmlToRdf converter = new MarcXmlToRdf(Mode.NAME_AS_SOURCE);
		converter.setOutputFormatWithoutSimultaneousWrite(OutputFormat.TXT_GZ);
		converter.setBibSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getXmlDir(), davService);
		converter.setDestDavDir(config.getWebdavBaseUrl() + "/" + config.getTxtDir(), davService);
		
		String reportList = config.getReportList();
		String[] reports = null;
		if (reportList != null) {
			reports = reportList.split("\\s+");
			for (String report : reports) {
				converter.addReport(Report.valueOf(report.toUpperCase(Locale.ENGLISH)));
			}
		}
		
		converter.run();
	
		if (reportList != null) {
			for (String report : reports) {
				String reportResult = converter.getReport(Report.valueOf(report));
				System.out.println(report);
				FileUtils.writeStringToFile(
						new File (config.getNonVoyIdPrefix() + "-"+ report + ".txt"),
						reportResult, "UTF-8", false);
			}
		}
		
	/*	// Upload N-Triples file
		System.out.println(tempFile.getAbsolutePath());
		String targetNTFile = config.getWebdavBaseUrl() + "/" + config.getDailyMrcNtDir()  
				+ "/" + config.getDailyMrcNtFilenamePrefix() + "-" + currentDate+"."+ String.valueOf(++fileCount)  +".nt.gz";
		InputStream is = new FileInputStream(file.toString());
		davService.saveFile(targetNTFile, is);						
		System.out.println("MARC N-Triples file saved to " + targetNTFile);
		Files.delete(file);
		*/
	}
	

}
