package edu.cornell.library.integration.marcXmlToRdf;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Locale;

import org.apache.commons.io.FileUtils;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Mode;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.OutputFormat;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Report;

public class RunExtractReport {
	
	DavService davService;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {		
	    new RunExtractReport(args);			
	}
	
	public RunExtractReport(String[] args) throws Exception {
		
		Collection<String> requiredFields = SolrBuildConfig.getRequiredArgsForWebdav();
		requiredFields.add("xmlDir");
		requiredFields.add("tdfDir");
		requiredFields.add("reportList");  // Exactly ONE EXTRACT_* report expected; addl ok.
		SolrBuildConfig config =
				SolrBuildConfig.loadConfig( args, requiredFields );
		
		davService = DavServiceFactory.getDavService(config);

		MarcXmlToRdf converter = new MarcXmlToRdf(Mode.NAME_AS_SOURCE);
		converter.setOutputFormatWithoutSimultaneousWrite(OutputFormat.TDF);
		converter.setBibSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getXmlDir(), davService);
		converter.setDestDavDir(config.getWebdavBaseUrl() + "/" + config.getTdfDir(), davService);
		
		String extractReport = null;
		
		String reportList = config.getReportList();
		String[] reports = null;
		if (reportList != null) {
			reports = reportList.split("\\s+");
			for (String report : reports) {
				converter.addReport(Report.valueOf(report.toUpperCase(Locale.ENGLISH)));
				if (report.toUpperCase().startsWith("EXTRACT_")) {
					if (extractReport == null)
						extractReport = report.toUpperCase();
					else {
						System.out.println("Error: only one extract report may be specified.");
						System.exit(1);
					}
				}
					
			}
		}
		
		if (extractReport == null) {
			System.out.println("Error: an extract report must be specified in properties file reportList directive.");
			System.exit(1);
		}
		
		converter.setDestFilenamePrefix(extractReport);
		converter.run();
	
		if (reports != null && reports.length > 0) {
			for (String report : reports) {
				String reportResult = converter.getReport(Report.valueOf(report));
				System.out.println(report);
				FileUtils.writeStringToFile(
						new File (config.getNonVoyIdPrefix() + "-"+ report + ".txt"),
						reportResult, StandardCharsets.UTF_8, false);
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
