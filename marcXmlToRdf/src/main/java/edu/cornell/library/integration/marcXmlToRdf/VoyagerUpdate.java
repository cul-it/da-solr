package edu.cornell.library.integration.marcXmlToRdf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Collection;
import java.util.HashSet;
import java.util.Scanner;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.indexer.utilies.IndexingUtilities;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToNTriples;

public class VoyagerUpdate {

	private String davUrl = null;

	DavService davService;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			new VoyagerUpdate(args);
		} catch (Exception e) {
			e.printStackTrace();
			
		}
		
	}
	
	public VoyagerUpdate(String[] args) throws Exception {
		
		VoyagerToSolrConfiguration config =
				VoyagerToSolrConfiguration.loadConfig( args );
		davService = DavServiceFactory.getDavService(config);
		davUrl = config.getWebdavBaseUrl();
		Path currentVoyagerBibList = null;
		Path currentVoyagerMfhdList = null;
		Collection<Integer> unsuppressedBibs = new HashSet<Integer>();
		Collection<Integer> unsuppressedMfhds = new HashSet<Integer>();
		String mostRecentBibFile = findMostRecentBibFile(config,davService, davUrl);		
		try{
			if (mostRecentBibFile != null) {
				System.out.println("Most recent bib file identified as: "+ mostRecentBibFile);
				currentVoyagerBibList = davService.getNioPath( mostRecentBibFile );
			}else{
			    System.out.println("No recent bib file found.");
			    System.exit(1);
			}
		} catch (Exception e) {
		    throw new Exception( "Could not get most recent bib file from '" + mostRecentBibFile +"'", e);
		}
		try {
			Scanner scanner = new Scanner(currentVoyagerBibList,StandardCharsets.UTF_8.name());
			while (scanner.hasNextLine()) {
				String id = scanner.nextLine();
				unsuppressedBibs.add(Integer.valueOf(id));
			}
			scanner.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		
		String mostRecentMfhdFile = findMostRecentMfhdFile(config,davService, davUrl);
		try {		
			if (mostRecentMfhdFile != null) {
				System.out.println("Most recent mfhd file identified as: " + mostRecentMfhdFile);
				currentVoyagerMfhdList = davService.getNioPath( mostRecentMfhdFile);
			}else{
			    System.out.println("No recent Mfhd holdings file found.");
                System.exit(1);
			}
		} catch (Exception e) {
			throw new Exception( "Could not get most recent Mfhd holding file '" + mostRecentMfhdFile + "'" , e);
		}
		try {
			Scanner scanner = new Scanner(currentVoyagerMfhdList,StandardCharsets.UTF_8.name());
			while (scanner.hasNextLine()) {
				String id = scanner.nextLine();
				unsuppressedMfhds.add(Integer.valueOf(id));
			}
			scanner.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		String targetNTFile = davUrl + config.getDailyMrcNtDir() +
				config.getDailyMrcNtFilenamePrefix() + currentDate+".nt.gz";
		
		File tempFile = File.createTempFile("VoyagerDaily_"+currentDate+"_", ".nt.gz");
		tempFile.deleteOnExit();
		System.out.println("Temp file : " + tempFile.getAbsolutePath());
		
		MarcXmlToNTriples.marcXmlToNTriples(unsuppressedBibs,
				                            unsuppressedMfhds, 
											davService, 
				                            davUrl + config.getDailyBibMrcXmlDir(),
				                            davUrl + config.getDailyMfhdMrcXmlDir(),
				                            tempFile);
		InputStream is = new FileInputStream(tempFile);
		davService.saveFile(targetNTFile, is);
		
		
		
	}
	
	public static String findMostRecentBibFile(VoyagerToSolrConfiguration config, 
			                    DavService davService , String davBaseUrl ) throws Exception{        
        try {
            return IndexingUtilities.findMostRecentFile( davService, davBaseUrl + config.getDailyBibUnsuppressedDir() , config.getDailyBibUnsuppressedFilenamePrefix());                    
        } catch (Exception cause) {
            throw new Exception("Could not find most recent bib file", cause);
        }        
    }

    public static String findMostRecentMfhdFile(VoyagerToSolrConfiguration config,
    		                    DavService davService, String davBaseUrl) throws Exception{                
        try {
            return IndexingUtilities.findMostRecentFile( davService, davBaseUrl + config.getDailyMfhdUnsuppressedDir(), config.getDailyMfhdUnsuppressedFilenamePrefix());            
        } catch (Exception e) {
            throw new Exception( "Could not get most recent Mfhd holding file.", e);
        }    
    }


}
