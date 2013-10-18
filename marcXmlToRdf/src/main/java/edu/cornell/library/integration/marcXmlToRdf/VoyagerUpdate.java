package edu.cornell.library.integration.marcXmlToRdf;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Scanner;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.ilcommons.util.FileNameUtils;

public class VoyagerUpdate {
	
	DavService davService;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {		
	    new VoyagerUpdate(args);			
	}
	
	public VoyagerUpdate(String[] args) throws Exception {
		
		VoyagerToSolrConfiguration config =
				VoyagerToSolrConfiguration.loadConfig( args );
		
		davService = DavServiceFactory.getDavService(config);				
		
		Collection<Integer> unsuppressedBibs;
		try{
		    String mostRecentBibFile = FileNameUtils.findMostRecentUnsuppressedBibIdFile(config, davService);
		    System.out.println("Most recent file of BIB IDs identified as: "+ mostRecentBibFile);
		    unsuppressedBibs = getIdsFromFile( mostRecentBibFile );
		}catch(Exception e){
		    throw new Exception("Could not get most recent BIB ID file", e);
		}
		
		Collection<Integer> unsuppressedMfhds = new HashSet<Integer>();
		try{
		    String mostRecentMfhdFile = FileNameUtils.findMostRecentUnsuppressedMfhdIdFile(config, davService);
		    System.out.println("Most recent file of MFHD IDs identified as: "+ mostRecentMfhdFile);		    
		    unsuppressedMfhds = getIdsFromFile( mostRecentMfhdFile );
		}catch(Exception e){
		    throw new Exception("Could not be the most recent MFHD ID file", e);
		}			
		
		String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
				
		File tempFile = File.createTempFile("VoyagerDaily_"+currentDate+"_", ".nt.gz");
		tempFile.deleteOnExit();
		System.out.println("Temp XML file : " + tempFile.getAbsolutePath());
		
		try{
		    MarcXmlToNTriples.marcXmlToNTriples(unsuppressedBibs,
				                            unsuppressedMfhds, 
											davService, 
											config.getWebdavBaseUrl() + "/" + config.getDailyBibMrcXmlDir(),
											config.getWebdavBaseUrl() + "/" + config.getDailyMfhdMrcXmlDir(),
				                            tempFile);
		}catch (Exception e){
		    throw new Exception("Problems while converting to N-Triples",e);
		}
			
		String targetNTFile = config.getWebdavBaseUrl() + "/" + config.getDailyMrcNtDir()  
                + "/" + config.getDailyMrcNtFilenamePrefix() + "-" + currentDate+".nt.gz";
		
	    InputStream is = new FileInputStream(tempFile);
	    davService.saveFile(targetNTFile, is);						
		System.out.println("MARC N-Triples file saved to " + targetNTFile);		
	}
	


	private Collection<Integer> getIdsFromFile(String idFileUrl ) throws Exception{
	    Path file = null;
	    Collection<Integer> idList = new HashSet<Integer>();
	    	    
        if (idFileUrl != null) {            
            file = davService.getNioPath( idFileUrl );
        }else{
            throw new Exception("No file found at " + idFileUrl);
        }
        
       
        Scanner scanner = new Scanner(file,StandardCharsets.UTF_8.name());
        while (scanner.hasNextLine()) {
            String id = scanner.nextLine();
            idList.add(Integer.valueOf(id));
        }
        scanner.close();        
        
        return idList;

	}

}
