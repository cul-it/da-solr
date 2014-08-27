package edu.cornell.library.integration.marcXmlToRdf;

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
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToNTriples.Mode;

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
		
		Collection<String> requiredFields = new HashSet<String>();
		requiredFields.add("dailyBibMrcXmlDir");
		requiredFields.add("dailyMfhdMrcXmlDir");
		requiredFields.add("dailyMrcNtDir");
		requiredFields.add("dailyMrcNtFilenamePrefix");
		VoyagerToSolrConfiguration config =
				VoyagerToSolrConfiguration.loadConfig( args, requiredFields );
		
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
					    
		MarcXmlToNTriples converter = new MarcXmlToNTriples(Mode.RECORD_COUNT_BATCHES);
		converter.setBibSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyBibMrcXmlDir(), davService);
		converter.setMfhdSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMfhdMrcXmlDir(), davService);
		converter.setDestDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMrcNtDir(),davService);
		converter.setUnsuppressedBibs(unsuppressedBibs, true, false);
		converter.setUnsuppressedMfhds(unsuppressedMfhds, true, false);
		converter.setDestFilenamePrefix( config.getDailyMrcNtFilenamePrefix() + "-" + currentDate );
		converter.run();

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
