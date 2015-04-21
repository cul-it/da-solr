package edu.cornell.library.integration.marcXmlToRdf;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Scanner;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.ilcommons.util.FileNameUtils;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Mode;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.OutputFormat;

public class VoyagerFull {
	
	DavService davService;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {		
	    new VoyagerFull(args);			
	}
	
	public VoyagerFull(String[] args) throws Exception {
		
		Collection<String> requiredFields = new HashSet<String>();
		requiredFields.add("fullXmlBibDir");
		requiredFields.add("fullXmlMfhdDir");
		requiredFields.add("n3Dir");
		requiredFields.add("dailyBibUnsuppressedDir");
		requiredFields.add("dailyBibUnsuppressedFilenamePrefix");
		requiredFields.add("dailyMfhdUnsuppressedDir");
		requiredFields.add("dailyMfhdUnsuppressedFilenamePrefix");
		SolrBuildConfig config =
				SolrBuildConfig.loadConfig( args, requiredFields );
		
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
		
		MarcXmlToRdf converter = new MarcXmlToRdf(Mode.ID_RANGE_BATCHES);
		converter.setOutputFormatWithoutSimultaneousWrite(OutputFormat.N3_GZ);
		converter.setBibSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getFullXmlBibDir(), davService);
		converter.setMfhdSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getFullXmlMfhdDir(), davService);
		converter.setDestDavDir(config.getWebdavBaseUrl() + "/" + config.getN3Dir(),davService);
		converter.setUnsuppressedBibs(unsuppressedBibs, true, false);
		converter.setUnsuppressedMfhds(unsuppressedMfhds, true, false);
		converter.setDestFilenamePrefix( "voyager" );
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
