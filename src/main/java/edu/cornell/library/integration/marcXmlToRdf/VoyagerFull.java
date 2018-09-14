package edu.cornell.library.integration.marcXmlToRdf;

import java.util.Collection;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Mode;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.OutputFormat;
import edu.cornell.library.integration.webdav.DavService;
import edu.cornell.library.integration.webdav.DavServiceFactory;

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
		
		Collection<String> requiredFields = Config.getRequiredArgsForWebdav();
		requiredFields.add("fullXmlBibDir");
		requiredFields.add("fullXmlMfhdDir");
		requiredFields.add("n3Dir");
		requiredFields.addAll( Config.getRequiredArgsForDB("Current") );
		Config config =
				Config.loadConfig( args, requiredFields );
		
		davService = DavServiceFactory.getDavService(config);				
		
		MarcXmlToRdf converter = new MarcXmlToRdf(Mode.ID_RANGE_BATCHES);
		converter.setDbForUnsuppressedIdFiltering(config.getDatabaseConnection("Current"));
		converter.setOutputFormatWithoutSimultaneousWrite(OutputFormat.N3_GZ);
		converter.setBibSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getFullXmlBibDir(), davService);
		converter.setMfhdSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getFullXmlMfhdDir(), davService);
		converter.setDestDavDir(config.getWebdavBaseUrl() + "/" + config.getN3Dir(),davService);
		converter.setDestFilenamePrefix( "voyager" );
		converter.run();

	}
}
