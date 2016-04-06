package edu.cornell.library.integration.marcXmlToRdf;

import java.util.Collection;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
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
		
		Collection<String> requiredFields = SolrBuildConfig.getRequiredArgsForWebdav();
		requiredFields.add("fullXmlBibDir");
		requiredFields.add("fullXmlMfhdDir");
		requiredFields.add("n3Dir");
		requiredFields.addAll( SolrBuildConfig.getRequiredArgsForDB("Current") );
		SolrBuildConfig config =
				SolrBuildConfig.loadConfig( args, requiredFields );
		
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
