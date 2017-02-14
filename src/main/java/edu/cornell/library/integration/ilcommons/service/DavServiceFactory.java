package edu.cornell.library.integration.ilcommons.service;
 
import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

public class DavServiceFactory {

   public static DavService getDavService(SolrBuildConfig config){
       DavService srvc = new DavServiceImpl(config.getWebdavUser(), config.getWebdavPassword());
       String localPath = config.getLocalBaseFilePath();
       if (localPath != null) {
    	   srvc.setDavUrlToLocalPathMapping(config.getWebdavBaseUrl(), localPath);
       }
       return srvc;
   }
    
}
