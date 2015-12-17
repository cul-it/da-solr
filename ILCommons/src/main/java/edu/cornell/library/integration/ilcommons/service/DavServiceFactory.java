package edu.cornell.library.integration.ilcommons.service;
 
import java.io.IOException;
import java.util.Properties;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.utilities.ClassPathPropertyLoader;

public class DavServiceFactory {
   
   @Deprecated
   public static DavService getDavService() {
      Properties props = null;
      ClassPathPropertyLoader loader = new ClassPathPropertyLoader();
      
      try {
         props = loader.load("dav.properties");
      } catch (IOException e) {
          throw new RuntimeException("Could not load dav.properties from classpath", e);
      }
      
      return new DavServiceImpl(props.getProperty("dav_user"), props.getProperty("dav_pass"));
   } 
   
   public static DavService getDavService(SolrBuildConfig config){
       DavService srvc = new DavServiceImpl(config.getWebdavUser(), config.getWebdavPassword());
       String localPath = config.getLocalBaseFilePath();
       if (localPath != null) {
    	   srvc.setDavUrlToLocalPathMapping(config.getWebdavBaseUrl(), localPath);
       }
       return srvc;
   }
    
}
