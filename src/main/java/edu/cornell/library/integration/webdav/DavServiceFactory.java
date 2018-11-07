package edu.cornell.library.integration.webdav;
 
import edu.cornell.library.integration.utilities.Config;

public class DavServiceFactory {

   public static DavService getDavService(Config config){
       DavService srvc = new DavServiceImpl(config.getWebdavUser(), config.getWebdavPassword());
       String localPath = config.getLocalBaseFilePath();
       if (localPath != null) {
    	   srvc.setDavUrlToLocalPathMapping(config.getWebdavBaseUrl(), localPath);
       }
       return srvc;
   }
    
}
