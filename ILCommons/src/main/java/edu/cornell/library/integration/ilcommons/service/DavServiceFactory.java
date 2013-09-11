package edu.cornell.library.integration.ilcommons.service;
 
import java.io.IOException; 
import java.util.Properties;

import edu.cornell.library.integration.ilcommons.util.ClassPathPropertyLoader;

public class DavServiceFactory {
   
   
   public static DavService getDavService() {
      Properties props = null;
      ClassPathPropertyLoader loader = new ClassPathPropertyLoader();
      try {
         props = loader.load("dav.properties");
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      return new DavServiceImpl(props.getProperty("dav_user"), props.getProperty("dav_pass"));
   } 
   
    
}
