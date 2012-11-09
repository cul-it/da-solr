package edu.cornell.library.integration.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;

import edu.cornell.library.integration.config.IntegrationDataProperties;

public class DavServiceImpl implements DavService {
   
   private IntegrationDataProperties integrationDataProperties;

   public DavServiceImpl() {
      // TODO Auto-generated constructor stub
   }

   /**
    * @return the integrationDataProperties
    */
   public IntegrationDataProperties getIntegrationDataProperties() {
      return integrationDataProperties;
   }

   /**
    * @param integrationDataProperties the integrationDataProperties to set
    */
   public void setIntegrationDataProperties(
         IntegrationDataProperties integrationDataProperties) {
      this.integrationDataProperties = integrationDataProperties;
   }

   public List<String> getFileList(String pathProp) throws IOException {
      List<String> filelist = new ArrayList<String>();
      Sardine sardine = SardineFactory.begin(integrationDataProperties.getDavUser(), integrationDataProperties.getDavPass());
      List<DavResource> resources = sardine.list(pathProp);
      for (DavResource res : resources) {
          filelist.add(res.toString());
      }
      return filelist;
   }

}
