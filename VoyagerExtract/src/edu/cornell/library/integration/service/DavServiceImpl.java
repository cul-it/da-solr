package edu.cornell.library.integration.service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;

import edu.cornell.library.integration.config.IntegrationDataProperties;

public class DavServiceImpl implements DavService {
   
   private String davUser;
   private String davPass;

   public DavServiceImpl() {
      // TODO Auto-generated constructor stub
   }

   /**
    * @return the davUser
    */
   public String getDavUser() {
      return this.davUser;
   }

   /**
    * @param davUser the davUser to set
    */
   public void setDavUser(String davUser) {
      this.davUser = davUser;
   }


   /**
    * @return the davPass
    */
   public String getDavPass() {
      return this.davPass;
   }

   /**
    * @param davPass the davPass to set
    */
   public void setDavPass(String davPass) {
      this.davPass = davPass;
   }

   public List<String> getFileList(String url) throws IOException {
      List<String> filelist = new ArrayList<String>();
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      List<DavResource> resources = sardine.list(url);
      for (DavResource res : resources) {
          filelist.add(res.getPath());
      }
      return filelist;
   }

   public String getFileAsString(String url) throws IOException {
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      InputStream istream = sardine.get(url);
      String str = convertStreamToString(istream);
      return str;
   } 

   public void saveFile(String url, InputStream dataStream) throws IOException {
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      sardine.put(url, dataStream);
   }
   
   protected String convertStreamToString(InputStream is)
         throws IOException {
     //
     // To convert the InputStream to String we use the
     // Reader.read(char[] buffer) method. We iterate until the
     // Reader return -1 which means there's no more data to
     // read. We use the StringWriter class to produce the string.
     //
     if (is != null) {
         Writer writer = new StringWriter();

         char[] buffer = new char[1024];
         try {
             Reader reader = new BufferedReader(
                     new InputStreamReader(is, "UTF-8"));
             int n;
             while ((n = reader.read(buffer)) != -1) {
                 writer.write(buffer, 0, n);
             }
         } finally {
             is.close();
         }
         return writer.toString();
     } else {       
         return "";
     }
 }
}
