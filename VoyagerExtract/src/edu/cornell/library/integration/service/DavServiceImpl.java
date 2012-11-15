package edu.cornell.library.integration.service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
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
    * @param davUser
    * @param davPass
    */
   public DavServiceImpl(String davUser, String davPass) {
      this.davUser = davUser;
      this.davPass = davPass;
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

   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.DavService#getFileList(java.lang.String)
    */
   public List<String> getFileList(String url) throws IOException {
      List<String> filelist = new ArrayList<String>();
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      List<DavResource> resources = sardine.list(url);
      for (DavResource res : resources) {
         if (! res.isDirectory()) {
            filelist.add(res.getName());
         }
      }
      return filelist;
   }
   
   /**
    * @param url
    * @return
    * @throws IOException
    */
   // this does not seem to work
  /* public List<String> getDirectories(String url) throws IOException {
      List<String> dirList = new ArrayList<String>();
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      // sardine does not allow passing the depth as an string (i.e. infinity)...set it to 99
      List<DavResource> resources = sardine.list(url, 5); 
      for (DavResource res : resources) {
         if (res.isDirectory()) {
            dirList.add(res.getName());
         }
      }
      return dirList;
   }*/

   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.DavService#getFileAsString(java.lang.String)
    */
   public String getFileAsString(String url) throws IOException {
      System.out.println("Getting file: "+ url);
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      InputStream istream = sardine.get(url);
      String str = convertStreamToString(istream);
      return str;
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.DavService#getFileAsInputStream(java.lang.String)
    */
   public InputStream getFileAsInputStream(String url) throws IOException {
      System.out.println("Getting file: "+ url);
      Sardine sardine = SardineFactory.begin(getDavUser(), getDavPass());
      InputStream istream = (InputStream) sardine.get(url);
      return istream; 
   } 

   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.DavService#saveFile(java.lang.String, java.io.InputStream)
    */
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
