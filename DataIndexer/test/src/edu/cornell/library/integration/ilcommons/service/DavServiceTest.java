package edu.cornell.library.integration.ilcommons.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.*;
import org.junit.Test; 

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
 

 
public class DavServiceTest {
   
   @Test
   public void testDaveServiceFactory() {
      DavService davService = DavServiceFactory.getDavService();
      assertNotNull(davService); 
      
   }
   
   @Test
   public void testGetFileList() {
      System.out.println("\ntestGetFileList\n");
      DavService davService = DavServiceFactory.getDavService();
       
      String url = "http://jaf30-dev.library.cornell.edu/data/test/test.txt";
      try {
         List<String> filelist = davService.getFileList(url);
         for (String s : filelist) {
            System.out.println(s);
         }
      } catch (Exception e) { 
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
   
   @Test
   public void testGetFileUrlList() {
      System.out.println("\ntestGetFileUrlList\n");
      DavService davService = DavServiceFactory.getDavService();
       
      String url = "http://jaf30-dev.library.cornell.edu/data/test/bib/bib.xml.full";
      try {
         List<String> filelist = davService.getFileUrlList(url);
         for (String s : filelist) {
            System.out.println(s);
         }
      } catch (Exception e) { 
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
   
   
   
   @Test
   public void testGetFile() {
      System.out.println("\ntestGetFile\n");
      DavService davService = DavServiceFactory.getDavService();
      String url = "http://jaf30-dev.library.cornell.edu/data/test/test.txt";
      try {
         String str = davService.getFileAsString(url);
         System.out.println("Returned len: "+ str.length());
         
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
   }
   
   @Test
   public void testSaveFile() {
      System.out.println("\ntestSaveFile\n");
      DavService davService = DavServiceFactory.getDavService();
      String url = "http://jaf30-dev.library.cornell.edu/data/test/test.txt";
      try {
         String testString = "This is a test";
         byte[] bytes = testString.getBytes("UTF-8");
         InputStream isr = new  ByteArrayInputStream(bytes);
         davService.saveFile(url, isr);          
         
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
   }
   
   @Test
   public void testSaveBytesToFile() {
      System.out.println("\ntestSaveBytesToFile\n");
      DavService davService = DavServiceFactory.getDavService();
      String url = "http://jaf30-dev.library.cornell.edu/data/test/test2.txt";
      try {
         String testString = "This is test2";
         byte[] bytes = testString.getBytes("UTF-8"); 
         davService.saveBytesToFile(url, bytes);          
         
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
   }

}
