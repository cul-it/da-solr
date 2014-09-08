package edu.cornell.library.integration.ilcommons.service;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.junit.Test;

 
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
       
      String url = "http://culdatadev.library.cornell.edu/data/voyager/bib/bib.mrc.updates";
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
       
      String url = "http://culdatadev.library.cornell.edu/data/voyager/bib/bib.mrc.updates";
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
      String url = "http://culdatadev.library.cornell.edu/data/test/test.txt";
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
      String url = "http://culdatadev.library.cornell.edu/data/test/test.txt";
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
      String url = "http://culdatadev.library.cornell.edu/data/test/test2.txt";
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
