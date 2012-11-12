package edu.cornell.library.integration.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

@ContextConfiguration(locations={"classpath:spring.xml"})
public class DavServiceTest extends AbstractJUnit4SpringContextTests {
   
   @Test
   public void testContext() {
      Assert.assertNotNull(applicationContext.getBean("davService"));
   }
   
   @Test
   public void testGetFileList() {
      System.out.println("\ntestGetFileList\n");
      DavService davService = (DavService) applicationContext.getBean("davService");
       
      String url = "http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.mrc.daily";
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
   public void testGetFile() {
      System.out.println("\ntestGetFile\n");
      DavService davService = (DavService) applicationContext.getBean("davService");
      String url = "http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.mrc.daily/bib.daily.mrc";
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
      DavService davService = (DavService) applicationContext.getBean("davService");
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

}
