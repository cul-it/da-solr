package edu.cornell.library.integration.dao;

import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import edu.cornell.library.integration.bo.BibMasterData;
import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.util.ObjectUtils;

@ContextConfiguration(locations={"classpath:test-spring.xml"})
public class CatalogDaoTest extends AbstractJUnit4SpringContextTests {
   
   @Test
   @Ignore
   public void testContext() {
      Assert.assertNotNull(applicationContext.getBean("catalogDao"));
   }
   
   @Test
   @Ignore
   public void testGetSuppressedBibId() {
      CatalogDao catalogDao = (CatalogDao) applicationContext.getBean("catalogDao");
      String fromDateString = "2013-01-01 00:00:00";
      String toDateString = "2013-01-31 23:59:00";
      try {
         List<String> bibIdlist = catalogDao.getSuppressedBibId(fromDateString, toDateString);
         int cnt = 1;
         for (String s : bibIdlist) {
            System.out.println(s);
            cnt++;
         }
         System.out.println("Number of records found: "+cnt);
      } catch (Exception e) { 
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
   
   @Test
   @Ignore
   public void testGetSuppressedMfhdId() {
      CatalogDao catalogDao = (CatalogDao) applicationContext.getBean("catalogDao");
      String fromDateString = "2013-01-01 00:00:00";
      String toDateString = "2013-01-31 23:59:00";
      int cnt = 1;
      try {
         List<String> mfhdIdlist = catalogDao.getSuppressedMfhdId(fromDateString, toDateString);        
         for (String s : mfhdIdlist) {
            System.out.println(s);
            cnt++;
         }
         System.out.println("Number of records found: "+cnt);
      } catch (Exception e) { 
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
   
   @Test
   public void testGetBibMasterData() {
      System.out.println("testGetBibMasterData");
      CatalogDao catalogDao = (CatalogDao) applicationContext.getBean("catalogDao");
      String bibid = "6100933"; 
      
      try {
         BibMasterData bibMasterData = catalogDao.getBibMasterData(bibid);        
         ObjectUtils.printBusinessObject(bibMasterData);
      } catch (Exception e) { 
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }

}
