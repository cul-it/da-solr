package edu.cornell.library.integration.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

@ContextConfiguration(locations={"classpath:test-spring.xml"})
public class CatalogDaoTestGetBibUpdates extends AbstractJUnit4SpringContextTests {
   
   @Test
   @Ignore
   public void testContext() {
      Assert.assertNotNull(applicationContext.getBean("catalogDao"));
   }
   

   
  
   @Test
   public void testGetUpdatedBibIdsUsingDateRange() {
      System.out.println("testGetBibData");
      CatalogDao catalogDao = (CatalogDao) applicationContext.getBean("catalogDao");
      Calendar now = Calendar.getInstance();
      String toDate = getDateString(now);
      String fromDate = getRelativeDateString(now, -24);
      System.out.println("fromDate: "+ fromDate);
      System.out.println("toDate: "+ toDate);
      System.out.println("Getting update BibIds");
      List<String> extraMfhdIdList = new ArrayList<String>();
      try {
    	  List<String>  bibList = catalogDao.getUpdatedBibIdsUsingDateRange(fromDate, toDate);
    	  System.out.println("found this many bib ids: "+ bibList.size());
          for (String bibid: bibList) {
          	  System.out.println("bibid: "+bibid);
          	  extraMfhdIdList = catalogDao.getMfhdIdsByBibId(bibid);
          	  if (extraMfhdIdList == null) {
          		  System.out.println("no mfhds found");
          	  } else {
                 for (String s2: extraMfhdIdList) {
           	         System.out.println("\tmfhdid: "+ s2);
                 }
          	  }
          }
      } catch (Exception e) { 
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      System.out.println("Done.");
   }
   
   protected String getDateString(Calendar cal) {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	   String ds = df.format(cal.getTime());
	   return ds;
   }
   
   protected String getRelativeDateString(Calendar cal, int offset) {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	   Calendar offsetCal = cal;
	   offsetCal.add(Calendar.HOUR, offset);
	   String ds = df.format(offsetCal.getTime());
	   return ds;
   }

}
