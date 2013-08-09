package edu.cornell.library.integration.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.marc4j.marc.Record;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

 

@ContextConfiguration(locations={"classpath:test-spring.xml"})
public class CatalogDaoTestGetMfhdUpdates extends AbstractJUnit4SpringContextTests {
   
   @Test
   @Ignore
   public void testContext() {
      Assert.assertNotNull(applicationContext.getBean("catalogDao"));
   }
   

   
  
   @Test
   public void testGetUpdatedMfhdIdsUsingDateRange() {
      System.out.println("testGetMfhdData");
      CatalogDao catalogDao = (CatalogDao) applicationContext.getBean("catalogDao");
      Calendar now = Calendar.getInstance();
      String toDate = getDateString(now);
      String fromDate = getRelativeDateString(now, -24);
      System.out.println("fromDate: "+ fromDate);
      System.out.println("toDate: "+ toDate);
      
      List<String> extraBibIdList = new ArrayList<String>();
      try {
    	  List<String>  mfhdList = catalogDao.getUpdatedMfhdIdsUsingDateRange(fromDate, toDate);
    	  System.out.println("found this many mfhd ids: "+ mfhdList.size());
          for (String s: mfhdList) {
             System.out.println("mfhdid: "+s);
             extraBibIdList = catalogDao.getBibIdsByMfhdId(s); 
             if (extraBibIdList == null) {
            	 System.out.println("no bibids found");
             } else {
                for (String s2: extraBibIdList) {
            	   System.out.println("\tbibid: "+ s2);
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
