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

import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.BibMasterData; 
import edu.cornell.library.integration.bo.MfhdData;
import edu.cornell.library.integration.util.ConvertUtils;
import edu.cornell.library.integration.util.ObjectUtils;

@ContextConfiguration(locations={"classpath:test-spring.xml"})
public class CatalogDaoTestGetBibMfhd extends AbstractJUnit4SpringContextTests {
   
   @Test
   @Ignore
   public void testContext() {
      Assert.assertNotNull(applicationContext.getBean("catalogDao"));
   }
   

   
  
   @Test
	public void testGetMfhdByBibId() {
		System.out.println("testGetMfhdByBibid");
		CatalogDao catalogDao = (CatalogDao) applicationContext
				.getBean("catalogDao");
		String bibid = "1419";
		try {

			List<String> mfhdList = catalogDao.getMfhdIdsByBibId(bibid);
			System.out.println("bibid: "+bibid);
			if ( mfhdList == null)  {
			   System.out.println("no mhfds found");	
			} else {
			   for (String s2 : mfhdList) {
				   System.out.println("\tmfhdid: " + s2);
			   }
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done.");
	}
   
    

}
