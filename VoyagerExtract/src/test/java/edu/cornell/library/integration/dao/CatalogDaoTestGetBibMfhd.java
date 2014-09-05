package edu.cornell.library.integration.dao;

import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

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
