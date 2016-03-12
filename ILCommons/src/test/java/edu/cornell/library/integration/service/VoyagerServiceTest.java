package edu.cornell.library.integration.service;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.util.ObjectUtils;

@ContextConfiguration(locations={"classpath:test-spring.xml"})
public class VoyagerServiceTest extends AbstractJUnit4SpringContextTests {
   
   @Test
   public void testContext() {
      Assert.assertNotNull(applicationContext.getBean("catalogService"));
   }
   
   @Test
   public void testGetFileList() {
      CatalogService catalogService = (CatalogService) applicationContext.getBean("catalogService");
      try {
         List<Location> locationlist = catalogService.getAllLocation();
         for (Location location : locationlist) {
            ObjectUtils.printBusinessObject(location);
         }
      } catch (Exception e) { 
         e.printStackTrace();
      }
   }

}
