package edu.cornell.library.integration.service;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

@ContextConfiguration(locations={"classpath:spring.xml"})
public class HttpServiceTest extends AbstractJUnit4SpringContextTests {
   
   @Test
   public void testContext() {
      Assert.assertNotNull(applicationContext.getBean("httpService"));
   }
   
   @Test
   public void testGetData() {
      HttpService service = (HttpService) applicationContext.getBean("httpService");
      try {
         String results = service.getData("http://culsearchdev.library.cornell.edu/data/voyager/bib/bib.mrc.full");
         System.out.println(results);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }

}
