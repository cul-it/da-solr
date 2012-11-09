package edu.cornell.library.integration.service;

import java.io.IOException;
import java.net.ConnectException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 

public class HttpService {
   
   protected final Log logger = LogFactory.getLog(getClass());

   public HttpService() {
      // TODO Auto-generated constructor stub
   }
   
   public String getData(String urlString) throws Exception{
      GetMethod getMethod = new GetMethod(urlString);
      

      HttpClient client = new HttpClient();
      client.getHttpConnectionManager().getParams().setConnectionTimeout(5000);
      int status = 0;
      try {
         status = client.executeMethod(getMethod);
         System.out.println("Status: "+status);
         String results = getMethod.getResponseBodyAsString();
         return results;
      } catch (ConnectException e) {
         //displayHeaders(getMethod);
         logger.error("ConnectException ", e);
         throw e;
      } catch (HttpException e) {
         logger.error("HttpException ", e);
         throw e;
      } catch (IOException e) {
         logger.error("IOException ", e);
         throw e;
      } finally {
         getMethod.releaseConnection();
      }
   }

}
