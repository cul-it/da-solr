package edu.cornell.library.integration.ilcommons.util;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
 
public class ClassPathPropertyLoader {

   /**
    * load the properties from the file
    * and returns a Properties Object
    * @return
    * @throws IOException
    */
   public Properties load(String propertyFileName) throws IOException {

      // get the input stream of the properties file
      InputStream in = ClassPathPropertyLoader.class.getClassLoader()
            .getResourceAsStream(propertyFileName);
      
      // error in case file is not found 
      if (in == null) {
         throw new IOException("Could not find file named " + propertyFileName + " on the classpath.");
      }
      
      // create the properties object and load in
      Properties props = new java.util.Properties();
      props.load(in);

      return props;
   }
   
   /**
    * Entry point, Test method
    * @param args
    * @throws Exception
    */
   public static final void main(String args[]) throws Exception {
      
      String propertyFileName = "config.properties";
      
      // create instance and load it
      ClassPathPropertyLoader loader = new ClassPathPropertyLoader();
      Properties properties = loader.load(propertyFileName);
      
      // print out properties
      System.out.print(properties.toString());
      
   }

}


