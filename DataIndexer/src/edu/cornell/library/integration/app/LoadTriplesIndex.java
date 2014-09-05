package edu.cornell.library.integration.app;

 

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.tdb.TDBFactory;

public class LoadTriplesIndex {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 
  
   
   private Model model;
   private final String TDBDIR = "/usr/local/src/integrationlayer/tdbIndex"; 
   private final String holdingsIndexFileName = "/usr/local/src/integrationlayer/DataIndexer/holdingsIndexfile.nt"; 
   private final String bibIndexFileName = "/usr/local/src/integrationlayer/DataIndexer/bibIndexfile.nt";
   /**
    * default constructor
    */
   public LoadTriplesIndex() { 
       
   } 
   
   /**
    * @return the model
    */
   public Model getModel() {
      return model;
   }


   /**
    * @param model the model to set
    */
   public void setModel(Model model) {
      this.model = model;
   }


   /**
    * @param args
    */
   public static void main(String[] args) {
     LoadTriplesIndex app = new LoadTriplesIndex(); 
     app.run();
   }
   

   /**
    * 
    */
   public void run() {      
      
      FileInputStream fis = null;
      
      File holdingsIndexFile  = new File(holdingsIndexFileName);
      File bibIndexFile  = new File(bibIndexFileName);
      
      try {
         System.out.println("Removing old triple store");
         FileUtils.deleteDirectory(new File(TDBDIR));
      } catch (IOException e1) {
         // TODO Auto-generated catch block
         e1.printStackTrace();
      }
      
      System.out.println("Getting model");
      Dataset dataset = TDBFactory.createDataset(TDBDIR);
      Model model = dataset.getDefaultModel();
      
      System.out.println("Loading BIB index triples");
      try {
         fis = new FileInputStream(bibIndexFile);
         readTriples(model, fis);
      } catch (Exception ex) {
         ex.printStackTrace();
      } finally {
         try {
            fis.close();
         } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
      
      System.out.println("Loading MFHD index triples");
      try {
         fis = new FileInputStream(holdingsIndexFile);
         readTriples(model, fis);
      } catch (Exception ex) {
         ex.printStackTrace();
      } finally {
         try {
            fis.close(); 
         } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
      model.close();
      
      
      
   } 
   
   protected void readTriples(Model model, InputStream in) {    
     System.out.println("Reading triples...");
     model.read(in, null, "N-TRIPLE"); 
   }
}
