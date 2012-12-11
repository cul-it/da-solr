package edu.cornell.library.integration.app;

 
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
 
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;  
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext; 

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory; 
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Selector;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.tdb.TDBFactory;
 

import edu.cornell.library.integration.bo.Triple;
import edu.cornell.library.integration.service.DavService; 
import edu.cornell.library.integration.util.IterableAdaptor;

public class LookupBibId {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 
   private final String TDBDIR = "/usr/local/src/integrationlayer/tdbIndex";
   private final String uriNs = "http://fbw4-dev.library.cornell.edu/individuals";
   private final String dataNs = "http://culdata.library.cornell.edu/canonical/0.1/";
   private final String dataDevNs = "http://culdatadev.library.cornell.edu/canonical/0.1/";
   
   private Model jenaModel;
   private Dataset dataset;
   
   /**
    * default constructor
    */
   public LookupBibId() { 
       
   } 
   
   


   /**
    * @return the jenaModel
    */
   public Model getJenaModel() {
      return jenaModel;
   }




   /**
    * @param jenaModel the jenaModel to set
    */
   public void setJenaModel(Model jenaModel) {
      this.jenaModel = jenaModel;
   }




   /**
    * @return the dataset
    */
   public Dataset getDataset() {
      return dataset;
   }




   /**
    * @param dataset the dataset to set
    */
   public void setDataset(Dataset dataset) {
      this.dataset = dataset;
   }




   /**
    * @param args
    */
   public static void main(String[] args) {
     LookupBibId app = new LookupBibId();
     if (args.length != 1 ) {
        System.err.println("You must provide a bibid");
        System.exit(-1);
     }
     app.run(args[0]);
   }
   

   /**
    * 
    */
   public void run(String bibid) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");      
      
      //System.out.println("Getting model");
      setDataset(TDBFactory.createDataset(TDBDIR));
      setJenaModel(dataset.getDefaultModel());
      
      //System.out.println("Model size: "+ jenaModel.size());
      
      // get Resources with bibId
      String subject = "<" +uriNs + "/b" + bibid + ">"; 
      String hasFile = "<" +dataNs + "hasFile>";
      String hasBib = "<http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord>";
      String hasHoldings = "<" +dataNs + "hasFile>"; 
      String object = "<" +uriNs + "/b" + bibid + ">";
      
      
      String query = "" + "SELECT ?holding ?filename \n" 
            + "WHERE {\n" 
            + " ?holding "  + hasBib + " "+ object + " .\n"
            + " ?holding "  + hasFile + " ?fileUri .\n"
            + " ?fileUri <http://www.w3.org/2000/01/rdf-schema#label> ?filename "
            + "}";
      //System.out.println(query);
      ResultSet resultSet = null;
      try {
         resultSet = executeSelectQuery(query, true);
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      List<Triple> triples = new ArrayList<Triple>();
      for (QuerySolution solution : IterableAdaptor.adapt(resultSet)) {
         triples.add(new Triple(solution.getResource("holding").getURI(), hasFile, solution.getLiteral("filename").toString()));
       
      }
      
      for (Triple triple : triples) {
         System.out.println(triple.getSubject());
         System.out.println(triple.getPredicate());
         System.out.println(triple.getObject());
         System.out.println();
      }
      jenaModel.close();
      
      
   }
   
   public ResultSet executeSelectQuery(String queryString, boolean datasetMode) throws IOException {
      QueryExecution qexec = buildQueryExec(queryString, datasetMode);
      ResultSet rs = qexec.execSelect();       
      return rs;
   }
   
   private QueryExecution buildQueryExec(String queryString, boolean datasetMode) throws IOException {
      QueryExecution qe;
      if(datasetMode) {
         qe = QueryExecutionFactory.create(QueryFactory.create(queryString, Syntax.syntaxARQ), getDataset());
      } else {
         qe = QueryExecutionFactory.create(QueryFactory.create(queryString, Syntax.syntaxARQ), getJenaModel());
      }
      return qe;
   }
}
