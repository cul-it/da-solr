package edu.cornell.library.integration.control;



import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.multiaction.MultiActionController;
import org.springframework.web.util.WebUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Selector;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.tdb.TDBFactory;

import edu.cornell.library.integration.bo.Triple;
import edu.cornell.library.integration.util.IterableAdaptor;



/**
 * Vivo Controller
 */
public class DataIndexerController extends MultiActionController { 
   private String showTriplesLocation = "ShowTriplesLocation";
   private String redirect300 = "Redirect300";
   private String redirect302 = "Redirect302";
   private String homePage = "HomePage"; 
   private String fatalErrorView = "FatalError";
   private final String TDBDIR = "/usr/local/src/integrationlayer/tdbIndex";
   private final String uriNs = "http://fbw4-dev.library.cornell.edu/individuals";
   private final String dataNs = "http://culdata.library.cornell.edu/canonical/0.1/";
   private final String dataDevNs = "http://culdatadev.library.cornell.edu/canonical/0.1/";
   
   private Model jenaModel;
   private Dataset dataset;

   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass());
   
   
   
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
    * showHomePage
    *
    * @param request a httpservlet request
    * @param response a httpservlet response
    * @throws ServletException a servletException
    * @throws IOException an IOException
    * @return ModelAndView
    */
   public ModelAndView showHomePage(HttpServletRequest request,
         HttpServletResponse response) throws ServletException, IOException {
      Map<String, Object> model = new HashMap<String, Object>(); 
       
      String view = new String(homePage);

      return new ModelAndView(view, model);
   }

   /**
    * getTriplesLocation
    *
    * @param request a httpservlet request
    * @param response a httpservlet response
    * @throws ServletException a servletException
    * @throws IOException an IOException
    * @return ModelAndView
    */
   /**
    * @param request
    * @param response
    * @return
    * @throws ServletException
    * @throws IOException
    */
   public ModelAndView showTriplesLocation(HttpServletRequest request,
         HttpServletResponse response) throws ServletException, IOException {
      Map<String, Object> model = new HashMap<String, Object>();
      String view = new String();
      
      String bibid = request.getParameter("bibid");
      String biburi = request.getParameter("biburi");
      String redirect = new String();
      redirect = request.getParameter("redirect");
      if (StringUtils.isEmpty(redirect)) {
         redirect = "true";   
      } else {
         redirect = request.getParameter("redirect");   
      }
      //System.out.println("Getting model");
      setDataset(TDBFactory.createDataset(TDBDIR));
      setJenaModel(dataset.getDefaultModel());
      
      //System.out.println("Model size: "+ jenaModel.size());
      
      // get Resources with bibId
      //String subject = "<" +uriNs + "/b" + bibid + ">"; 
       
      String hasFile = "<" +dataNs + "hasFile>";
      String hasBib = "<http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord>";
      String hasHoldings = "<" +dataNs + "hasFile>";
      
      // use full biburi if provided, otherwise generate object from the namespace and bibid
      String object = new String();
      if (! StringUtils.isEmpty(biburi)) {
         object = biburi;
      } else {
         object = "<" +uriNs + "/b" + bibid + ">";
      }
 
      String query = "" + "SELECT ?filename \n" 
            + "WHERE {\n" 
            + " ?holding "  + hasBib + " "+ object + " .\n"
            + " ?holding "  + hasFile + " ?fileUri .\n"
            + " ?fileUri <http://www.w3.org/2000/01/rdf-schema#label> ?filename "
            + "}";
      ResultSet resultSet = null;
      try {
         resultSet = executeSelectQuery(query, true);
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      List<String> fileUriList = new ArrayList<String>();
      //List<Triple> triples = new ArrayList<Triple>();
      
      for (QuerySolution solution : IterableAdaptor.adapt(resultSet)) {
         //triples.add(new Triple(solution.getResource("holding").getURI(), hasFile, solution.getLiteral("filename").toString())); 
         fileUriList.add( (String) solution.getLiteral("filename").toString());
      }
      
      jenaModel.close();
      model.put("bibid", bibid);
      
      //model.put("triples", triples);
      
      if (redirect.equals("false")) {
         model.put("fileUriList", fileUriList);
         view = new String(showTriplesLocation);
      } else {
         logger.info("fileUriList size "+ fileUriList.size());
         if (fileUriList.size() == 1) {
            model.put("url", fileUriList.get(0));
            view = redirect302;
         } else {
            
            model.put("fileUriList", fileUriList);
            view = redirect300;
            //view = "redirect:"+ fileUriList.get(0);
         }
      }
      return new ModelAndView(view, model);
   }
   
   /**
    * @param queryString
    * @param datasetMode
    * @return
    * @throws IOException
    */
   public ResultSet executeSelectQuery(String queryString, boolean datasetMode) throws IOException {
      QueryExecution qexec = buildQueryExec(queryString, datasetMode);
      ResultSet rs = qexec.execSelect();       
      return rs;
   }
   
   /**
    * @param queryString
    * @param datasetMode
    * @return
    * @throws IOException
    */
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
