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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.cornell.library.integration.service.DavService;



/**
 * Vivo Controller
 */
public class DataIndexerController extends MultiActionController { 
   private String showTriplesLocation = "ShowTriplesLocation";
   private String homePage = "HomePage"; 
   private String fatalErrorView = "FatalError";

   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass());
   
   private DavService davService;

    

   /**
    * @return the davService
    */
   public DavService getDavService() {
      return davService;
   }

   /**
    * @param userService the davService to set
    */
   public void setDavService(DavService davService) {
      this.davService = davService;
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
   public ModelAndView getTriplesLocation(HttpServletRequest request,
         HttpServletResponse response) throws ServletException, IOException {
      Map<String, Object> model = new HashMap<String, Object>();
      String id = request.getParameter("id");
       
      String view = new String(showTriplesLocation);

      return new ModelAndView(view, model);
   }
   
    
    
}
