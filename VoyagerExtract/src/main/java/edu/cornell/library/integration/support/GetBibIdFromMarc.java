package edu.cornell.library.integration.support;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.util.ConvertUtils;

public class GetBibIdFromMarc {

	/** Logger for this class and subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private DavService davService;
	private CatalogService catalogService;

	/**
	 * default constructor
	 */
	public GetBibIdFromMarc() {

	}

	/**
	 * @return the davService
	 */
	public DavService getDavService() {
		return this.davService;
	}

	/**
	 * @param davService
	 *            the davService to set
	 */
	public void setDavService(DavService davService) {
		this.davService = davService;
	}

	/**
	 * @return the catalogService
	 */
	public CatalogService getCatalogService() {
		return this.catalogService;
	}

	/**
	 * @param catalogService
	 *            the catalogService to set
	 */
	public void setCatalogService(CatalogService catalogService) {
		this.catalogService = catalogService;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		GetBibIdFromMarc app = new GetBibIdFromMarc();
		app.run();
	}

	/**
    * 
    */
	public void run() {
		ApplicationContext ctx = new ClassPathXmlApplicationContext(
				"spring.xml");

		if (ctx.containsBean("catalogService")) {
			setCatalogService((CatalogService) ctx.getBean("catalogService"));
		} else {
			System.err.println("Could not get catalogService");
			System.exit(-1);
		}

		setDavService(DavServiceFactory.getDavService());

 
		// Get list of BibIds from full extract
		//String mrcFile = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.full/bib.1373292506.1.mrc";

		String srcDir = "http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.full.done";

		List<String> srcList = new ArrayList<String>();
		try {
			// System.out.println("Getting list of bib marc files");
			srcList = davService.getFileList(srcDir);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		//
		
		
		List<String> extractBibIdList = new ArrayList<String>();
		String mrc = new String();
		
		for (String srcFile : srcList) {
		   System.out.println("Reading bibids from mrc file: "+ srcFile);
	 
		   try {
			  mrc = davService.getFileAsString(srcDir +"/"+ srcFile);
			  for (String s: ConvertUtils.getBibIdFromMarc(mrc,null)) {
			     System.out.println(s);
			  }
			   
			  
		   } catch (Exception e) {
			  // TODO Auto-generated catch block
			  e.printStackTrace();
		   }
		}
		
		for (String s: extractBibIdList) {
			System.out.println(s);
		}
		System.out.println("Done.");


	}

	public void saveListToFile(List<String> bibIdList) throws Exception {
		String fname = "/usr/local/src/integrationlayer/VoyagerExtract/bibid-full-"
				+ getDateString() + ".txt";
		File file = new File(fname);

		StringBuilder sb = new StringBuilder();
		for (String s : bibIdList) {
			sb.append(s + "\n");
		}

		try {
			FileUtils.writeStringToFile(file, sb.toString());
		} catch (IOException ex) {
			throw ex;
		}

	}

	protected String getDateString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Calendar now = Calendar.getInstance();
		String ds = df.format(now.getTime());
		return ds;
	}

}
