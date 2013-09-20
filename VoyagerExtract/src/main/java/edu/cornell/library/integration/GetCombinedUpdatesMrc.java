package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.MfhdData;
import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;

public class GetCombinedUpdatesMrc extends VoyagerToSolrStep {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 
   protected static Integer reportPeriod = 1; //days

   public static final String TMPDIR = "/tmp";
   /**
    * default constructor
    */
   public GetCombinedUpdatesMrc() { 
       
   }  
   
   /**
    * @param args
 * @throws Exception 
    */
   public static void main(String[] args) throws Exception {
     GetCombinedUpdatesMrc app = new GetCombinedUpdatesMrc();
     if (args.length != 3 ) {
        System.err.println("You must provide bib, mfhd and updateBibs Dirs as an arguments");
        System.exit(-1);
     }
      
     VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig(args );
     
     String bibDestDir  = config.getWebdavBaseUrl() + config.getDailyMrcDir();          
     String mfhdDestDir  = config.getWebdavBaseUrl() + config.getDailyMfhdDir() ;
     String updateBibsDir  = config.getWebdavBaseUrl() + config.getDailyCombinedMrcDir();
     
     app.run(config, bibDestDir, mfhdDestDir, updateBibsDir, reportPeriod);
   }
   

   /**
 * @param config 
    * 
    */

	public void run(VoyagerToSolrConfiguration config, 
	        String bibDestDir, String mfhdDestDir, String updateBibsDir,
	        Integer reportPeriod) throws Exception{

		if ( getCatalogService() == null ){
			System.err.println("Could not get catalogService");
			System.exit(-1);
		}

		setDavService(DavServiceFactory.getDavService( config ));

		Calendar now = Calendar.getInstance();
		String toDate = midnightThisMorning();
		String fromDate = daysEarlier(reportPeriod);
		String today = getDateString(now);

 
		System.out.println("fromDate: "+ fromDate);
        System.out.println("toDate: "+ toDate);
        
		// get list of bibids updates using recent date String
		List<String> bibIdList = new ArrayList<String>();
		List<String> mfhdIdList = new ArrayList<String>();
		List<String> extraBibIdList = new ArrayList<String>();
		List<String> extraMfhdIdList = new ArrayList<String>();
        
		// get recently update bib and mfhd ids
		try {
			System.out.println("Getting recently updated bibids");
			bibIdList = getCatalogService().getUpdatedBibIdsUsingDateRange(fromDate,
					toDate);
			System.out.println("BibIDList size: "+ bibIdList.size());
			System.out.println("Getting recently updated mfhd ids");
			mfhdIdList = getCatalogService().getUpdatedMfhdIdsUsingDateRange(
					fromDate, toDate);
			System.out.println("MfhdIDList size: " + mfhdIdList.size());
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(-1);
		}
		
        // Check mfhdIdList for  bibIds that are not in the bibIdList
		System.out.println("Adding extra bibids");
		List<String> tmpBibIdList = new ArrayList<String>();
		for (String mfhdid : mfhdIdList) {
			try {
				tmpBibIdList.clear(); 
				tmpBibIdList = getCatalogService().getBibIdsByMfhdId(mfhdid);
				for (String bibid : tmpBibIdList) {
					if (! bibIdList.contains(bibid)) {
						extraBibIdList.add(bibid);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
                System.exit(-1);
			} 
		} 
		System.out.println("ExtraBibIDList: " + extraBibIdList.size());
		//
		// add bib ids which have deleted mfhd ids
		//
		String bibListForUpdateFileName = "bibListForUpdate-"+ today +".txt"; 

		String tmpFilePath = TMPDIR +"/"+ bibListForUpdateFileName;
		List<String> bibListForUpdateList = new ArrayList<String>();
		File bibListForUpdateFile = null;
	    try {

	    	bibListForUpdateFile = getDavService().getFile(updateBibsDir+"/"+bibListForUpdateFileName, tmpFilePath);
			bibListForUpdateList = FileUtils.readLines(bibListForUpdateFile);
		} catch (Exception e1) {
		        System.err.println("Failed reading: "+ updateBibsDir+"/"+bibListForUpdateFileName);
			e1.printStackTrace();
			System.exit(-1);
		} finally {
			bibListForUpdateFile.delete();	
		}
	    System.out.println("bibListForUpdate: " + bibListForUpdateList.size() + " (from deleted and suppressed mfhds)");
	    for (String s: bibListForUpdateList) {
	    	if (! extraBibIdList.contains(s)) {
				extraBibIdList.add(s);
			}	
	    }
	     
	    
		System.out.println("Adding extra holdings ids");

		// add extra holdings ids from each bibId to extraMfhdIdList
		List<String> tmpMfhdIdList = new ArrayList<String>();
		for (String bibid : bibIdList) {
			try {
				tmpMfhdIdList.clear(); 
				tmpMfhdIdList = getCatalogService().getMfhdIdsByBibId(bibid);
				for (String mfhdid: tmpMfhdIdList) {
					if (! mfhdIdList.contains(mfhdid)) {
						extraMfhdIdList.add(mfhdid);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
                System.exit(-1);
			} 
		}
		// add mfhds for bibid in extraBibIdList as well
		for (String bibid : extraBibIdList) {
			try {
				tmpMfhdIdList.clear(); 
				tmpMfhdIdList = getCatalogService().getMfhdIdsByBibId(bibid);
				for (String mfhdid: tmpMfhdIdList) {
					if (! mfhdIdList.contains(mfhdid)) {
						extraMfhdIdList.add(mfhdid);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
                System.exit(-1);
			} 
		}
		//
		
		
		System.out.println("ExtraMfhdIDList: " + extraMfhdIdList.size());
		int totalBibId = bibIdList.size() + extraBibIdList.size();
		int totalMfhdId = mfhdIdList.size() + extraMfhdIdList.size();
		System.out.println("Total BibIDList: " + totalBibId);
		System.out.println("Total MfhdIDList: " + totalMfhdId);

		// iterate over bibids, concatenate bib data to create mrc
		int recno = 0;
		int maxrec = 10000;
		int seqno = 1;
		StringBuffer sb = new StringBuffer();
		
		for (String bibid : bibIdList) { 
			try {
		       //Getting bib mrc for bibid
		       List<BibData> bibDataList = getCatalogService().getBibData(bibid);
		       
               for (BibData bibData : bibDataList) { 
            	   sb.append(bibData.getRecord()); 
		       }
		       recno = recno + 1;
		       if (recno >= maxrec) {
		    	   saveBibMrc(sb.toString(), seqno, bibDestDir);
		    	   seqno = seqno + 1;
		    	   sb = new StringBuffer();
		       }		        
		    } catch (Exception e) { 
		    	e.printStackTrace(); 
                System.exit(-1);
		    } 
		}
		try {
			saveBibMrc(sb.toString(), seqno, bibDestDir);
			seqno = seqno + 1;
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(-1);
		}
		sb = new StringBuffer();
		recno  = 0;
		for (String bibid : extraBibIdList) { 
			try {
		       //System.out.println("Getting bib mrc for bibid: " + bibid);
		       List<BibData> bibDataList = getCatalogService().getBibData(bibid);
		       
               for (BibData bibData : bibDataList) { 
            	   sb.append(bibData.getRecord()); 
		       }
		       recno = recno + 1;
		       if (recno >= maxrec) {
		    	   saveBibMrc(sb.toString(), seqno, bibDestDir);
		    	   seqno = seqno + 1;
		    	   sb = new StringBuffer();
		       }
		        
		    } catch (Exception e) {                 
		    	e.printStackTrace(); 
                System.exit(-1);
		    } 
		}
		try {
			saveBibMrc(sb.toString(), seqno, bibDestDir);
			seqno = seqno + 1;
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(-1);
		}
		
		// save mhfhd marc
		sb = new StringBuffer();
		recno = 0;
		seqno = 1;
		for (String mfhdid : mfhdIdList) { 
			try {
		       //System.out.println("Getting mfhd mrc for mfhdid: " + mfhdid);
		       List<MfhdData> mfhdDataList = getCatalogService().getMfhdData(mfhdid);
		       
               for (MfhdData mfhdData : mfhdDataList) { 
            	   sb.append(mfhdData.getRecord()); 
		       }
		       recno = recno + 1;
		       if (recno >= maxrec) {
		    	   saveMfhdMrc(sb.toString(), seqno, mfhdDestDir);
		    	   seqno = seqno + 1;
		    	   sb = new StringBuffer();
		       }
		        
		       //System.out.println("mrc: " + mrc);
		        
		    } catch (Exception e) { 
		    	e.printStackTrace(); 
                System.exit(-1);
		    } 
		}
		try {
			saveMfhdMrc(sb.toString(), seqno, mfhdDestDir);
			seqno = seqno + 1;
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(-1);
		}
		
		sb = new StringBuffer();
		recno  = 0;
		for (String mfhdid : extraMfhdIdList) { 
			try {
		       //System.out.println("Getting mfhd mrc for mfhdid: " + mfhdid);
		       List<MfhdData> mfhdDataList = getCatalogService().getMfhdData(mfhdid);
		       
               for (MfhdData mfhdData : mfhdDataList) { 
            	   sb.append(mfhdData.getRecord()); 
		       }
		       recno = recno + 1;
		       if (recno >= maxrec) {
		    	   saveMfhdMrc(sb.toString(), seqno, mfhdDestDir);
		    	   seqno = seqno + 1;
		    	   sb = new StringBuffer();
		       }
		        
		       //System.out.println("mrc: " + mrc);
		       // 
		    } catch (Exception e) {                 
		    	e.printStackTrace(); 
                System.exit(-1);
		    } 
		}
		try {
			saveMfhdMrc(sb.toString(), seqno, mfhdDestDir);
			seqno = seqno + 1;
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(-1);
		}	 
        System.out.println("Done.");
	}

 
	public void saveBibMrc(String mrc, int seqno, String destDir)
			throws Exception {
		Calendar now = Calendar.getInstance();
		String url = destDir + "/bib.update." + getDateString(now) + "."+ seqno +".mrc";
		// System.out.println("Saving mrc to: "+ url);
		try {

			// FileUtils.writeStringToFile(new File("/tmp/test.mrc"), mrc,
			// "UTF-8");
			InputStream isr = IOUtils.toInputStream(mrc, "UTF-8");
			getDavService().saveFile(url, isr);

		} catch (UnsupportedEncodingException ex) {
			throw ex;
		} catch (Exception ex) {
			throw ex;
		}
	}
	
	public void saveMfhdMrc(String mrc, int seqno, String destDir)	throws Exception {
		Calendar now = Calendar.getInstance();
		String url = destDir + "/mfhd.update." + getDateString(now) + "."+ seqno +".mrc";
		// System.out.println("Saving mrc to: "+ url);
		try {

			// FileUtils.writeStringToFile(new File("/tmp/test.mrc"), mrc,
			// "UTF-8");
			InputStream isr = IOUtils.toInputStream(mrc, "UTF-8");
			getDavService().saveFile(url, isr);

		} catch (UnsupportedEncodingException ex) {
			throw ex;
		} catch (Exception ex) {
			throw ex;
		}
	}
       
   
   /**
    * @param str
    * @return
    * @throws UnsupportedEncodingException
    */
   protected InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);	
   }
   
   /**
    * @return
    */
   protected String midnightThisMorning() {
	   Calendar today = Calendar.getInstance();
	   today.set(Calendar.HOUR_OF_DAY, 0);
	   today.set(Calendar.MINUTE, 0);
	   today.set(Calendar.SECOND, 0);
	   today.set(Calendar.MILLISECOND, 0); 
	   return getDateTimeString(today);
   }
   
   protected String daysEarlier(Integer noDays) {
	   Calendar date = Calendar.getInstance();
	   date.set(Calendar.HOUR_OF_DAY, 0);
	   date.set(Calendar.MINUTE, 0);
	   date.set(Calendar.SECOND, 0);
	   date.set(Calendar.MILLISECOND, 0);
	   date.add(Calendar.DAY_OF_MONTH, - noDays);
	   return getDateTimeString(date);
   }
   
   
   protected String getDateTimeString(Calendar cal) {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	   String ds = df.format(cal.getTime());
	   return ds;
   }
   
   protected String getDateString(Calendar cal) {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); 
	   String ds = df.format(cal.getTime());
	   return ds;
   }
   
   protected String getRelativeDateString(Calendar cal, int offset) {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	   Calendar offsetCal = cal;
	   offsetCal.add(Calendar.HOUR, offset);
	   String ds = df.format(offsetCal.getTime());
	   return ds;
   }
      
    
}
