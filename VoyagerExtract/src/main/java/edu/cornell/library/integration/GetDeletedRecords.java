package edu.cornell.library.integration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.marc4j.MarcException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.Record;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;

public class GetDeletedRecords {

   protected final Log logger = LogFactory.getLog(getClass());

   public static final String TMPDIR = "/tmp";

   private DavService davService;

   public GetDeletedRecords() {
      // TODO Auto-generated constructor stub
   }

   /**
    * @return the davService
    */
   public DavService getDavService() {
      return this.davService;
   }

   /**
    * @param davService
    *           the davService to set
    */
   public void setDavService(DavService davService) {
      this.davService = davService;
   }

   public int countRecords(DavService davService, String srcDir, String srcFile)
         throws Exception {
      int total = 0;
      Record record = null;
      // InputStream is = davService.getFileAsInputStream(srcDir + "/"
      // +srcFile);
      String tmpFilePath = TMPDIR + "/" + srcFile;
      // File f = davService.getFile(srcDir +"/"+ srcFile, tmpFilePath);
      File f = new File("/tmp/mfhd.deleted.mrc");
      FileInputStream is = new FileInputStream(f);
      MarcPermissiveStreamReader reader = null;
      boolean permissive = true;
      boolean convertToUtf8 = true;
      reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8);

      int cnt = 0;
      String lastDateString = "20121201010000.0";
      Calendar lastCalDate = getCalDate(lastDateString);
      SimpleDateFormat outputformat = new SimpleDateFormat("yyyy.MM.dd 'at' hh:mm:ss a");
      while (reader.hasNext()) {
      //for (int i = 0 ; i < 300 ; i++) {
         try {
            record = reader.next();
            if (testUpdateTS(record, lastCalDate)) {
               displayRecord(record);
            }

            total++;
         } catch (MarcException me) {
            logger.error("MarcException reading record", me);
            continue;
         } catch (Exception e) {
            e.printStackTrace();
            continue;
         }

      } // end while loop
      // display last record
      System.out.println(record.toString());

      try {
         is.close();
      } catch (IOException e) {
         e.printStackTrace();
      }

      return total;
   }
   
   protected void displayRecord(Record record) {
      // System.out.println(record.toString());

      ControlField f001 = (ControlField) record.getVariableField("001");
      if (f001 != null) {
         System.out.println("001 field: " + f001.getData().toString());
      }
      ControlField f004 = (ControlField) record.getVariableField("004");
      if (f004 != null) {
         System.out.println("004 field: " + f004.getData().toString());
      }
      ControlField f005 = (ControlField) record.getVariableField("005");
      if (f005 != null) {
         System.out.println("005 field: " + f005.getData().toString());
      }

      System.out.println();   
   }
   
   protected boolean testUpdateTS(Record record, Calendar lastCalDate) {
      
      String recordDateString = new String();
      ControlField f005 = (ControlField) record.getVariableField("005");
      if (f005 != null) {
         recordDateString = f005.getData().toString();
      }
      
      Calendar recordCalDate = getCalDate(recordDateString);
      
             
       
      return recordCalDate.after(lastCalDate);
      
      
   }
   
   protected Calendar getCalDate(String ds) {
       
      DateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss.0");
      Calendar  calDate = Calendar.getInstance();
      Date thisdate = null;
      try {
         thisdate = (Date) formatter.parse(ds); 
      } catch (ParseException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      calDate.setTime(thisdate);
      return calDate;
   }

   public void run(String srcType, String srcDir) {
      setDavService(DavServiceFactory.getDavService());

      // get list of Full mrc files
      List<String> srcList = new ArrayList<String>();
      try {
         // System.out.println("Getting list of bib marc files");
         srcList = davService.getFileList(srcDir);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      if (srcList.size() == 0) {
         System.out.println("No Marc files available to process");
      } else {
         for (String srcFile : srcList) {
            // System.out.println("Converting mrc file: "+ srcFile);
            try {
               int total = countRecords(davService, srcDir, srcFile);
               System.out.println(srcFile + ": " + total);
            } catch (Exception e) {
               try {
                  System.out
                        .println("Exception thrown. Could not read records: "
                              + srcFile);
                  e.printStackTrace();

               } catch (Exception e1) {
                  e1.printStackTrace();
               }
            }
         }
      }

   }

   public static void main(String[] args) {
      GetDeletedRecords app = new GetDeletedRecords();
      if (args.length != 2) {
         System.err.println("You must provide a src Dir as an argument");
         System.exit(-1);
      }
      String srcType = args[0];
      String srcDir = args[1];
      app.run(srcType, srcDir);
   }

}
