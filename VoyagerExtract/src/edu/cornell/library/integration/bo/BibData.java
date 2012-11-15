package edu.cornell.library.integration.bo;

import java.sql.Clob;

import edu.cornell.library.integration.util.ObjectUtils;
 
public class BibData {
   
   private String bibId;
   private String record;
   private Clob clob;

   public BibData() {
      // TODO Auto-generated constructor stub
   }

   /**
    * @return the bibId
    */
   public String getBibId() {
      return bibId;
   }

   /**
    * @param bibId the bibId to set
    */
   public void setBibId(String bibId) {
      this.bibId = bibId;
   }

    

   /**
    * @return the record
    */
   public String getRecord() {
      return this.record;
   }

   /**
    * @param record the record to set
    */
   public void setRecord(String record) {
      this.record = record;
   }

   /**
    * @return the clob
    */
   public Clob getClob() {
      return clob;
   }

   /**
    * @param clob the clob to set
    */
   public void setClob(Clob clob) {
      this.clob = clob;
   }

}
