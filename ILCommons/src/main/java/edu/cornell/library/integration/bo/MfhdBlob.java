package edu.cornell.library.integration.bo;

 
import oracle.sql.CLOB;
 
public class MfhdBlob {
   
   private String bibId;
    
   
   private CLOB clob;

   public MfhdBlob() {
      // TODO Auto-generated constructor stub
   }

   /**
    * @return the bibId
    */
   public String getMfhdId() {
      return bibId;
   }

   /**
    * @param bibId the bibId to set
    */
   public void setMfhdId(String bibId) {
      this.bibId = bibId;
   }

  
   /**
    * @return the clob
    */
   public CLOB getClob() {
      return this.clob;
   }

   /**
    * @param clob the clob to set
    */
   public void setClob(CLOB clob) {
      this.clob = clob;
   }

  
}
