package edu.cornell.library.integration.bo;

 
import oracle.sql.CLOB;
 
public class BibBlob {
   
   private String bibId;
    
   
   private CLOB clob;

   public BibBlob() {
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
