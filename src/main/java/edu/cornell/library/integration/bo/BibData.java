package edu.cornell.library.integration.bo;

 
 
public class BibData {
   
   private String bibId;
   private String seqnum;
   private String record;
   
   

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
    * @return the seqnum
    */
   public String getSeqnum() {
      return seqnum;
   }

   /**
    * @param seqnum the seqnum to set
    */
   public void setSeqnum(String seqnum) {
      this.seqnum = seqnum;
   }

    

    

}
