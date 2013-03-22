package edu.cornell.library.integration.bo;

 
 
public class MarcData {
   
   private String id;
   private String seqnum;
   private String record;
   
   

   public MarcData() {
      // TODO Auto-generated constructor stub
   }

   /**
    * @return the id
    */
   public String getId() {
      return id;
   }

   /**
    * @param id the id to set
    */
   public void setId(String id) {
      this.id = id;
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
