package edu.cornell.library.integration.bo;

 
 
public class MfhdData {
   
   private String mfhdId;
   private String seqnum;
   private String record;
   
   

   public MfhdData() {
      // TODO Auto-generated constructor stub
   }

   /**
    * @return the mfhdId
    */
   public String getMfhdId() {
      return mfhdId;
   }

   /**
    * @param mfhdId the mfhdId to set
    */
   public void setMfhdId(String mfhdId) {
      this.mfhdId = mfhdId;
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
