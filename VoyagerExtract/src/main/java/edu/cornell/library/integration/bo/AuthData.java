package edu.cornell.library.integration.bo;

 
 
public class AuthData {
   
   private String authId;
   private String seqnum;
   private String record;
   
   

   public AuthData() {
      // TODO Auto-generated constructor stub
   }

   /**
    * @return the authId
    */
   public String getAuthId() {
      return authId;
   }

   /**
    * @param authId the authId to set
    */
   public void setAuthId(String authId) {
      this.authId = authId;
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
