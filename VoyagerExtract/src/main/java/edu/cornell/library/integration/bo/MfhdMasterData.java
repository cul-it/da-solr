package edu.cornell.library.integration.bo;

 
 
public class MfhdMasterData {
   
   private String mfhdId;
   private String suppressed;
   private String createDate;
   private String updateDate; 
   

   public MfhdMasterData() {
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
    * @return the suppressed
    */
   public String getSuppressed() {
      return suppressed;
   }

   /**
    * @param suppressed the suppressed to set
    */
   public void setSuppressed(String suppressed) {
      this.suppressed = suppressed;
   }

   /**
    * @return the createDate
    */
   public String getCreateDate() {
      return createDate;
   }

   /**
    * @param createDate the createDate to set
    */
   public void setCreateDate(String createDate) {
      this.createDate = createDate;
   }

   /**
    * @return the updateDate
    */
   public String getUpdateDate() {
      return updateDate;
   }

   /**
    * @param updateDate the updateDate to set
    */
   public void setUpdateDate(String updateDate) {
      this.updateDate = updateDate;
   }

    

}
