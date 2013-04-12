package edu.cornell.library.integration.bo;

 
 
public class BibMasterData {
   
   private String bibId;
   private String suppressed;
   private String createDate;
   private String updateDate; 
   

   public BibMasterData() {
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
