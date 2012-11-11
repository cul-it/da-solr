package edu.cornell.library.integration.bo;

public class Location {
   
   private String locationId;
   private String locationName;
   private String locationDisplayName;   
   private String locationCode;
   private String suppressInOpac;
   private int mfhdCount;
   private String libraryId;
   private String locationOpac;
   private String locationSpineLabel;
   private String libraryName;
   private String libraryDisplayName;

   public Location() {
      // TODO Auto-generated constructor stub
   }

   /**
    * @return the locationCode
    */
   public String getLocationCode() {
      return locationCode;
   }

   /**
    * @param locationCode the locationCode to set
    */
   public void setLocationCode(String locationCode) {
      this.locationCode = locationCode;
   }

   /**
    * @return the locationId
    */
   public String getLocationId() {
      return locationId;
   }

   /**
    * @param locationId the locationId to set
    */
   public void setLocationId(String locationId) {
      this.locationId = locationId;
   }

   /**
    * @return the suppressInOpac
    */
   public String getSuppressInOpac() {
      return suppressInOpac;
   }

   /**
    * @param suppressInOpac the suppressInOpac to set
    */
   public void setSuppressInOpac(String suppressInOpac) {
      this.suppressInOpac = suppressInOpac;
   }

   /**
    * @return the mfhdCount
    */
   public int getMfhdCount() {
      return mfhdCount;
   }

   /**
    * @param mfhdCount the mfhdCount to set
    */
   public void setMfhdCount(int mfhdCount) {
      this.mfhdCount = mfhdCount;
   }

   /**
    * @return the libraryId
    */
   public String getLibraryId() {
      return libraryId;
   }

   /**
    * @param libraryId the libraryId to set
    */
   public void setLibraryId(String libraryId) {
      this.libraryId = libraryId;
   }

   /**
    * @return the locationOpac
    */
   public String getLocationOpac() {
      return locationOpac;
   }

   /**
    * @param locationOpac the locationOpac to set
    */
   public void setLocationOpac(String locationOpac) {
      this.locationOpac = locationOpac;
   }

   /**
    * @return the locationSpineLabel
    */
   public String getLocationSpineLabel() {
      return locationSpineLabel;
   }

   /**
    * @param locationSpineLabel the locationSpineLabel to set
    */
   public void setLocationSpineLabel(String locationSpineLabel) {
      this.locationSpineLabel = locationSpineLabel;
   }

   /**
    * @return the locationName
    */
   public String getLocationName() {
      return locationName;
   }

   /**
    * @param locationName the locationName to set
    */
   public void setLocationName(String locationName) {
      this.locationName = locationName;
   }

   /**
    * @return the locationDisplayName
    */
   public String getLocationDisplayName() {
      return locationDisplayName;
   }

   /**
    * @param locationDisplayName the locationDisplayName to set
    */
   public void setLocationDisplayName(String locationDisplayName) {
      this.locationDisplayName = locationDisplayName;
   }

   /**
    * @return the libraryName
    */
   public String getLibraryName() {
      return libraryName;
   }

   /**
    * @param libraryName the libraryName to set
    */
   public void setLibraryName(String libraryName) {
      this.libraryName = libraryName;
   }

   /**
    * @return the libraryDisplayName
    */
   public String getLibraryDisplayName() {
      return libraryDisplayName;
   }

   /**
    * @param libraryDisplayName the libraryDisplayName to set
    */
   public void setLibraryDisplayName(String libraryDisplayName) {
      this.libraryDisplayName = libraryDisplayName;
   }

}
