package edu.cornell.library.integration.config;

public class ConverterProperties {
   
   private String sourceDir;
   private String saveDir;
   private String outputDir;
   private String destination;
   private String errorDir;
   private String errorXmlDir;
   private String logDir;
   private String logLevel;
   private String marcSchema; 
   private String marcEncoding;
   private int splitSize;
   private boolean translateLeaderBadCharsToZero; 
   private boolean translateNonleaderBadCharsToSpaces;

   public ConverterProperties() {
      this.sourceDir = "";
      this.saveDir = "";
      this.outputDir = "";
      this.destination = "";
      this.errorDir = "";
      this.errorXmlDir = "";
      this.logDir = "";
      this.logLevel = "";
      this.marcSchema  = ""; 
      this.marcEncoding = "";
      this.splitSize = 10000;
      this.translateLeaderBadCharsToZero = true; 
      this.translateNonleaderBadCharsToSpaces = true;
   }

   /**
    * @return the sourceDir
    */
   public String getSourceDir() {
      return sourceDir;
   }

   /**
    * @param sourceDir the sourceDir to set
    */
   public void setSourceDir(String sourceDir) {
      this.sourceDir = sourceDir;
   }

   /**
    * @return the saveDir
    */
   public String getSaveDir() {
      return saveDir;
   }

   /**
    * @param saveDir the saveDir to set
    */
   public void setSaveDir(String saveDir) {
      this.saveDir = saveDir;
   }

   /**
    * @return the outputDir
    */
   public String getOutputDir() {
      return outputDir;
   }

   /**
    * @param outputDir the outputDir to set
    */
   public void setOutputDir(String outputDir) {
      this.outputDir = outputDir;
   }

   /**
    * @return the destination
    */
   public String getDestination() {
      return destination;
   }

   /**
    * @param destination the destination to set
    */
   public void setDestination(String destination) {
      this.destination = destination;
   }

   /**
    * @return the errorDir
    */
   public String getErrorDir() {
      return errorDir;
   }

   /**
    * @param errorDir the errorDir to set
    */
   public void setErrorDir(String errorDir) {
      this.errorDir = errorDir;
   }

   /**
    * @return the errorXmlDir
    */
   public String getErrorXmlDir() {
      return errorXmlDir;
   }

   /**
    * @param errorXmlDir the errorXmlDir to set
    */
   public void setErrorXmlDir(String errorXmlDir) {
      this.errorXmlDir = errorXmlDir;
   }

   /**
    * @return the logDir
    */
   public String getLogDir() {
      return logDir;
   }

   /**
    * @param logDir the logDir to set
    */
   public void setLogDir(String logDir) {
      this.logDir = logDir;
   }

   /**
    * @return the logLevel
    */
   public String getLogLevel() {
      return logLevel;
   }

   /**
    * @param logLevel the logLevel to set
    */
   public void setLogLevel(String logLevel) {
      this.logLevel = logLevel;
   }

   /**
    * @return the marcSchema
    */
   public String getMarcSchema() {
      return marcSchema;
   }

   /**
    * @param marcSchema the marcSchema to set
    */
   public void setMarcSchema(String marcSchema) {
      this.marcSchema = marcSchema;
   }

   /**
    * @return the marcEncoding
    */
   public String getMarcEncoding() {
      return marcEncoding;
   }

   /**
    * @param marcEncoding the marcEncoding to set
    */
   public void setMarcEncoding(String marcEncoding) {
      this.marcEncoding = marcEncoding;
   }

   /**
    * @return the splitSize
    */
   public int getSplitSize() {
      return splitSize;
   }

   /**
    * @param splitSize the splitSize to set
    */
   public void setSplitSize(int splitSize) {
      this.splitSize = splitSize;
   }

   /**
    * @return the translateLeaderBadCharsToZero
    */
   public boolean isTranslateLeaderBadCharsToZero() {
      return translateLeaderBadCharsToZero;
   }

   /**
    * @param translateLeaderBadCharsToZero the translateLeaderBadCharsToZero to set
    */
   public void setTranslateLeaderBadCharsToZero(
         boolean translateLeaderBadCharsToZero) {
      this.translateLeaderBadCharsToZero = translateLeaderBadCharsToZero;
   }

   /**
    * @return the translateNonleaderBadCharsToSpaces
    */
   public boolean isTranslateNonleaderBadCharsToSpaces() {
      return translateNonleaderBadCharsToSpaces;
   }

   /**
    * @param translateNonleaderBadCharsToSpaces the translateNonleaderBadCharsToSpaces to set
    */
   public void setTranslateNonleaderBadCharsToSpaces(
         boolean translateNonleaderBadCharsToSpaces) {
      this.translateNonleaderBadCharsToSpaces = translateNonleaderBadCharsToSpaces;
   }
   

}
