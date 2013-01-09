package edu.cornell.library.integration.old;

import org.marc4j.ErrorHandler;
import org.marc4j.MarcReaderException;

public class MarcErrorHandler implements ErrorHandler {

   public MarcErrorHandler() {
      // TODO Auto-generated constructor stub
   }

   public void error(MarcReaderException ex) {
      ex.printStackTrace();

   }

   public void fatalError(MarcReaderException ex) {
      ex.printStackTrace();

   }

   public void warning(MarcReaderException ex) {
      ex.printStackTrace();

   }

}
