package edu.cornell.library.integration;

import java.io.*;
import org.marc4j.*;
import org.marc4j.marc.Leader;
import org.marc4j.helpers.ErrorHandlerImpl;

public class TaggedWriter implements MarcHandler {

   /** The Writer object */
   private Writer out;

   /** Set the writer object */
   public void setWriter(Writer out) {
      this.out = out;
   }

   public void startCollection() {
      if (out == null)
         System.exit(0);
   }

   public void startRecord(Leader leader) {
      rawWrite("Leader ");
      rawWrite(leader.marshal());
      rawWrite('\n');
   }

   public void controlField(String tag, char[] data) {
      rawWrite(tag);
      rawWrite(' ');
      rawWrite(new String(data));
      rawWrite('\n');
   }

   public void startDataField(String tag, char ind1, char ind2) {
      rawWrite(tag);
      rawWrite(' ');
      rawWrite(ind1);
      rawWrite(ind2);
   }

   public void subfield(char code, char[] data) {
      rawWrite('$');
      rawWrite(code);
      rawWrite(new String(data));
   }

   public void endDataField(String tag) {
      rawWrite('\n');
   }

   public void endRecord() {
      rawWrite('\n');
   }

   public void endCollection() {
      try {
         out.flush();
         out.close();
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   private void rawWrite(char c) {
      try {
         out.write(c);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   private void rawWrite(String s) {
      try {
         out.write(s);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

}
