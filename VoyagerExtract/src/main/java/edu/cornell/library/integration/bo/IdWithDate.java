package edu.cornell.library.integration.bo;

public class IdWithDate {

   public Integer id;
   public String modifiedDate;

   public IdWithDate(int id, String date) {
	   this.id = id;
	   this.modifiedDate = date;
	   if (this.modifiedDate == null)
		   this.modifiedDate = "";
   }
   
   public String toString() {
	   StringBuilder sb = new StringBuilder();
	   sb.append(id);
	   sb.append('|');
	   sb.append(modifiedDate);
	   return sb.toString();
   }
}
