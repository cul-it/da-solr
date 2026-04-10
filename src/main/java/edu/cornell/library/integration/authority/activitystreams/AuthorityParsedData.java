package edu.cornell.library.integration.authority.activitystreams;

import edu.cornell.library.integration.authority.AuthoritySource;

public class AuthorityParsedData {
	public String id = null;
	public String lccn = null;
	public AuthoritySource vocab = null;
	public MadsRecordStatus recordStatus = null;
	public String authorativeLabel = null;
	public MadsHeadingType headingType = null;
	public boolean isSubdivision = false;
	public boolean undifferentiated = false;
	public String moddate = null;
	public int numUpdates = 0;
	public String source = null;

	public void print() {
		System.out.println("ID: " + id);
		System.out.println("LCCN: " + lccn);
		System.out.println("Vocab: " + vocab);
		System.out.println("Record status: " + recordStatus.getDisplay());
		System.out.println("authorativeLabel: " + authorativeLabel);
		System.out.println("Heading type: " + headingType);
		System.out.println("Subdivision: " + isSubdivision);
		System.out.println("Undifferentiated: " + undifferentiated);
		System.out.println("Mod date: " + moddate);
		System.out.println("Num updates: " + numUpdates);
		System.out.println("Source: " + source);
	}
}
