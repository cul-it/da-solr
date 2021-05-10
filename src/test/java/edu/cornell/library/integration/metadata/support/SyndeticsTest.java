package edu.cornell.library.integration.metadata.support;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.junit.Test;

import edu.cornell.library.integration.marc.MarcRecord;

public class SyndeticsTest {

	@Test
	public void readSyndeticsMarc() throws XMLStreamException, IOException {
		String xml = "<USMARC><Leader/><VarFlds><VarCFlds><Fld001>69440529</Fld001>"
		+ "<Fld005>20200919000000.0</Fld005><Fld008/></VarCFlds>"
		+ "<VarDFlds><NumbCode><Fld020 I1=\"BLANK\" I2=\"BLANK\"><a>9781984854575</a></Fld020></NumbCode>"
		+ "<MainEnty><Fld100 I1=\"BLANK\" I2=\"BLANK\"><a>Murphy, Chris</a></Fld100></MainEnty>"
		+ "<Titles><Fld245 I1=\"BLANK\" I2=\"BLANK\"><a>The\\Violence Inside Us: A Brief History of an "
		+ "Ongoing American Tragedy</a></Fld245></Titles>"
		+ "<SSIFlds><Fld970 I1=\"0\" I2=\"1\"><t>Preface</t><p>p. ix</p></Fld970><Fld970 I1=\"1\" I2=\"1\">"
		+ "<l>1</l><t>SOS</t><p>p. 3</p></Fld970><Fld970 I1=\"1\" I2=\"1\"><l>2</l>"
		+ "<t>The Violence Inside Us</t><p>p. 26</p></Fld970><Fld970 I1=\"1\" I2=\"1\"><l>3</l>"
		+ "<t>American Violence</t><p>p. 64</p></Fld970><Fld970 I1=\"1\" I2=\"1\"><l>4</l><t>The "
		+ "Violence We See</t><p>p. 113</p></Fld970><Fld970 I1=\"1\" I2=\"1\"><l>5</l><t>The Violence "
		+ "We Ignore</t><p>p. 152</p></Fld970><Fld970 I1=\"1\" I2=\"1\"><l>6</l><t>The Violence We Export</t>"
		+ "<p>p. 202</p></Fld970><Fld970 I1=\"1\" I2=\"1\"><l>7</l><t>Curbing the Means of Violence</t>"
		+ "<p>p. 234</p></Fld970><Fld970 I1=\"1\" I2=\"1\"><l>8</l><t>Pulling Out the Roots of Violence</t>"
		+ "<p>p. 262</p></Fld970><Fld970 I1=\"1\" I2=\"1\"><l>9</l><t>What Lies Inside Us</t><p>p. 290</p>"
		+ "</Fld970><Fld970 I1=\"0\" I2=\"1\"><t>Acknowledgments</t><p>p. 309</p></Fld970>"
		+ "<Fld970 I1=\"0\" I2=\"1\"><t>A Note on Sources Used in This Book</t><p>p. 313</p></Fld970>"
		+ "<Fld970 I1=\"0\" I2=\"1\"><t>Index</t><p>p. 345</p></Fld970><Fld997 I1=\"BLANK\" I2=\"BLANK\">"
		+ "</Fld997></SSIFlds></VarDFlds></VarFlds></USMARC>";
		String expected =
		"001    69440529\n" + 
		"005    20200919000000.0\n" + 
		"008    \n" + 
		"020    ‡a 9781984854575\n" + 
		"100    ‡a Murphy, Chris\n" + 
		"245    ‡a The\\Violence Inside Us: A Brief History of an Ongoing American Tragedy\n" + 
		"970 01 ‡t Preface ‡p p. ix\n" + 
		"970 11 ‡l 1 ‡t SOS ‡p p. 3\n" + 
		"970 11 ‡l 2 ‡t The Violence Inside Us ‡p p. 26\n" + 
		"970 11 ‡l 3 ‡t American Violence ‡p p. 64\n" + 
		"970 11 ‡l 4 ‡t The Violence We See ‡p p. 113\n" + 
		"970 11 ‡l 5 ‡t The Violence We Ignore ‡p p. 152\n" + 
		"970 11 ‡l 6 ‡t The Violence We Export ‡p p. 202\n" + 
		"970 11 ‡l 7 ‡t Curbing the Means of Violence ‡p p. 234\n" + 
		"970 11 ‡l 8 ‡t Pulling Out the Roots of Violence ‡p p. 262\n" + 
		"970 11 ‡l 9 ‡t What Lies Inside Us ‡p p. 290\n" + 
		"970 01 ‡t Acknowledgments ‡p p. 309\n" + 
		"970 01 ‡t A Note on Sources Used in This Book ‡p p. 313\n" + 
		"970 01 ‡t Index ‡p p. 345\n" + 
		"997   \n";
		MarcRecord rec = Syndetics.readSyndeticsMarc(xml);
		assertEquals(expected,rec.toString());
	}

}
