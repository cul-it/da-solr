package edu.cornell.library.integration.marc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.xml.stream.XMLStreamException;

import org.junit.Test;

public class MarcRecordTest {

	@Test
	public void testConstructMarcRecordObjectFromXML() throws IOException, XMLStreamException {
		MarcRecord rec = new MarcRecord(
				MarcRecord.RecordType.HOLDINGS,
				"  <record>\n"+
				"    <leader>00182nx  a22000851  4500</leader>\n"+
				"    <controlfield tag=\"001\">7797875</controlfield>\n"+
				"    <controlfield tag=\"004\">7367575</controlfield>\n"+
				"    <controlfield tag=\"005\">20110815105618.0</controlfield>\n"+
				"    <controlfield tag=\"008\">1108150u||||8|||4001uu|||0000000</controlfield>\n"+
				"    <datafield tag=\"852\" ind1=\"8\" ind2=\" \">\n"+
				"      <subfield code=\"b\">serv,remo</subfield>\n"+
				"      <subfield code=\"h\">No call number</subfield>\n"+
				"    </datafield>\n"+
				"  </record>" );
		assertEquals(
				"000    00182nx  a22000851  4500\n"+
				"001    7797875\n"+
				"004    7367575\n"+
				"005    20110815105618.0\n"+
				"008    1108150u||||8|||4001uu|||0000000\n"+
				"852 8  ‡b serv,remo ‡h No call number\n", rec.toString());
	}

	@Test
	public void testConstructMarcRecordObjectFromMarc21()
			throws IOException, URISyntaxException {

		String record = new String(Files.readAllBytes(Paths.get(
				getClass().getClassLoader().getResource("auth_sample.mrc").toURI())));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.AUTHORITY,record.getBytes());
		assertEquals(
				"000    00551cz  a2200169n  4500\n"+
				"001    8700001\n"+
				"005    20121106141524.0\n"+
				"008    110330n| acannaabn           a aaa     c\n"+
				"010    ‡a no2011049997\n"+
				"035    ‡a (OCoLC)oca08812724\n"+
				"035    ‡a (DLC)no2011049997\n"+
				"040    ‡a NIC ‡b eng ‡c NIC ‡d InNd\n"+
				"053  0 ‡a PQ7822.R664\n"+
				"100 1  ‡a Romero, Cecilia, ‡d 1974-\n"+
				"400 1  ‡a Romero Mérida, Cecilia, ‡d 1974-\n"+
				"400 1  ‡a Mérida, Cecilia Romero, ‡d 1974-\n"+
				"670    ‡a Entre las horas, 2010: ‡b t.p. (Cecilia Romero) inside front flap"
				+ " (Cecilia Romero Mérida, b. January 1974)\n", rec.toString());
	}
}
