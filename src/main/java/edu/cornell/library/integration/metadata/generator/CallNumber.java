package edu.cornell.library.integration.metadata.generator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;

public class CallNumber implements SolrFieldGenerator {

static edu.cornell.library.integration.metadata.support.CallNumber cn = null;

public CallNumber() {}
public CallNumber(String staticCallNumberJson) throws IOException {
	cn = new edu.cornell.library.integration.metadata.support.CallNumber(staticCallNumberJson);
}

@Override
public String getVersion() { return "1.2"; }

@Override
public List<String> getHandledFields() { return Arrays.asList("050","950","holdings"); }

@Override
public SolrFields generateSolrFields(MarcRecord rec, Config config)
		throws SQLException, IOException {
	if (cn == null)
		cn = (config.isOkapiConfigured("Folio"))?
				new edu.cornell.library.integration.metadata.support.CallNumber(config.getOkapi("Folio")):
					new edu.cornell.library.integration.metadata.support.CallNumber();
	for (DataField f : rec.dataFields)
		cn.tabulateCallNumber(f);
	if ( rec.marcHoldings != null ) for ( MarcRecord h : rec.marcHoldings )
		for ( DataFieldSet fs : h.matchAndSortDataFields() )
			for ( DataField f : fs.getFields() )
				if ( f.mainTag.equals("852") )
					cn.tabulateCallNumber(f);
	if ( rec.folioHoldings != null ) for ( Map<String,Object> h : rec.folioHoldings)
		cn.tabulateCallNumber(h);
	return cn.getCallNumberFields(config);
}

@Override
public SolrFields generateNonMarcSolrFields(Map<String, Object> instance, Config config){
	// generateSolrFields correctly handles folioHoldings, so we'll defer to that method
	MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC);
	if ( instance.containsKey("holdings") ) {
		rec.folioHoldings = (List<Map<String, Object>>) instance.get("holdings");
		try {
			return generateSolrFields( rec, config );
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	return null;
}
}