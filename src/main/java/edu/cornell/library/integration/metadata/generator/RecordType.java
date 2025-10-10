package edu.cornell.library.integration.metadata.generator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.StatisticalCodes;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.BooleanSolrField;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Record type "Catalog" is required to be visible in Blacklight.
 */
public class RecordType implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.5"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("948","holdings","instance"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

		Boolean isShadow = false;
		Boolean hasUnsuppressedHolding = false;

		for ( DataField f : rec.dataFields )
			if ( f.tag.equals("948") )
				for ( Subfield sf : f.subfields )
					if ( sf.code.equals('h') && sf.value.equalsIgnoreCase("public services shadow record"))
						isShadow = true;

		if (rec.folioHoldings != null)
			for ( Map<String,Object> hRec : rec.folioHoldings ) {
				if (hRec.containsKey("discoverySuppress") && (boolean) hRec.get("discoverySuppress")) continue;
				hasUnsuppressedHolding = true;
			}

		SolrFields sfs = new SolrFields();
		List<String> statCodes = null;
		if ( rec.instance != null && rec.instance.containsKey("statisticalCodeIds") )
			statCodes = StatisticalCodes.dereferenceStatCodes(
				(List<String>)rec.instance.get("statisticalCodeIds"));
			
		if (isShadow)
			sfs.add(new SolrField("type","Shadow"));
		else
			sfs.add(getTypeField(rec.instance,statCodes,"MARC", hasUnsuppressedHolding));
		sfs.add(new SolrField("source","MARC"));

		if ( statCodes != null ) {
			for (String code : statCodes )
				sfs.add(new SolrField("statcode_facet","instance_"+code));
			if ( statCodes.contains("no-google-img")) sfs.add(new BooleanSolrField("no_google_img_b", true));
			if ( statCodes.contains("no-syndetics") ) sfs.add(new BooleanSolrField("no_syndetics_b",  true));
		}
		return sfs;
	}

	private static SolrField getTypeField(
			Map<String, Object> instance, List<String> statCodes, String source, boolean hasUnsuppressedHolding ) {

		if ( instance != null &&
				(( instance.containsKey("discoverySuppress") && (boolean) instance.get("discoverySuppress") )
				|| ( instance.containsKey("staffSuppress") && (boolean) instance.get("staffSuppress") )) )
			return new SolrField("type","Suppressed Bib");
		else if ( statCodes != null && statCodes.contains("Delete") )
			return new SolrField("type","Delete");
		else if (source.equals("MARC") || (source.equals("FOLIO") && hasUnsuppressedHolding))
			return new SolrField("type","Catalog");
		else
			return new SolrField("type","Hidden not suppressed");
	}

	@Override
	public SolrFields generateNonMarcSolrFields(Map<String, Object> instance, Config config) {

		Boolean hasUnsuppressedHolding = false;

		List<String> statCodes = null;
		if (instance.containsKey("statisticalCodeIds"))
			statCodes = StatisticalCodes.dereferenceStatCodes(
					(List<String>)instance.get("statisticalCodeIds"));

		if ( instance.containsKey("holdings") )
			for (Map<String,Object> hRec : (List<Map<String,Object>>) instance.get("holdings")) {
				if (hRec.containsKey("discoverySuppress") && (boolean) hRec.get("discoverySuppress")) continue;
				hasUnsuppressedHolding = true;
			}

		SolrFields sfs = new SolrFields();
		String source = (String)instance.get("source");
		sfs.add(getTypeField(instance,statCodes,source, hasUnsuppressedHolding));
		sfs.add(new SolrField("source",source));
		if (statCodes != null ) {
			for (String code : statCodes ) sfs.add(new SolrField("statcode_facet","instance_"+code));
			if ( statCodes.contains("no-google-img")) sfs.add(new BooleanSolrField("no_google_img_b", true));
			if ( statCodes.contains("no-syndetics") ) sfs.add(new BooleanSolrField("no_syndetics_b",  true));
		}
		return sfs;
	}

}
