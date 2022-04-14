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
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Currently, the only record types are "Catalog" and "Shadow", where shadow records are 
 * detected through a 948‡h note. Blacklight searches will be filtered to type:Catalog,
 * so only records that should NOT be returned in Blacklight work-level searches should
 * vary from this.
 */
public class RecordType implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.3"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("948","holdings","instance"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

		Boolean isShadow = false;

		for ( DataField f : rec.dataFields )
			if ( f.tag.equals("948") )
				for ( Subfield sf : f.subfields )
					if ( sf.code.equals('h') && sf.value.equalsIgnoreCase("public services shadow record"))
						isShadow = true;

		for ( MarcRecord hRec : rec.marcHoldings ) 
			for ( DataField f : hRec.dataFields )
				if ( f.tag.equals("852") )
					for ( Subfield sf : f.subfields )
						if ( sf.code.equals('x') && sf.value.equalsIgnoreCase("public services shadow record") )
							isShadow = true;

		SolrFields sfs = new SolrFields();
		List<String> statCodes = null;
		if ( rec.instance != null && rec.instance.containsKey("statisticalCodeIds") )
			statCodes = StatisticalCodes.dereferenceStatCodes(
				(List<String>)rec.instance.get("statisticalCodeIds"));
			
		if (isShadow)
			sfs.add(new SolrField("type","Shadow"));
		else
			sfs.add(getTypeField(rec.instance,statCodes));
		sfs.add(new SolrField("source","Folio"));
		if ( statCodes != null )
			for (String code : statCodes )
				sfs.add(new SolrField("statcode_t","instance_"+code));
		return sfs;
	}

	private static SolrField getTypeField( Map<String, Object> instance, List<String> statCodes ) {

		if ( instance != null &&
				(( instance.containsKey("discoverySuppress") && (boolean) instance.get("discoverySuppress") )
				|| ( instance.containsKey("staffSuppress") && (boolean) instance.get("staffSuppress") )) )
			return new SolrField("type","Suppressed Bib");
		else if ( statCodes != null && statCodes.contains("Delete") )
			return new SolrField("type","Delete");
		else
			return new SolrField("type","Catalog");
	}

	@Override
	public SolrFields generateNonMarcSolrFields(Map<String, Object> instance, Config config) {

		List<String> statCodes = StatisticalCodes.dereferenceStatCodes(
				(List<String>)instance.get("statisticalCodeIds"));

		SolrFields sfs = new SolrFields();
		sfs.add(getTypeField(instance,statCodes));
		sfs.add(new SolrField("source","Folio"));
		for (String code : statCodes )
			sfs.add(new SolrField("statcode_t","instance_"+code));
		return sfs;
	}

}
