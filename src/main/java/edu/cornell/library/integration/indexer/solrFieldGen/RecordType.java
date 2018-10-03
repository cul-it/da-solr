package edu.cornell.library.integration.indexer.solrFieldGen;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

/**
 * Currently, the only record types are "Catalog" and "Shadow", where shadow records are 
 * detected through a 948‡h note. Blacklight searches will be filtered to type:Catalog,
 * so only records that should NOT be returned in Blacklight work-level searches should
 * vary from this.
 */
public class RecordType implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("948","holdings"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

		Boolean isShadow = false;

		for ( DataField f : rec.dataFields )
			if ( f.tag.equals("948") )
				for ( Subfield sf : f.subfields )
					if ( sf.code.equals('h') && sf.value.equalsIgnoreCase("public services shadow record") )
						isShadow = true;

		for ( MarcRecord hRec : rec.holdings ) 
			for ( DataField f : hRec.dataFields )
				if ( f.tag.equals("852") )
					for ( Subfield sf : f.subfields )
						if ( sf.code.equals('x') && sf.value.equalsIgnoreCase("public services shadow record") )
							isShadow = true;

		SolrFields sfs = new SolrFields();
		sfs.add(new SolrField("type",isShadow?"Shadow":"Catalog"));
		sfs.add(new SolrField("source","Voyager"));
		return sfs;
	}

}
