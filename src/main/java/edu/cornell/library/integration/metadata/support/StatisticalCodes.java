package edu.cornell.library.integration.metadata.support;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.utilities.Config;

public class StatisticalCodes {
	
	private static ReferenceData codes = null;

	public static List<String> dereferenceStatCodes ( Config config, List<String> codeUuids ) throws IOException {
		if ( codes == null ) {
			if ( config.isOkapiConfigured("Folio") )
				codes = new ReferenceData(config.getOkapi("Folio"), "/statistical-codes", "code") ;
			else return null;
		}
		List<String> dereferencedCodes = new ArrayList<>();
		for (String uuid : codeUuids) {
			String code = codes.getName(uuid);
			if ( code != null ) dereferencedCodes.add( code );
		}
		return dereferencedCodes;
	}
}
