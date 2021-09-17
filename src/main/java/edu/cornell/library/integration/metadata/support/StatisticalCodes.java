package edu.cornell.library.integration.metadata.support;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.folio.ReferenceData;

public class StatisticalCodes {
	
	private static ReferenceData codes = null;

	public static void initializeCodes(OkapiClient folio ) throws IOException {
		if ( codes == null ) codes = new ReferenceData(folio, "/statistical-codes", "code") ;
	}

	public static List<String> dereferenceStatCodes ( List<String> codeUuids ) {
		if ( codes == null ) {
			System.out.println("Statistical Codes uninitiated.");
			System.exit(1);
		}
		List<String> dereferencedCodes = new ArrayList<>();
		for (String uuid : codeUuids) {
			String code = codes.getName(uuid);
			if ( code != null ) dereferencedCodes.add( code );
		}
		return dereferencedCodes;
	}
}
