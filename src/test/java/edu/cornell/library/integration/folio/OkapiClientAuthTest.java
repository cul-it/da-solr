package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import edu.cornell.library.integration.utilities.Config;

public class OkapiClientAuthTest {

	public static void main(String[] args) throws IOException, InterruptedException {

		Map<String, String> env = System.getenv();
		String folioConfig = env.get("target_folio");
		if ( folioConfig == null )
			throw new IllegalArgumentException("target_folio must be set in environment to name of target Folio instance.");
		Config config = Config.loadConfig(new HashSet<>());
		OkapiClient folio = config.getOkapi(folioConfig);
		
		folio.printLoginStatus(folio);

		Instant testCompletedAt = Instant.now().plus(17, ChronoUnit.MINUTES);
		System.out.println("\nWe're logged in. The access tokens are meant to last 10 minutes, so we will make"
				+ " periodic queries for the next 17, ending at "+testCompletedAt+ ". This should give enough time to"
				+ " require two refreshes of the access token.");
		System.out.println("We'll just be retrieving statistical codes each time. The values won't be verified,"
				+ " just that they are retrievable.");

		Random generator = new Random();
		retrieveStatCodes(folio, generator);
		while (Instant.now().isBefore(testCompletedAt)) {
			int seconds = 30 + generator.nextInt(30);
			System.out.format("\nWaiting %d seconds\n", seconds);
			Thread.sleep(seconds*1000);
			folio.printLoginStatus(folio);
			retrieveStatCodes(folio, generator);
		}
	}

	private static void retrieveStatCodes(OkapiClient folio, Random generator) throws IOException {
		ReferenceData statCodes = new ReferenceData(folio,"/statistical-codes","code");
		Object[] values = statCodes.dataByName.entrySet().toArray();
		System.out.format("codes: %d; random code: %s\n", values.length,
				values[generator.nextInt(values.length)]);

	}

}
