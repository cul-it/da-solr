package edu.cornell.library.integration.authority;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class FlipCandidateTests {

	@Test
	void flippableDatesTests() {

		List<DateFlip> flips= new ArrayList<DateFlip>(Arrays.asList(
				new DateFlip("BEFORE", "AFTER",false),
				new DateFlip("1900-2000", "1900-2000",false),
				new DateFlip("1900-2000", "1900-2000,",false),
				new DateFlip("1900-2000.", "1900-2000",false),
				new DateFlip("1900-2000,", "1900-2000.",false),
				new DateFlip("1900-", "1900-2000",true),
				new DateFlip("1900-,", "1900-2000",true),
				new DateFlip("1900-.", "1900-2000",true),
				new DateFlip("1900-", "1900-2000.",true),
				new DateFlip("1900-", "1900-2000,",true),
				new DateFlip("1905-.", "1900-2000.",false),
				new DateFlip("1905-1986", "1905-",false),
				new DateFlip("-1912", "1869-1912.",true),
				new DateFlip("-65 A.D.", "3 B.C.-65 A.D",true),
				new DateFlip("b. 1905", "1905-",true),
				new DateFlip("b. 1905", "1905-1987",true),
				new DateFlip("b.1905", "1905-",true),
				new DateFlip("d. 1905", "-1905",true),
				new DateFlip("d. 1905", "1855-1905",true),
				new DateFlip("b. 1905", "-1905",false),
				new DateFlip("d. 1905", "1905-",false),
				new DateFlip("1912-ca. 1985", "1912-approximately 1985",true),
				new DateFlip("ca. 4 B.C.-65 A.D.", "approximately 4 B.C.-65 A.D.",true),
				new DateFlip("fl. ca. 4th cent.", "active approximately 4th century",true),
				new DateFlip("fl.ca.4th cent", "active approximately 4th century",true),
				new DateFlip("ca. 3rd cent.", "approximately 3rd century",true),
				new DateFlip("25th cent.", "24th century",false),
				new DateFlip("b. ca. 1912", "approximately 1912-",true),
				new DateFlip("b. ca. 1912", "approximately 1912-1999",true),
				new DateFlip("1912-1999", "1913-1999",true),
				new DateFlip("1912-1999", "1913-1985",false),
				new DateFlip("1912-1999", "1917-1999",false),
				new DateFlip("ca. 1912-1999", "1913-1999",true),
				new DateFlip("ca. 1912-1999", "approximately 1915-1999",true),
				new DateFlip("approximately 1912-1999", "1917-1999",false),
				new DateFlip("1755-approximately 1825", "1755-1824", true),
				new DateFlip("1912-1999", "approximately 1913-1999",true),
				new DateFlip("1912-1999", "approximately 1912-1999",true),
				new DateFlip("1912-1999", "approximately 1912-1997",true),
				new DateFlip("ca. 4 B.C.-65 A.D.", "7 B.C.-65 A.D.",false)
				
				
				));
		for (DateFlip f : flips) {
			boolean actual = ProcessAuthorityChangeFile.flippableDateChange(f.d1, f.d2);
			System.out.printf("%30s %33s %8s\n", f.d1, f.d2, actual);
			assertEquals(f.a, actual);
		}
	}

	private static class DateFlip {
		String d1;
		String d2;
		boolean a;
		DateFlip(String d1, String d2, boolean a) {
			this.d1 = d1;
			this.d2 = d2;
			this.a = a;
		}
	}
}
