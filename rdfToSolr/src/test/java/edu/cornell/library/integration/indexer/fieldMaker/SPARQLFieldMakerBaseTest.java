package edu.cornell.library.integration.indexer.fieldMaker;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SPARQLFieldMakerBaseTest {
    public SPARQLFieldMakerBase fMaker;

    public SPARQLFieldMakerBaseTest(SPARQLFieldMakerBase fMaker) {
        this.fMaker = fMaker;
    }

    @Test
    public void testEmptyMaker() throws Exception{
    	fMaker.buildFields("bogus", null);
    }

    @Parameterized.Parameters
    public static List<Object[]> instancesToTest() {
        return Arrays.asList(
                    new Object[]{new SPARQLFieldMakerImpl() },
                    new Object[]{new SPARQLFieldMakerImpl() }                    
        );
    }
}
