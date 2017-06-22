package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.nodeToString;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

public class JenaResultsToMarcRecord {

	public static void addControlFieldResultSet( final MarcRecord rec, final ResultSet rs ) {
		addControlFieldResultSet(rec,rs,false);
	}
	public static void addControlFieldResultSet( final MarcRecord rec, final ResultSet rs, boolean nonBreaking ) {
		while (rs.hasNext()) {
			final QuerySolution sol = rs.nextSolution();
			addControlFieldQuerySolution( rec, sol, nonBreaking );
		}

	}

	public static void addControlFieldQuerySolution( final MarcRecord rec, final QuerySolution sol ) {
		addControlFieldQuerySolution(rec,sol,false);
	}
	public static void addControlFieldQuerySolution(
			final MarcRecord rec, final QuerySolution sol, boolean nonBreaking ) {
		final String f_uri = nodeToString( sol.get("field") );
		final Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
		final ControlField f = new ControlField(field_no,
				nodeToString(sol.get("tag")),nodeToString(sol.get("value")));
		if (nonBreaking)
			f.value = f.value.replaceAll(" ", "\u00A0");
		rec.controlFields.add(f);
		if (f.tag.equals("001"))
			rec.id = f.value;
		else if (f.tag.equals("005"))
			rec.modifiedDate = f.value;
	}

	public static void addDataFieldResultSet( final MarcRecord rec, final ResultSet rs ) {
		while( rs.hasNext() ){
			final QuerySolution sol = rs.nextSolution();
			addDataFieldQuerySolution(rec, sol, null);
		}
	}
	public static void addDataFieldResultSet( final MarcRecord rec, final ResultSet rs, final String mainTag ) {
		while( rs.hasNext() ){
			final QuerySolution sol = rs.nextSolution();
			addDataFieldQuerySolution(rec, sol,mainTag);
		}
	}

	public static void addDataFieldQuerySolution( final MarcRecord rec, final QuerySolution sol ) {
		addDataFieldQuerySolution(rec, sol, null);
	}

	public static void addDataFieldQuerySolution(
			final MarcRecord rec, final QuerySolution sol, final String mainTag ) {
		final String f_uri = nodeToString( sol.get("field") );
		final Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
		final String sf_uri = nodeToString( sol.get("sfield") );
		final Integer sfield_no = Integer.valueOf( sf_uri.substring( sf_uri.lastIndexOf('_') + 1 ) );
		DataField f = null;
		for (DataField df : rec.dataFields)
			if (df.id == field_no)
				f = df;
		if (f == null) {
			f = new DataField();
			f.id = field_no;
			f.tag = nodeToString( sol.get("tag"));
			f.ind1 = nodeToString(sol.get("ind1")).charAt(0);
			f.ind2 = nodeToString(sol.get("ind2")).charAt(0);
			if (sol.contains("p")) {
				final String p = nodeToString(sol.get("p"));
				f.mainTag = p.substring(p.length() - 3);
			} else if (mainTag != null)
				f.mainTag = mainTag;
		}
		final Subfield sf = new Subfield(sfield_no,
				nodeToString( sol.get("code")).charAt(0), nodeToString( sol.get("value")));
		if (sf.code.equals('6')) {
			if ((sf.value.length() >= 6) && Character.isDigit(sf.value.charAt(4))
					&& Character.isDigit(sf.value.charAt(5))) {
				f.linkNumber = Integer.valueOf(sf.value.substring(4, 6));
			}
		}
		f.subfields.add(sf);
		rec.dataFields.add(f);

	}


}
