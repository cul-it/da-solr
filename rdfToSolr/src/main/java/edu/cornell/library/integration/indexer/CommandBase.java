package edu.cornell.library.integration.indexer;

import java.lang.reflect.Constructor;

public class CommandBase {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected static RecordToDocument getRecordToDocumentImpl(String recToDocImplClassName) {
		try{
			Class recToDocImplClass = Class.forName(recToDocImplClassName);		
			Constructor zeroArgCons = recToDocImplClass.getConstructor(null);
			return (RecordToDocument) zeroArgCons.newInstance(null);			
		}catch(Exception ex){
			System.err.println("could not instanciate class " + recToDocImplClassName);
			System.err.print( ex.toString());
			ex.printStackTrace(System.err);
			System.exit(1);
		}	
		return null;
	}
	
}
