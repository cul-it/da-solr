package edu.cornell.library.integration.utilities;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.FolioClient;
import edu.cornell.library.integration.folio.ReferenceData;

public class CollectNonMarcFieldData {

	public static void main(String[] args) throws Exception {


		Config config = Config.loadConfig(Config.getRequiredArgsForDB("Current"));
		FolioClient folio = config.getFolio("Folio");

		Map<String,Integer> noteTypesCounts = new HashMap<>();
		Map<String,Integer> publicNoteTypesCounts = new HashMap<>();
		ReferenceData instanceNoteTypes = new ReferenceData( folio, "/instance-note-types","name");

		Map<String,Integer> identifierTypesCounts = new HashMap<>();
		ReferenceData identifierTypes = new ReferenceData( folio,  "/identifier-types","name");

		int totalSubjects = 0;

		try (Connection inventory = config.getDatabaseConnection("Current");
			 Statement stmt = inventory.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT * FROM instanceFolio WHERE source = 'FOLIO'")){
			int counted = 0;
			Map<String,Integer> fieldsFoundTotal = new HashMap<>();
			while (rs.next()) {
				Map<String,Object> instance = mapper.readValue(rs.getString("content"), Map.class);
				if (isSuppressed(instance)) continue;
				counted++;
				for (String field : instance.keySet()) {
					Object value = instance.get(field);
					if (isEmpty(value)) {
						System.out.println("EMPTY "+field+": "+mapper.writeValueAsString(instance.get(field)));
						continue;
					}
					if ( ! fieldsFoundTotal.containsKey(field))
						fieldsFoundTotal.put(field, 1);
					else
						fieldsFoundTotal.put(field, fieldsFoundTotal.get(field)+1);

					if (field.equals("notes")) {
						for (Map<String,Object> note : (List<Map<String,Object>>)value) {
							String noteType = instanceNoteTypes.getName((String)note.get("instanceNoteTypeId"));
							if (noteTypesCounts.containsKey(noteType))
								noteTypesCounts.put(noteType, noteTypesCounts.get(noteType)+1);
							else
								noteTypesCounts.put(noteType, 1); 
							if ( (boolean)note.getOrDefault("staffOnly", false) ) continue ;
							if (! noteType.equals("General note"))
								System.out.format("%s '%s' note %s\n", (String)instance.get("hrid"),noteType,
										mapper.writeValueAsString(instance.get(field)));
							if (publicNoteTypesCounts.containsKey(noteType))
								publicNoteTypesCounts.put(noteType, publicNoteTypesCounts.get(noteType)+1);
							else
								publicNoteTypesCounts.put(noteType, 1); 
						}
					}
					if (field.equals("identifiers")) {
						for (Map<String,String> identifier : (List<Map<String,String>>)value) {
							String identifierType = identifierTypes.getName(identifier.get("identifierTypeId"));
							if (identifierTypesCounts.containsKey(identifierType))
								identifierTypesCounts.put(identifierType, identifierTypesCounts.get(identifierType)+1);
							else
								identifierTypesCounts.put(identifierType, 1);
							if ( ! identifierType.equals("ISBN"))
								System.out.format("%s '%s' identifier %s\n", (String)instance.get("hrid"),identifierType,
										mapper.writeValueAsString(instance.get(field)));
						}
					}
					if (field.equals("subjects")) {
						System.out.format("%s: %s\n",instance.getOrDefault("hrid", ""),mapper.writeValueAsString(value));
						totalSubjects += ((List)value).size();
//						List<Map<String,String>> values = (List)value;
//						for (Map<String,String> subject : values) if ( ! isEmpty(subject) ) totalSubjects++;
					}

				}
			}
			for (String field : fieldsFoundTotal.keySet())
				System.out.format("%s %d\n",field,fieldsFoundTotal.get(field));
			System.out.println();
			for (String noteType : noteTypesCounts.keySet())
				System.out.format("note type '%s': %d\n",noteType,noteTypesCounts.get(noteType));
			for (String noteType : publicNoteTypesCounts.keySet())
				System.out.format("public note type '%s': %d\n",noteType,noteTypesCounts.get(noteType));
			for (String idType : identifierTypesCounts.keySet())
				System.out.format("identifer type '%s': %d\n",idType,identifierTypesCounts.get(idType));
			System.out.format("total subject count: %d\n", totalSubjects);
			System.out.format("%d records tabulated\n",counted);
		}

	}
	private static boolean isSuppressed(Map<String, Object> instance) {
		if (((boolean)instance.getOrDefault("discoverySuppress", false))
				|| ((boolean)instance.getOrDefault("staffSuppress", false)))
			return true;
		return false;
	}
	private static boolean isEmpty(Object value) throws JsonProcessingException {
		if (value == null) return true;
		switch (value.getClass().getCanonicalName()) {
		case "java.lang.Boolean": return false;
		case "java.lang.String": return ((String)value).isBlank();
		case "java.util.ArrayList": {
			boolean allEmpty = true;
			for (Object element : (List)value)
				if ( ! isEmpty(element)) allEmpty = false;
			return allEmpty;
		}
		case "java.util.LinkedHashMap": {
			Map<String,Object> map = (Map)value;
			if (map.isEmpty()) return true;
			boolean allEmpty = true;
			for (String key : map.keySet())
				if ( ! key.isBlank() && ! isEmpty(map.get(key)))
					allEmpty = false;
			return allEmpty;
		}
		case "java.lang.Integer": return false;
		}

		System.out.println(value.getClass().getCanonicalName()+" "+mapper.writeValueAsString(value));
		return false;
	}
	static ObjectMapper mapper = new ObjectMapper();
}
