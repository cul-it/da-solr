package edu.cornell.library.integration.utilities;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class Excel {

	public static List<Map<String,String>> readExcel(String filename) throws IOException {
		FileInputStream is = new FileInputStream(filename);
		return readExcel(is);
	}

	public static List<Map<String,String>> readExcel(InputStream is) throws IOException {
		XSSFWorkbook wb = new XSSFWorkbook(is);
		int sheets = wb.getNumberOfSheets();
		XSSFSheet sheet = wb.getSheetAt(0);
		Map<Integer,String> headers = new HashMap<>();

		XSSFRow headingRow = sheet.getRow(0);
		for (int col = 0; col < headingRow.getLastCellNum(); col++) {
			String value = headingRow.getCell(col).getStringCellValue();
			headers.put(col, value);
		}
		

		List<Map<String,String>> allData = new ArrayList<>();
		for(int r = 1; r <= sheet.getLastRowNum(); r++) {
			XSSFRow row = sheet.getRow(r);
			Map<String,String> rowData = new HashMap<>();
			if(row != null) {
				for(int col : headers.keySet()) {
					XSSFCell cell = row.getCell(col);
					if (cell == null) continue;
					String value = cell.getStringCellValue();
					if (value != null && ! value.isEmpty())
						rowData.put(headers.get(col), value);
				}
				allData.add(rowData);
			}
		}
		return allData;
	}

	
	public static void main(String[] args) throws IOException {
		List<Map<String,String>> data = readExcel("C:\\Users\\fbw4\\Documents\\authUpdates\\names-that-may-be-fictitious_full.xlsx");
	}
}

