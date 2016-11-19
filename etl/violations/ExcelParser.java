import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ExcelParser
{
    private StringBuilder currentString = null;

    public String parseExcelData(InputStream is)
    {
        try {
            XSSFWorkbook workbook = new XSSFWorkbook(is);
            XSSFSheet sheet = workbook.getSheetAt(0);

            // Iterate through each rows from first sheet
            Iterator<Row> rowIterator = sheet.iterator();
            currentString = new StringBuilder();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();

                // For each row, iterate through each columns
                Iterator<Cell> cellIterator = row.cellIterator();

                while (cellIterator.hasNext()) {

                    Cell cell = cellIterator.next();

                    switch (cell.getCellTypeEnum()) {
                        case BOOLEAN:
                            currentString.append(cell.getBooleanCellValue() + ",");
                            break;

                        case NUMERIC:
                            currentString.append(cell.getNumericCellValue() + ",");
                            break;

                        case STRING:
                            currentString.append(cell.getStringCellValue() + ",");
                            break;

                        case BLANK:
                            break;
                    }
                }
            }

            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return currentString.toString();
    }
}