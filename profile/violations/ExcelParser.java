import org.apache.poi.ss.usermodel.*;

import java.io.InputStream;
import java.util.Iterator;

public class ExcelParser
{
    private StringBuilder currentString = null;
    private StringBuilder rowString = null;

    public String parseExcelData(InputStream is)
    {
        try {
            Workbook workbook = WorkbookFactory.create(is);
            Sheet sheet = workbook.getSheetAt(0);

            // Iterate through each rows from first sheet
            Iterator<Row> rowIterator = sheet.iterator();
            currentString = new StringBuilder();
            String precinct = "";
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                rowString = new StringBuilder();

                // For each row, iterate through each columns
                Iterator<Cell> cellIterator = row.cellIterator();

                while (cellIterator.hasNext()) {

                    Cell cell = cellIterator.next();

                    switch (cell.getCellTypeEnum()) {
                        case BOOLEAN:
                            rowString.append(cell.getBooleanCellValue() + ",");
                            break;

                        case NUMERIC:
                            rowString.append(cell.getNumericCellValue() + ",");
                            break;

                        case STRING:
                            rowString.append(cell.getStringCellValue().replace('\n', ';') + ",");
                            break;

                        case BLANK:
                            break;
                    }
                }

                if (rowString.toString().toLowerCase().contains("precinct") && rowString.toString().split(",").length < 2 && rowString.toString().split(" ").length < 3) {
                    precinct = rowString.toString();
                } else if (!precinct.isEmpty()) {
                    rowString.insert(0, precinct);
                }

                currentString.append(rowString.append("\n"));
            }

            is.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return currentString.toString();
    }
}