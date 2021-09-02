# CsvReadWrite
### a simple csv read/write library, support multiple character Row Delimiter and auto fit different line change marks.
* support multiple character Row Delimiter and auto fit different line change marks.
* support multiple character Column Delimiter.
* support multiple character Text Qualifier.

## Supported parameters
* csvFileAbsPath  => csv file path
* columnDelimiter => column delimiter e.g. [,|'~] or ||  $$
* rowDelimiter    => row delimiter e.g. \r or \n or \r\n or |\r\n or |$$\r\n
* textQualifier   => text qualifier e.g. " ' $
* Encoding        => encoding
* RowDelimiterAcceptAllLineChange_rn_r_n => flag for turn on auto fit line change in row delimiter
* ColumnCountFromFirstRow => retrive column count from first row, if set to false please provide expected column count.

## Reading a CSV file
```c#
using(IO.CsvStreamReader csv = new IO.CsvStreamReader(csvFileAbsPath, Encoding, ColumnDelimiter, RowDelimiter, TextQualifier, RowDelimiterAcceptAllLineChange_rn_r_n, ColumnCountFromFirstRow, TrailerFieldsCount))
{
    csv.TrimFields = True;
    //if(!ColumnCountFromFirstRow) csv.ColumnCount = {n};
    while(csv.Read())
    {
             for(int i=0;i<csv.FieldCount;i++)
             {
                 Console.Write(csv[i]);
             }
    }

    List<string> trailer = csv.TrailerFields;
}
```
