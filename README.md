# CsvReadWrite rfc4180+
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

* trailerFieldCount , set to 0 for no trailer.

## Write\Read a CSV file
```c#

static void Main(string[] args)
{
    string csvFileAbsPath = @"G:\data\csv\test.csv";
    string encoding = "UTF-8";
    int fieldCount = 5;
    int trailerFieldCount = 2;
    string columnDelimiter="|";
    string rowDelimiter="\r\n";
    string qualifier="\"";
    bool isFullQualify=false;
    bool isTrim=true;

    using (CsvStreamWriter csvw = new CsvStreamWriter(csvFileAbsPath, encoding, fieldCount, trailerFieldCount, columnDelimiter, rowDelimiter, qualifier, isFullQualify, isTrim)) 
    {
        //header
        csvw.write(new string[]{"H-A", "H-B", "H-C", "H-D", "H-E" });

        //2 rows
        csvw.write(new string[]{"A1", "B\"\"B1", "C", "D", "E" });
        csvw.write(new string[]{"A2", "B", "C\r\n\"\"C2|\"\"C3", "D", "E" });

        //trailer
        csvw.writeTrailer(new string[]{"2", "20210902" });
    }

    bool rowDelimiterAcceptAllLineChange_rn_r_n = true;
    bool columnCountFromFirstRow = true;
    bool isEscapeQualifer = true;
    
    using(CsvStreamReader csvr = 
    new CsvStreamReader(csvFileAbsPath, encoding, columnDelimiter, rowDelimiter, qualifier, rowDelimiterAcceptAllLineChange_rn_r_n, 
                        columnCountFromFirstRow, isEscapeQualifer, trailerFieldCount))
    {
        csvr.TrimFields = true;
        //if(!ColumnCountFromFirstRow) csv.ColumnCount = {n};
        while(csvr.Read())
        {
                 for(int i=0;i<csvr.FieldsCount;i++)
                 {
                     Console.Write(csvr[i] + "|");
                 }

                 Console.WriteLine();
        }

        Console.WriteLine("-------------------------------------trailer------------------------------------");

        List<string> trailer = csvr.TrailerFields;
        foreach(string s in trailer) 
        { 
            Console.Write(s + "|");
        }

        Console.WriteLine();
    }

    Console.ReadKey();
}

```
