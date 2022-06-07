using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using CsvReadWrite;

using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Data.SqlClient;
using System.Data.Common;
using System.Security.Cryptography;

namespace CsvToSQLTable
{
    public class _DataColumn : DataColumn
    {
        public string SqlDataType { get; set; }
        public bool IsPrimaryKey { get; set; }
        public int csvPosition { get; set; }
        public bool IsText { get; set; }
        public string CollationName { get; set; }
    }

    class Program
    {
        private static int SQL_COM_TIMEOUT = 1800;
        private static int _Int_DataLoadBuffSize = 20 * 1024 * 1024;
        static void Main(string[] args)
        {
            string _Str_CsvFilePath = ConfigurationManager.AppSettings.Get("CsvFilePath");
            string _Str_CsvColumnDelimiter = ConfigurationManager.AppSettings.Get("CsvColumnDelimiter");
            string _Str_CsvRowDelimiter = ConfigurationManager.AppSettings.Get("CsvRowDelimiter");
            string _Str_CsvTextQualifier = ConfigurationManager.AppSettings.Get("CsvTextQualifier");
            string _Str_CsvEncoding = ConfigurationManager.AppSettings.Get("CsvEncoding");
            string _Str_ADODBConn = ConfigurationManager.AppSettings.Get("ADODBConn");
            string _Str_SchemaName = ConfigurationManager.AppSettings.Get("SchemaName");

            int _Int_CsvTrailerFieldCount = int.Parse(ConfigurationManager.AppSettings.Get("CsvTrailerFieldCount"));
            int _Int_ColumnSize = int.Parse(ConfigurationManager.AppSettings.Get("ColumnSize"));

            if (args.Length > 0)
            {
                if (args[0].ToLower() == "-t")
                {
                    List<string> columns = null;

                    using (CsvStreamReader csvReader = new CsvStreamReader(
                         _Str_CsvFilePath
                       , _Str_CsvEncoding
                       , _Str_CsvColumnDelimiter
                       , _Str_CsvRowDelimiter
                       , _Str_CsvTextQualifier
                       , true
                       , true
                       , _Int_CsvTrailerFieldCount))
                    {
                        csvReader.TrimFields = true;

                        while (csvReader.Read())
                        {
                            columns = new List<string>();
                            for (int i = 0; i < csvReader.FieldsCount; i++)
                            {
                                columns.Add($"[{csvReader[i].ToUpper().Trim()}] [NVARCHAR]({_Int_ColumnSize})");
                            }

                            break;
                        }
                    }

                    string schemaName = _Str_SchemaName;
                    string tableName = Path.GetFileNameWithoutExtension(_Str_CsvFilePath);

                    CreateTableDropIfExists(_Str_ADODBConn, schemaName, tableName, columns);

                    Int64 purgeCount = ProcessTruncate(_Str_ADODBConn, schemaName, tableName);
                    Console.WriteLine($"[{schemaName}].[{tableName}] truncated {purgeCount} rows");

                    ProcessDataLoad(_Str_ADODBConn, schemaName, tableName);
                }
            }
            else
            {


                List<string> csvHeader = null;

                using (CsvStreamReader csvReader = new CsvStreamReader(
                    _Str_CsvFilePath
                   , _Str_CsvEncoding
                   , _Str_CsvColumnDelimiter
                   , _Str_CsvRowDelimiter
                   , _Str_CsvTextQualifier
                   , true
                   , true
                   , _Int_CsvTrailerFieldCount))
                {
                    csvReader.TrimFields = true;

                    while (csvReader.Read())
                    {
                        csvHeader = new List<string>();
                        for (int i = 0; i < csvReader.FieldsCount; i++)
                        {
                            csvHeader.Add(csvReader[i].ToUpper().Trim());
                        }

                        break;
                    }
                }

                int idx = 0;
                foreach (string h in csvHeader)
                {
                    Console.WriteLine($"[{idx++}] {h}");
                }

            read_user_input_column:
                Console.WriteLine();
                Console.WriteLine("---Please select column to search");
                Console.WriteLine();

                string selected_column_idx = Console.ReadLine().Trim();
                selected_column_idx = selected_column_idx.Equals("") ? "0" : selected_column_idx;
                int search_column_idx = int.Parse(selected_column_idx);

                if (search_column_idx < csvHeader.Count)
                {
                    Console.WriteLine($"User selected => [{selected_column_idx}] [{csvHeader[search_column_idx]}] as Column");
                }
                else
                {
                    goto read_user_input_column;
                }


            read_user_input_search_value:
                Console.WriteLine();
                Console.WriteLine("---Please input a value to search");
                Console.WriteLine();

                string selected_search_value = Console.ReadLine().Trim();
                selected_search_value = selected_search_value.Equals("") ? "" : selected_search_value;

                if (selected_search_value.Length > 1)
                {
                    Console.WriteLine($"User input search value => [{selected_search_value}]");
                }
                else
                {
                    goto read_user_input_search_value;
                }

                Console.WriteLine();


                using (CsvStreamReader csvReader = new CsvStreamReader(
                    _Str_CsvFilePath
                   , _Str_CsvEncoding
                   , _Str_CsvColumnDelimiter
                   , _Str_CsvRowDelimiter
                   , _Str_CsvTextQualifier
                   , true
                   , true
                   , _Int_CsvTrailerFieldCount))
                {
                    csvReader.TrimFields = true;
                    csvReader.Read();

                    long row_number = 0;
                    while (csvReader.Read())
                    {
                        row_number++;

                        bool is_match = csvReader[search_column_idx].Equals(selected_search_value, StringComparison.OrdinalIgnoreCase);
                        bool is_part_match = csvReader[search_column_idx].IndexOf(selected_search_value, StringComparison.OrdinalIgnoreCase) >= 0;

                        if (is_match | is_part_match)
                        {
                            Console.WriteLine($"row_number[{row_number}] is_match[{is_match}] is_part_match[{is_part_match}] :");

                            for (int i = 0; i < csvReader.FieldsCount; i++)
                            {
                                Console.Write($"{_Str_CsvTextQualifier}{csvReader[i]}{_Str_CsvTextQualifier}{_Str_CsvColumnDelimiter}");
                            }

                            Console.Write($"{_Str_CsvRowDelimiter}");
                            Console.WriteLine();
                        }
                    }
                }

                Console.WriteLine();
                Console.WriteLine("-------------------Search Completed-------------------");
                Console.WriteLine();
                Console.WriteLine("Press Enter to finish.");
                Console.ReadLine();
            }
        }

        private static Int64 Append(DataTable dataTable, string connString, string schema_name, string table_name)
        {
            Int64 _LoadCount = 0;

            if (null == dataTable || dataTable.Rows.Count <= 0)
            {
                return _LoadCount;
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connString, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.Default))
            {
                bulkCopy.BulkCopyTimeout = SQL_COM_TIMEOUT;
                bulkCopy.DestinationTableName = $"[{schema_name}].[{table_name}]";
                bulkCopy.WriteToServer(dataTable);
                _LoadCount += dataTable.Rows.Count;
            }

            return _LoadCount;
        }

        private static void CreateTableDropIfExists(string connString, string schema_name, string table_name, List<string> columns)
        {
            using (SqlConnection conn = new SqlConnection(connString))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    command.CommandTimeout = SQL_COM_TIMEOUT;
                    conn.Open();
                    try
                    {
                        command.CommandText = $"DROP TABLE [{schema_name}].[{table_name}];";
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        // Handle exception properly
                    }

                    string ddl = $"CREATE TABLE [{schema_name}].[{table_name}](";

                    ddl = ddl + String.Join(",", columns);

                    ddl = ddl + ");";

                    try
                    {
                        command.CommandText = ddl;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        // Handle exception properly
                        throw ex;
                    }
                    finally
                    {
                        conn.Close();
                    }

                }
            }
        }

        private static Int64 ProcessTruncate(string connString, string schema_name, string table_name)
        {
            Int64 purgeCount = 0;

            using (SqlConnection conn = new SqlConnection(connString))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    command.CommandTimeout = SQL_COM_TIMEOUT;

                    try
                    {
                        conn.Open();
                        command.CommandText = $"SELECT CAST(COUNT(*) AS BIGINT) AS CT FROM [{schema_name}].[{table_name}];";
                        purgeCount = (Int64)(command.ExecuteScalar() ?? 0);

                        command.CommandText = $"TRUNCATE TABLE [{schema_name}].[{table_name}];";
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        // Handle exception properly
                        throw ex;
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }

            return purgeCount;
        }

        private static DataTable GetDataTable(DataTable schemaTable, string schema_name, string table_name)
        {
            DataTable dataTable = new DataTable($"[{schema_name}].[{table_name}]");
            foreach (DataRow row in schemaTable.Rows)
            {
                _DataColumn dataTableColumn = new _DataColumn();

                dataTableColumn.DataType = (Type)row["DataType"];
                dataTableColumn.ColumnName = row["ColumnName"].ToString();
                dataTableColumn.AllowDBNull = (Boolean)row["AllowDBNull"];
                dataTableColumn.DefaultValue = (object)row["DefaultValue"];
                dataTableColumn.SqlDataType = (string)row["SqlDataType"];
                dataTableColumn.IsPrimaryKey = (bool)row["IsPrimaryKey"];
                dataTableColumn.IsText = (bool)row["IsText"];
                dataTableColumn.CollationName = row["CollationName"].ToString();
                dataTableColumn.csvPosition = -1;

                dataTable.Columns.Add(dataTableColumn);
            }

            return dataTable;
        }

        private static DataTable GetReadingTableSchema(string connectionString, string schema_name, string table_name)
        {
            DataTable schemaDataTable = null;

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string sql = $"SELECT TOP 1 * FROM [{schema_name}].[{table_name}] WHERE 1=2";
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.CommandTimeout = SQL_COM_TIMEOUT;

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        schemaDataTable = reader.GetSchemaTable();
                    }
                }
                conn.Close();
            }

            if (!schemaDataTable.Columns.Contains("DefaultValue"))
            {
                schemaDataTable.Columns.Add("DefaultValue", typeof(String));
            }

            if (!schemaDataTable.Columns.Contains("SqlDataType"))
            {
                schemaDataTable.Columns.Add("SqlDataType", typeof(String));
            }

            if (!schemaDataTable.Columns.Contains("IsPrimaryKey"))
            {
                schemaDataTable.Columns.Add("IsPrimaryKey", typeof(bool));
            }

            if (!schemaDataTable.Columns.Contains("IsText"))
            {
                schemaDataTable.Columns.Add("IsText", typeof(bool));
            }

            if (!schemaDataTable.Columns.Contains("CollationName"))
            {
                schemaDataTable.Columns.Add("CollationName", typeof(String));
            }

            foreach (DataRow dr in schemaDataTable.Rows)
            {
                string columnDataTypeName = dr["DataTypeName"].ToString().ToLower();
                string sqlsc = "";

                switch (columnDataTypeName)
                {
                    case "char":
                    case "nchar":
                    case "varchar":
                    case "nvarchar":
                        sqlsc += string.Format(" " + columnDataTypeName + "({0}) ", Int32.Parse(dr["ColumnSize"].ToString()) == -1 ? "max" : dr["ColumnSize"].ToString());
                        break;
                    case "numeric":
                    case "decimal":
                        sqlsc += string.Format(" " + columnDataTypeName + "({0},{1}) ", dr["NumericPrecision"].ToString(), dr["NumericScale"].ToString());
                        break;
                    case "float":
                        sqlsc += string.Format(" " + columnDataTypeName + "({0}) ", dr["NumericPrecision"].ToString());
                        break;
                    default:
                        sqlsc += columnDataTypeName;
                        break;
                }

                dr["SqlDataType"] = sqlsc;
                dr["IsText"] = (columnDataTypeName == "text" || columnDataTypeName == "ntext");
            }

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string sql = "SELECT COLUMN_NAME, COLUMN_DEFAULT, COLLATION_NAME AS COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=@TABLE_SCHEMA AND TABLE_NAME = @TABLE_NAME;";
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.CommandTimeout = SQL_COM_TIMEOUT;

                    cmd.Parameters.AddWithValue("@TABLE_SCHEMA", schema_name);
                    cmd.Parameters.AddWithValue("@TABLE_NAME", table_name);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string columnName = reader[0].ToString();
                            string defaultValue = null;
                            string collationName = null;

                            if (null != reader[1] && reader[1].ToString().Length > 0)
                            {
                                defaultValue = reader[1].ToString().Replace("(", "").Replace(")", "").Replace("'", "");

                                foreach (DataRow dr in schemaDataTable.Rows)
                                {
                                    if (dr["ColumnName"].ToString().ToUpper().Equals(columnName.ToUpper()))
                                    {
                                        dr["DefaultValue"] = defaultValue;
                                    }
                                }
                            }

                            if (null != reader[2] && reader[2].ToString().Length > 0)
                            {
                                collationName = reader[2].ToString();

                                foreach (DataRow dr in schemaDataTable.Rows)
                                {
                                    if (dr["ColumnName"].ToString().ToUpper().Equals(columnName.ToUpper()))
                                    {
                                        dr["CollationName"] = collationName;
                                    }
                                }
                            }
                        }
                    }
                }
                conn.Close();
            }

            foreach (DataRow dr in schemaDataTable.Rows)
            {
                dr["IsPrimaryKey"] = false;
            }

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string sql = "select cu.COLUMN_NAME from INFORMATION_SCHEMA.TABLE_CONSTRAINTS c join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu on c.CONSTRAINT_SCHEMA=cu.CONSTRAINT_SCHEMA and c.CONSTRAINT_NAME=cu.CONSTRAINT_NAME WHERE c.CONSTRAINT_TYPE='PRIMARY KEY' and c.TABLE_SCHEMA = @TABLE_SCHEMA and c.TABLE_NAME= @TABLE_NAME;";
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.CommandTimeout = SQL_COM_TIMEOUT;

                    cmd.Parameters.AddWithValue("@TABLE_SCHEMA", schema_name);
                    cmd.Parameters.AddWithValue("@TABLE_NAME", table_name);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string columnName = reader[0].ToString();

                            foreach (DataRow dr in schemaDataTable.Rows)
                            {
                                if (dr["ColumnName"].ToString().ToUpper().Equals(columnName.ToUpper()))
                                {
                                    dr["IsPrimaryKey"] = true;
                                }
                            }
                        }
                    }
                }

                conn.Close();
            }

            return schemaDataTable;
        }

        private static void ProcessDataLoad(string connectionString, string schema_name, string table_name)
        {
            string _Str_CsvFilePath = ConfigurationManager.AppSettings.Get("CsvFilePath");
            string _Str_CsvColumnDelimiter = ConfigurationManager.AppSettings.Get("CsvColumnDelimiter");
            string _Str_CsvRowDelimiter = ConfigurationManager.AppSettings.Get("CsvRowDelimiter");
            string _Str_CsvTextQualifier = ConfigurationManager.AppSettings.Get("CsvTextQualifier");
            string _Str_CsvEncoding = ConfigurationManager.AppSettings.Get("CsvEncoding");

            string _Str_ADODBConn = ConfigurationManager.AppSettings.Get("ADODBConn");
            string _Str_SchemaName = ConfigurationManager.AppSettings.Get("SchemaName");

            int _Int_CsvTrailerFieldCount = int.Parse(ConfigurationManager.AppSettings.Get("CsvTrailerFieldCount"));

            Int64 TotalLoadCount = 0;
            var watch = System.Diagnostics.Stopwatch.StartNew();


            DataTable dataTable = GetDataTable(GetReadingTableSchema(connectionString, schema_name, table_name), schema_name, table_name);

            List<string> csvHeader = null;

            using (CsvStreamReader csvReader = new CsvStreamReader(_Str_CsvFilePath
                       , _Str_CsvEncoding
                       , _Str_CsvColumnDelimiter
                       , _Str_CsvRowDelimiter
                       , _Str_CsvTextQualifier
                       , true
                       , true
                       , _Int_CsvTrailerFieldCount))
            {
                csvReader.TrimFields = true;

                while (csvReader.Read())
                {
                    csvHeader = new List<string>();
                    for (int i = 0; i < csvReader.FieldsCount; i++)
                    {
                        csvHeader.Add(csvReader[i].ToUpper().Trim());
                    }

                    break;
                }
            }

            string compareResult = "";
            List<string> tabColumnNames = (from dc in dataTable.Columns.Cast<_DataColumn>()
                                           select dc.ColumnName.ToUpper()).ToList<string>();

            foreach (string h in tabColumnNames)
            {
                if (!csvHeader.Contains(h))
                {
                    compareResult = compareResult + "<Tab>" + h + "|";
                }
            }

            foreach (string h in csvHeader)
            {
                if (!tabColumnNames.Contains(h))
                {
                    compareResult = compareResult + "<Csv>" + h + "|";
                }
            }

            Console.WriteLine($"[{schema_name}].[{table_name}] Column Compare between Table and Csv file: diff =>[{compareResult}]");

            if (null != csvHeader)
            {
                foreach (_DataColumn col in dataTable.Columns.Cast<_DataColumn>())
                {
                    col.csvPosition = csvHeader.FindIndex(a => a == col.ColumnName.ToUpper());
                }
            }

            using (CsvStreamReader csvReader = new CsvStreamReader(_Str_CsvFilePath
                       , _Str_CsvEncoding
                       , _Str_CsvColumnDelimiter
                       , _Str_CsvRowDelimiter
                       , _Str_CsvTextQualifier
                       , true
                       , true
                       , _Int_CsvTrailerFieldCount))
            {
                using (SHA1Managed sha1 = new SHA1Managed())
                {
                    //MessageBox.Show("using (SHA1Managed sha1 = new SHA1Managed())");

                    csvReader.TrimFields = false;

                    string[] dataRow = null;
                    int currentBuffSize = 0;

                    //Skip header line
                    if (csvReader.Read())
                    {
                        int dataRowSize = csvHeader.Count;

                        while (csvReader.Read())
                        {
                            dataRow = new string[dataRowSize];

                            //MessageBox.Show("dataRowSize "+ dataRowSize + "");

                            for (int i = 0; i < csvReader.FieldsCount; i++)
                            {
                                dataRow[i] = csvReader[i];
                                currentBuffSize += null == csvReader[i] ? 0 : csvReader[i].Length;
                            }

                            DataRow loadDataRow = dataTable.NewRow();

                            foreach (_DataColumn col in dataTable.Columns.Cast<_DataColumn>())
                            {
                                if (col.csvPosition >= 0)
                                {
                                    string value = "";

                                    if (!col.IsText)
                                    {
                                        value = dataRow[col.csvPosition];
                                    }
                                    else
                                    {
                                        value = dataRow[col.csvPosition];
                                    }

                                    if (0 == col.SqlDataType.ToLower().IndexOf("char") || 0 == col.SqlDataType.ToUpper().IndexOf("varchar") || 0 == col.SqlDataType.ToUpper().IndexOf("text"))
                                    {
                                        if (null != value)
                                        {
                                            byte[] data = Encoding.GetEncoding(_Str_CsvEncoding).GetBytes(value);
                                            value = Encoding.GetEncoding("windows-1252").GetString(
                                                        Encoding.Convert(Encoding.GetEncoding(_Str_CsvEncoding), Encoding.GetEncoding("windows-1252"), data)
                                                    );
                                        }
                                    }

                                    if (null == value || value.Length == 0)
                                    {
                                        if (null != col.DefaultValue && col.DefaultValue.ToString().Length > 0 && col.AllowDBNull == false)
                                        {
                                            value = col.DefaultValue.ToString();
                                        }
                                    }

                                    //MessageBox.Show("Convert.ChangeType: " + value + " " + col.ColumnName + " " + col.DataType);
                                    if (!(col.DataType.Name.ToUpper().IndexOf("CHAR") >= 0 || col.DataType.Name.ToUpper().IndexOf("TEXT") >= 0))
                                    {
                                        if (value != null && (value.ToUpper() == "NULL" || value.Length == 0))
                                        {
                                            value = null;
                                        }
                                    }

                                    if (col.DataType == Type.GetType("System.Boolean"))
                                    {
                                        switch ((value ?? "").ToLower())
                                        {
                                            case null:
                                            case "":
                                                loadDataRow[col.ColumnName] = DBNull.Value;
                                                break;
                                            case "1":
                                            case "true":
                                            case "t":
                                            case "yes":
                                            case "y":
                                                loadDataRow[col.ColumnName] = Convert.ChangeType("true", col.DataType);
                                                break;
                                            default:
                                                loadDataRow[col.ColumnName] = Convert.ChangeType("false", col.DataType);
                                                break;
                                        }
                                    }
                                    else
                                    {
                                        loadDataRow[col.ColumnName] = (null == value || value.Length == 0 ? DBNull.Value : Convert.ChangeType(value, col.DataType));
                                    }

                                    //MessageBox.Show("Convert.ChangeType done");
                                }
                            }

                            dataTable.Rows.Add(loadDataRow);

                            //MessageBox.Show("dataTable.Rows.Add(loadDataRow);");

                            if (currentBuffSize >= _Int_DataLoadBuffSize)
                            {
                                Int64 _LoadCount = Append(dataTable, _Str_ADODBConn, schema_name, table_name);
                                TotalLoadCount = TotalLoadCount + _LoadCount;

                                Console.WriteLine($"[{schema_name}].[{table_name}] loaded a batch BuffSize[{currentBuffSize}], rows[{dataTable.Rows.Count}], LoadCount[{_LoadCount}], TotalLoadCount[{TotalLoadCount}] Execution Time: {watch.ElapsedMilliseconds / 1000.0} sec");

                                dataTable.Clear();
                                currentBuffSize = 0;
                            }
                        }

                        Int64 loadCount = Append(dataTable, _Str_ADODBConn, schema_name, table_name);
                        TotalLoadCount = TotalLoadCount + loadCount;

                        Console.WriteLine($"[{schema_name}].[{table_name}] loaded final batch BuffSize[{currentBuffSize}], rows[{dataTable.Rows.Count}], LoadCount[{loadCount}], TotalLoadCount[{TotalLoadCount}] Execution Time: {watch.ElapsedMilliseconds / 1000.0} sec");

                        dataTable.Clear();
                        currentBuffSize = 0;
                    }
                }
            }

            watch.Stop();
        }

    }
}
