using System;
using System.IO;
using System.Text;


namespace CsvReadWrite
{
	public class CsvStreamWriter : IDisposable
	{
		public string strRowDelimiter { get; private set; }

		public string strColumnDelimiter { get; private set; }
		public string strQualifier { get; private set; }
		public string Encoding { get; private set; }
		public long CsvLength { get; private set; }
		public string CsvFileAbsolutePath { get; private set; }

		private TextWriter csv = null;

		public int FieldCount { get; private set; }
		public int TrailerFieldCount { get; private set; }
		public bool IsFullQualify{ get; private set; }
		public bool IsTrim{ get; private set; }

		public CsvStreamWriter(string csvFileAbsolutePath, string encoding, int fieldCount, int trailerFieldCount, string columnDelimiter=",", string rowDelimiter="\r\n", string qualifier="\"", bool isFullQualify=false, bool isTrim=true)
		{
			CsvFileAbsolutePath = csvFileAbsolutePath;
			if ((CsvFileAbsolutePath ?? "").Length == 0)
				throw new ArgumentException("Csv File Absolute Path cannot be empty.");

			strColumnDelimiter = (columnDelimiter ?? "");

			if (strColumnDelimiter.Length == 0)
				throw new ArgumentException("Column Delimiter cannot be empty.");

			strRowDelimiter = (rowDelimiter ?? "");
			if (!strRowDelimiter.EndsWith("\r") && !strRowDelimiter.EndsWith("\n"))
			{
				StringBuilder msg = new StringBuilder();
				msg.Append("unexpected format => " + this.CsvFileAbsolutePath + " ");
				msg.Append("RowDelimiter missing line change character (\\r or \\n or \\r\\n)");
				msg.Append("please also see isRowDelimiterAcceptDifferentLineChangeCharacter ");
				msg.Append("with isRowDelimiterAcceptDifferentLineChangeCharacter Reader will treat \\r or \\n or \\r\\n same");

				throw new CsvUnexpectedFormatException(msg.ToString());
			}

			if (strRowDelimiter.Length == 0)
				throw new ArgumentException("Row Delimiter cannot be empty.");

			if ("" == (qualifier ?? ""))
			{
				strQualifier = "\"";
			}
			else
			{
				strQualifier = qualifier;
			}

			if (strRowDelimiter == strQualifier || strColumnDelimiter == strQualifier || strColumnDelimiter == strRowDelimiter)
			{
				StringBuilder msg = new StringBuilder();
				msg.Append("unexpected format => " + this.CsvFileAbsolutePath + " ");
				msg.Append("Qualifier or ColumnDelimiter same as RowDelimiter ");
				msg.Append("Qualifier(" + this.strQualifier + ") ");
				msg.Append("ColumnDelimiter(" + this.strColumnDelimiter + ") ");
				msg.Append("RowDelimiter(" + this.strRowDelimiter + ") ");

				throw new CsvUnexpectedFormatException(msg.ToString());
			}

			Encoding = (encoding ?? "UTF-8");
			
			this.FieldCount = fieldCount;
			this.TrailerFieldCount = trailerFieldCount;
			this.IsFullQualify = isFullQualify;
			this.IsTrim = isTrim;

			csv = new StreamWriter(csvFileAbsolutePath,false, System.Text.Encoding.GetEncoding(this.Encoding));
		}

		private void write_row(int field_count, string[] fields) 
		{ 
			for(int i = 0; i< field_count; i++) 
			{
				string v = fields[i];
				bool isAddQualify = (this.IsFullQualify || (v ?? "").IndexOf(this.strQualifier)>=0 || (v ?? "").IndexOf(this.strColumnDelimiter)>=0 || (v ?? "").IndexOf(this.strRowDelimiter)>=0 || (v ?? "").IndexOf("\r")>=0 || (v ?? "").IndexOf("\n") >= 0);

				if (isAddQualify)
				{
					csv.Write(this.strQualifier);
				}

				if((v ?? "").IndexOf(this.strQualifier) >= 0) 
				{
					v = v.Replace(this.strQualifier, this.strQualifier + this.strQualifier);
				}

				csv.Write(this.IsTrim ? (v ?? "").Trim() : v);

				if (isAddQualify)
				{
					csv.Write(this.strQualifier);
				}

				csv.Write(i < (field_count - 1)? this.strColumnDelimiter : this.strRowDelimiter);
			}
		}

		public void write(string[] fields)
		{
			this.write_row(this.FieldCount, fields);
		}

		public void writeTrailer(string[] fields) 
		{ 
			this.write_row(this.TrailerFieldCount, fields);
		}

		public void Dispose()
		{
			if (csv != null)
			{
				csv.Dispose();
			}
		}
	}

}
