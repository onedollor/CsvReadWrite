using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace CsvReadWrite
{
    public class CsvStreamReader : IDisposable
    {
		public char[] RowDelimiter { get; private set; }
		public string strRowDelimiter { get; private set; }

		//Row Delimiter treat \r\n or \r or \n same.
		public bool IsRowDelimiterAcceptDifferentLineChangeCharacter { get; private set; }
		public string strRowDelimiter_r { get; private set; }
		public string strRowDelimiter_n { get; private set; }
		public string strRowDelimiter_rn { get; private set;}

		public char[] ColumnDelimiter { get; private set; }
		public string strColumnDelimiter { get; private set; }
		public char[] Qualifier { get; private set; }
		public string strQualifier { get; private set; }
		public string Encoding { get; private set; }
		public long CsvLength { get; private set; }
		private bool IsColumnCountFromFirstRow { get; set; }
		private bool IsEscapeQualifer { get; set; }
		public string CsvFileAbsolutePath { get; private set; }
		public Int64 RowCount
		{
			get { return this.rowCount; }
		}

		private Int64 rowCount = 0;

		public int ColumnCount
		{
			get { return this.columnCount; }
			set { this.columnCount = value; }
		}
		private int columnCount = 0;

		private CharBuffer csv = null;
		/// <summary>
		/// If true start/end spaces are excluded from field values (except values in quotes). True by default.
		/// </summary>
		public bool TrimFields { get; set; } = true;

		public int TrailerFieldCount { get; set; } = 0;

		List<string> fields = new List<string>();
		List<string> trailerFields = new List<string>();
		List<string> header = new List<string>();

		public List<string> TrailerFields 
		{ 
			get { return this.trailerFields; }
		}

		public CsvStreamReader(string csvFileAbsolutePath, string encoding, string columnDelimiter=",", string rowDelimiter = "\r\n", string qualifier = "\"", bool isRowDelimiterAcceptDifferentLineChangeCharacter = true, bool isColumnCountFromFirstRow = true, bool isEscapeQualifer = false, int trailerFieldCount = 0)
		{
			CsvFileAbsolutePath = csvFileAbsolutePath;
			if ((CsvFileAbsolutePath ?? "").Length == 0)
				throw new ArgumentException("Csv File Absolute Path cannot be empty.");

			strColumnDelimiter = (columnDelimiter ?? "");
			ColumnDelimiter = strColumnDelimiter.ToCharArray();
			
			if (ColumnDelimiter.Length == 0)
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
			RowDelimiter = strRowDelimiter.ToCharArray();

			if (this.strRowDelimiter.EndsWith("\r\n"))
			{
				this.strRowDelimiter_rn = this.strRowDelimiter;
				this.strRowDelimiter_r = this.strRowDelimiter.Substring(0, this.RowDelimiter.Length - 2) + "\r";
				this.strRowDelimiter_n = this.strRowDelimiter.Substring(0, this.RowDelimiter.Length - 2) + "\n";
			}
			else if (this.strRowDelimiter.EndsWith("\r")) 
			{
				this.strRowDelimiter_rn = this.strRowDelimiter+"\n";
				this.strRowDelimiter_r = this.strRowDelimiter;
				this.strRowDelimiter_n = this.strRowDelimiter.Substring(0, this.RowDelimiter.Length - 1) + "\n";
			}
			else if (this.strRowDelimiter.EndsWith("\n") && !this.strRowDelimiter.EndsWith("\r\n"))
			{
				this.strRowDelimiter_rn = this.strRowDelimiter.Substring(0, this.RowDelimiter.Length - 1) + "\r\n";
				this.strRowDelimiter_r = this.strRowDelimiter.Substring(0, this.RowDelimiter.Length - 1) + "\r";
				this.strRowDelimiter_n = this.strRowDelimiter;
			}

			IsRowDelimiterAcceptDifferentLineChangeCharacter = isRowDelimiterAcceptDifferentLineChangeCharacter;

			if (RowDelimiter.Length == 0)
				throw new ArgumentException("Row Delimiter cannot be empty.");

			IsColumnCountFromFirstRow = isColumnCountFromFirstRow;
			IsEscapeQualifer = isEscapeQualifer;
			
			if ("" == (qualifier ?? ""))
			{
				strQualifier = "\"";
			}
			else 
			{
				strQualifier = qualifier;	
			}
			Qualifier = strQualifier.ToCharArray();

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

			this.rowCount = 0;

			this.CsvLength = new FileInfo(this.CsvFileAbsolutePath).Length;

			this.TrailerFieldCount = trailerFieldCount;

			//csv = new CharBuffer(this.CsvFileAbsolutePath, this.Encoding, CharBuffer.MAX_BUFFER_SIZE);
			csv = new CharBuffer(this.CsvFileAbsolutePath, this.Encoding, CharBuffer.MIN_BUFFER_SIZE * 10);
		}

		public int FieldsCount
		{
			get
			{
				return this.fields.Count;
			}
		}

		public string this[int idx]
		{
			get
			{
				if (idx < fields.Count)
				{
					return fields[idx];
				}
				return null;
			}
		}

		public int GetValueLength(int idx)
		{
			if (idx < fields.Count)
			{
				return (fields[idx] ?? "").Length;
			}
			return -1;
		}

		public bool Read()
		{
			this.fields = new List<string>();

			if (csv.Peek() < 0)
			{
				return false;
			}
			else
			{
				//Skip empty line
				if (new string(csv.Peek(this.RowDelimiter.Length)) == this.strRowDelimiter)
				{
					csv.Read(this.RowDelimiter.Length);
					return this.Read();
				}
				else if (new string(csv.Peek(2)) == "\r\n")
				{
					csv.Read(2);
					return this.Read();
				}
				else if ((char)csv.Peek() == '\r' || (char)csv.Peek() == '\n')
				{
					csv.Read();
					return this.Read();
				}
			}

			bool isReadNext = true;

			bool isColumnDelimiterFound = false;
			bool isRowDelimiterFound = false;

			StringBuilder sbColumnValue = new StringBuilder();

			bool isNullField = true;
			
			while (isReadNext)
			{
				if (isColumnDelimiterFound || isRowDelimiterFound || csv.Peek() < 0)
				{
					if(isNullField && sbColumnValue.ToString().Length == 0)
					{
						this.fields.Add(null);
					}
					else
					{
						this.fields.Add(this.TrimFields ? sbColumnValue.ToString().Trim() : sbColumnValue.ToString());
					}

					isColumnDelimiterFound = false;

					if (isRowDelimiterFound || csv.Peek() < 0)
					{
						isReadNext = false;
					}
					else
					{
						sbColumnValue = new StringBuilder();
						isNullField = true;
					}
				}
				else
				{
					string peekQualifier = new string(csv.Peek(this.Qualifier.Length));

					if (this.strQualifier == peekQualifier)
					{
						bool isQualifierFound = false;
						isNullField = false;
						
						csv.Read(this.Qualifier.Length);

						while (!isQualifierFound && csv.Peek() >= 0)
						{
							if(this.IsEscapeQualifer)
							{
								if (new string(csv.Peek(this.Qualifier.Length + this.RowDelimiter.Length)) == (this.strQualifier + this.strRowDelimiter))
								{
									isQualifierFound = true;
									isRowDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.RowDelimiter.Length);
								}
								else if (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && new string(csv.Peek(this.Qualifier.Length + this.strRowDelimiter_rn.Length)) == (this.strQualifier + this.strRowDelimiter_rn))
								{
									isQualifierFound = true;
									isRowDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.strRowDelimiter_rn.Length);
								}
								else if (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && new string(csv.Peek(this.Qualifier.Length + this.strRowDelimiter_r.Length)) == (this.strQualifier + this.strRowDelimiter_r))
								{
									isQualifierFound = true;
									isRowDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.strRowDelimiter_r.Length);
								}
								else if (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && new string(csv.Peek(this.Qualifier.Length + this.strRowDelimiter_n.Length)) == (this.strQualifier + this.strRowDelimiter_n))
								{
									isQualifierFound = true;
									isRowDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.strRowDelimiter_n.Length);
								}
								else if (new string(csv.Peek(this.Qualifier.Length + this.ColumnDelimiter.Length)) == (this.strQualifier + this.strColumnDelimiter))
								{
									isQualifierFound = true;
									isColumnDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.ColumnDelimiter.Length);
								}
								else if (new string(csv.Peek(this.Qualifier.Length)) == this.strQualifier)
								{
									sbColumnValue.Append(csv.Read(this.Qualifier.Length));
								}
								else
								{
									sbColumnValue.Append((char)csv.Read());
								}
							}
							else
							{
								if (new string(csv.Peek(this.Qualifier.Length * 2)) == (this.strQualifier + this.strQualifier))
								{
									sbColumnValue.Append(csv.Read(this.Qualifier.Length));
									csv.Read(this.Qualifier.Length);
								}
								else if (new string(csv.Peek(this.Qualifier.Length + this.RowDelimiter.Length)) == (this.strQualifier + this.strRowDelimiter))
								{
									isQualifierFound = true;
									isRowDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.RowDelimiter.Length);
								}
								else if (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && new string(csv.Peek(this.Qualifier.Length + this.strRowDelimiter_rn.Length)) == (this.strQualifier + this.strRowDelimiter_rn))
								{
									isQualifierFound = true;
									isRowDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.strRowDelimiter_rn.Length);
								}
								else if (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && new string(csv.Peek(this.Qualifier.Length + this.strRowDelimiter_r.Length)) == (this.strQualifier + this.strRowDelimiter_r))
								{
									isQualifierFound = true;
									isRowDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.strRowDelimiter_r.Length);
								}
								else if (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && new string(csv.Peek(this.Qualifier.Length + this.strRowDelimiter_n.Length)) == (this.strQualifier + this.strRowDelimiter_n))
								{
									isQualifierFound = true;
									isRowDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.strRowDelimiter_n.Length);
								}
								else if (new string(csv.Peek(this.Qualifier.Length + this.ColumnDelimiter.Length)) == (this.strQualifier + this.strColumnDelimiter))
								{
									isQualifierFound = true;
									isColumnDelimiterFound = true;
									csv.Read(this.Qualifier.Length);
									csv.Read(this.ColumnDelimiter.Length);
								}
								else
								{
									sbColumnValue.Append((char)csv.Read());
								}								
							}
						}
					}
					else
					{
						string peekColumnDelimiter = new string(csv.Peek(this.ColumnDelimiter.Length));
						string peekRowDelimiter = new string(csv.Peek(this.RowDelimiter.Length));

						string peekRowDelimiter_rn = "";
						string peekRowDelimiter_r = "";
						string peekRowDelimiter_n = "";

						if (!this.IsRowDelimiterAcceptDifferentLineChangeCharacter)
						{
							while (peekColumnDelimiter != this.strColumnDelimiter
								   && peekRowDelimiter != this.strRowDelimiter
								   && csv.Peek() >= 0)
							{
								sbColumnValue.Append((char)csv.Read());
								peekColumnDelimiter = new string(csv.Peek(this.ColumnDelimiter.Length));
								peekRowDelimiter = new string(csv.Peek(this.RowDelimiter.Length));
							}
						}
						else
						{
							peekRowDelimiter_rn = new string(csv.Peek(this.strRowDelimiter_rn.Length));
							peekRowDelimiter_r = new string(csv.Peek(this.strRowDelimiter_r.Length));
							peekRowDelimiter_n = new string(csv.Peek(this.strRowDelimiter_n.Length));

							while (peekColumnDelimiter != this.strColumnDelimiter
								   && peekRowDelimiter != this.strRowDelimiter
								   && (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && peekRowDelimiter_rn != this.strRowDelimiter_rn)
								   && (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && peekRowDelimiter_r != this.strRowDelimiter_r)
								   && (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && peekRowDelimiter_n != this.strRowDelimiter_n)
								   && csv.Peek() >= 0)
							{
								sbColumnValue.Append((char)csv.Read());
								peekColumnDelimiter = new string(csv.Peek(this.ColumnDelimiter.Length));
								peekRowDelimiter = new string(csv.Peek(this.RowDelimiter.Length));

								peekRowDelimiter_rn = new string(csv.Peek(this.strRowDelimiter_rn.Length));
								peekRowDelimiter_r = new string(csv.Peek(this.strRowDelimiter_r.Length));
								peekRowDelimiter_n = new string(csv.Peek(this.strRowDelimiter_n.Length));
							}
						}

						if (csv.Peek() < 0)
						{
							isRowDelimiterFound = true;
						}
						else if (peekRowDelimiter == this.strRowDelimiter)
						{
							isRowDelimiterFound = true;
							csv.Read(this.RowDelimiter.Length);
						}
						else if (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && peekRowDelimiter_rn == this.strRowDelimiter_rn)
						{
							isRowDelimiterFound = true;
							csv.Read(this.strRowDelimiter_rn.Length);
						}
						else if (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && peekRowDelimiter_r == this.strRowDelimiter_r)
						{
							isRowDelimiterFound = true;
							csv.Read(this.strRowDelimiter_r.Length);
						}
						else if (this.IsRowDelimiterAcceptDifferentLineChangeCharacter && peekRowDelimiter_n == this.strRowDelimiter_n)
						{
							isRowDelimiterFound = true;
							csv.Read(this.strRowDelimiter_n.Length);
						}
						else if (peekColumnDelimiter == this.strColumnDelimiter)
						{
							isColumnDelimiterFound = true;
							csv.Read(this.ColumnDelimiter.Length);
						}
					}
				}
			}

			if (0 == this.fields.Count)
			{
				return this.Read();
			}
			else
			{
				if (this.ColumnCount > 0 && this.fields.Count != this.ColumnCount)
				{
                    if (this.TrailerFieldCount > 0 && this.fields.Count == this.TrailerFieldCount) 
					{ 
						for(int i =0; i<this.TrailerFieldCount;i++) 
						{ 
							this.trailerFields.Add(this.fields[i]);
						}

						return false;
					}
                    else 
					{ 
						StringBuilder msg = new StringBuilder();
						msg.Append("unexpected format => " + this.CsvFileAbsolutePath + " ");
						msg.Append("expected this.ColumnCount(" + this.ColumnCount + ") ");
						msg.Append("row(" + (this.rowCount + 1) + ") ");
						msg.Append("actual this.fields.Count(" + this.fields.Count + ") ");

						for (int i = 0; i < this.fields.Count; i++)
						{
							msg.Append("{Field[" + i + "](" + this.fields[i] + ")} ");
						}

						throw new CsvUnexpectedFormatException(msg.ToString());
					}
				}
                else 
				{ 
					if (0 == this.rowCount && this.IsColumnCountFromFirstRow)
					{
						this.ColumnCount = this.fields.Count;
					}

					this.rowCount++;
				}
			}

			return true;
		}

		public void Dispose()
		{
			if (csv != null)
			{
				csv.Dispose();
			}
		}
	}

	public class CsvUnexpectedFormatException : Exception
	{
		public CsvUnexpectedFormatException()
		{
		}

		public CsvUnexpectedFormatException(string message)
			: base(message)
		{
		}

		public CsvUnexpectedFormatException(string message, Exception inner)
			: base(message, inner)
		{
		}
	}

}
