using CsvReadWrite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvReader
{
    class Program
    {
        static void Main(string[] args)
        {
            string p = @"G:\data\csv\test.csv";
            using (CsvStreamWriter cw = new CsvStreamWriter(p, "|", "\r\n", "\"", "UTF-8", 5)) 
            {
                string[] f = { "A", "B", "C", "D", "E" };

                cw.write(f, false, false);

            }
        }
    }
}
