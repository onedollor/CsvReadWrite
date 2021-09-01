using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CsvReadWrite
{
    public class CharBuffer : IDisposable
    {
        public const int MAX_BUFFER_SIZE = 10 * 1024 ^ 2;
        public const int MIN_BUFFER_SIZE = 200;
        public const int BUFFER_TAIL_SIZE = 20;

        char[] buffer = null;
        private int buffSize = MIN_BUFFER_SIZE;
        private bool isCopyTail = false;

        private StreamReader fp = null;

        int charPos = 0;
        int charLen = 0;

        int charTailLen = 0;
        public string FileAbsolutePath { get; private set; }

        public CharBuffer(string fileAbsolutePath, string encoding)
            : this(fileAbsolutePath, encoding, MIN_BUFFER_SIZE)
        {
        }
        
        
        public CharBuffer(string fileAbsolutePath, string encoding, int size) 
        {
            this.FileAbsolutePath = fileAbsolutePath;
            fp = new StreamReader(this.FileAbsolutePath, Encoding.GetEncoding(encoding));

            this.buffSize = size > MAX_BUFFER_SIZE ? MAX_BUFFER_SIZE : (size < MIN_BUFFER_SIZE ? MIN_BUFFER_SIZE : size);
            this.buffer = new char[this.buffSize];

            this.LoadBuffer();
        }
        
        
        public int Read()
        {
            if (this.charPos >= this.charLen)
            {
                LoadBuffer();

                if (this.charPos >= this.charLen)
                {
                    if (this.fp.Peek() >= 0)
                    {
                        return this.fp.Read();
                    }
                    else 
                    {
                        return this.fp.Peek();
                    }
                }
            }

            return this.buffer[this.charPos++];
        }
        
        
        public char[] Read(int length)
        {
            List<char> r = new List<char>();
            if (0 == length) 
            {
                throw new ReadException("try to read 0 length");
            }

            for (int i = 0; i < length; i++)
            {
                if (this.charPos >= this.charLen)
                {
                    LoadBuffer();

                    if (this.charPos >= this.charLen) 
                    {
                        break;
                    }
                }

                if (this.charPos < this.charLen)
                {
                    r.Add(this.buffer[this.charPos++]);
                }
            }

            return r.ToArray();
        }


        public int Peek() 
        {
            if (this.charTailLen > 0 || this.charPos < this.charLen)
            {
                return this.buffer[this.charPos];
            }
            else 
            {
                return this.fp.Peek();
            }
        }


        public char[] Peek(int length)
        {
            List<char> r = new List<char>();

            if (length > BUFFER_TAIL_SIZE)
            {
                StringBuilder msg = new StringBuilder();
                msg.Append("Peek over length => " + this.FileAbsolutePath + " ");
                msg.Append("Peek length(" + (length) + ") ");
                msg.Append("MAX accepted Peek length (" + BUFFER_TAIL_SIZE + ") ");

                throw new PeekException(msg.ToString());
            }
            else
            {
                int pos = this.charPos;

                for (int i = 0; i < length; i++)
                {
                    if(pos < (this.charLen + this.charTailLen)) 
                    {
                        r.Add(this.buffer[pos++]);
                    }
                }
            }

            return r.ToArray();
        }


        public bool Eof() 
        {
            return this.fp.Peek() < 0;
        }


        private int CopyTailBuffer()
        {
            if (isCopyTail)
            {
                int s = this.buffSize - BUFFER_TAIL_SIZE;
                int p = 0;

                for (int tp = s; tp < s + this.charTailLen;) 
                {
                    this.buffer[p++] = this.buffer[tp++];
                }

                this.charTailLen = 0;
                isCopyTail = false;
                return p;
            }
            else 
            {
                return 0;
            };
        }
        
        
        private void LoadBuffer()
        {
            int p = this.CopyTailBuffer();

            while (fp.Peek() >= 0 && p < (buffSize - BUFFER_TAIL_SIZE))
            {
                this.buffer[p++] = (char)fp.Read();
            }

            if (fp.Peek() >= 0) 
            {
                int tp = 0;

                while (fp.Peek() >= 0 && tp < BUFFER_TAIL_SIZE)
                {
                    this.buffer[p + tp] = (char)fp.Read();
                    tp++;
                }

                if (tp > 0) 
                {
                    this.charTailLen = tp;
                    this.isCopyTail = true;
                }
            }

            this.charPos = 0;
            this.charLen = p;
        }
        
        
        public void Dispose()
        {
            buffer = null;

            if (fp != null)
            {
                fp.Dispose();
            }
        }
    }


    public class PeekException : Exception
    {
        public PeekException()
        {
        }

        public PeekException(string message)
            : base(message)
        {
        }

        public PeekException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }


    public class ReadException : Exception
    {
        public ReadException()
        {
        }

        public ReadException(string message)
            : base(message)
        {
        }

        public ReadException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
