package br.ufmg.cs.systems.fractal.util;

import java.io.InputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class TextFileParser {
   final private int BUFFER_SIZE = 1 << 16;
   private DataInputStream din;
   private byte[] buffer;
   private boolean endOfStream, newLine;
   private int bufferPointer, bytesRead;

   public TextFileParser(InputStream in) throws IOException {
      din = new DataInputStream(in);
      buffer = new byte[BUFFER_SIZE];
      bufferPointer = bytesRead =  0;
      endOfStream = newLine = false;
   }

   public boolean endOfStream() {
      return endOfStream;
   }

   public boolean skipBlank() throws IOException {
      byte c;
      boolean _newLine;
      do {
         c = read();
      } while (!endOfStream && c <= ' ');

      // first non blank character
      bufferPointer--;

      _newLine = newLine;
      newLine = false;

      return _newLine || endOfStream;
   }

   public int nextInt() throws IOException {
      skipBlank();
      int ret = 0;
      byte c = read();
      boolean neg = (c == '-');
      if (neg)
         c = read();
      do
      {
         ret = ret * 10 + c - '0';
      }  while ((c = read()) >= '0' && c <= '9');

      bufferPointer--;

      if (neg)
         return -ret;
      return ret;
   }

   public long nextLong() throws IOException {
      long ret = 0;
      byte c = read();
      while (c <= ' ')
         c = read();
      boolean neg = (c == '-');
      if (neg)
         c = read();
      do {
         ret = ret * 10 + c - '0';
      }
      while ((c = read()) >= '0' && c <= '9');
      if (neg)
         return -ret;
      return ret;
   }

   public double nextDouble() throws IOException {
      double ret = 0, div = 1;
      byte c = read();
      while (c <= ' ')
         c = read();
      boolean neg = (c == '-');
      if (neg)
         c = read();

      do {
         ret = ret * 10 + c - '0';
      }
      while ((c = read()) >= '0' && c <= '9');

      if (c == '.')
      {
         while ((c = read()) >= '0' && c <= '9')
         {
            ret += (c - '0') / (div *= 10);
         }
      }

      if (neg)
         return -ret;
      return ret;
   }

   private void fillBuffer() throws IOException {
      bytesRead = din.read(buffer, bufferPointer = 0, BUFFER_SIZE);
      if (bytesRead == -1) {
         buffer[0] = -1;
         endOfStream = true;
      }
   }

   public byte read() throws IOException {
      if (bufferPointer == bytesRead)
         fillBuffer();
      byte c = buffer[bufferPointer++];
      if (c == '\n') newLine = true;
      return c;
   }

   public void close() throws IOException {
      if (din == null)
         return;
      din.close();
   }

}
