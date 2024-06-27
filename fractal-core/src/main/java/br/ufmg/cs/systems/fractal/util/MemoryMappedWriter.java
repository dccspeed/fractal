package br.ufmg.cs.systems.fractal.util;

import scala.Int;

import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MemoryMappedWriter {

    public static void writeStatus(String path) {
        try (RandomAccessFile file = new RandomAccessFile(path, "rw");
             FileChannel fileChannel = file.getChannel()) {
            // map the file into memory
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.BYTES);
            mappedByteBuffer.order(ByteOrder.BIG_ENDIAN);
            mappedByteBuffer.putInt(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void write2dLongTensor(long[][] data, String path) {
        try (RandomAccessFile file = new RandomAccessFile(path, "rw");
             FileChannel fileChannel = file.getChannel()) {

            int nrows = data.length;
            int ncols = data[0].length;

            // calculate the size needed for the data
            int dataSize = Integer.BYTES           // status of write (0: not-ready, 1: ready)
                    + 2 * Integer.BYTES            // two dimensions
                    + nrows * ncols * Long.BYTES;  // rows and columns

            // map the file into memory
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataSize);
            mappedByteBuffer.order(ByteOrder.BIG_ENDIAN);

            // write not ready
            mappedByteBuffer.putInt(0);

            // write shape of tensor
            mappedByteBuffer.putInt(nrows);
            mappedByteBuffer.putInt(ncols);

            // write data
            for (long[] row : data) {
                for (long n : row) {
                    mappedByteBuffer.putLong(n);
                }
            }

            // write ready at first position
            mappedByteBuffer.putInt(0, 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
