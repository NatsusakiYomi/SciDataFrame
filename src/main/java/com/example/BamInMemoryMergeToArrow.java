package com.example;

import htsjdk.samtools.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.Channels;
import java.util.*;

public class BamInMemoryMergeToArrow {

    public static void printMemory(){
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();    // JVM 最大可用内存（Xmx）
        long totalMemory = runtime.totalMemory(); // 当前分配的堆内存
        long freeMemory = runtime.freeMemory();   // 堆中空闲内存
        long usedMemory = totalMemory - freeMemory;

        System.out.printf("JVM 内存使用: %.2f MB / %.2f MB (最大: %.2f MB)\n",
                usedMemory / (1024.0 * 1024),
                totalMemory / (1024.0 * 1024),
                maxMemory / (1024.0 * 1024));
    }

    public static void main(String[] args) throws Exception {
//        String PathPrefix="C:\\Users\\NatsusakiYomi\\Downloads\\";
//        int port =23457;
        if (args.length != 2) {
            System.err.println("Usage: java -jar BamMerger.jar <bam1,bam2,...> <port>");
            System.exit(1);
        }
        printMemory();

        int port = Integer.parseInt(args[1]);

        String PathPrefix = args[0];

        File bam1 = new File(PathPrefix+"HG00096.chrom11.ILLUMINA.bwa.GBR.low_coverage.20120522.bam");
        File bam2 = new File(PathPrefix+"HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam");

        // 1. 打开输入 BAM 流
        SamReader reader1 = SamReaderFactory.makeDefault().open(bam1);
        SamReader reader2 = SamReaderFactory.makeDefault().open(bam2);
        SAMFileHeader header = reader1.getFileHeader().clone();
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        System.out.println("BAM read.");
        printMemory();



        // 2. 将合并后的 BAM 写入 ByteArrayOutputStream（内存中）
        ByteArrayOutputStream bamOutputStream = new ByteArrayOutputStream();
        SAMFileWriterFactory writerFactory = new SAMFileWriterFactory();
        SAMFileWriter writer = writerFactory.makeBAMWriter(header, true, bamOutputStream);


        for (SAMRecord rec : reader1) writer.addAlignment(rec);
        for (SAMRecord rec : reader2) writer.addAlignment(rec);
        writer.close();
        reader1.close();
        reader2.close();

        System.out.println("BAM merged.");
        printMemory();

        byte[] mergedBamBytes = bamOutputStream.toByteArray();
//        byte[] mergedBamBytes = new byte[10];
        System.out.printf("Merged BAM in memory, %d in total.", mergedBamBytes.length);
        // 3. 使用 Arrow 构建仅一列的表（Binary 类型，存一整块 BAM 数据）
        RootAllocator allocator = new RootAllocator();
        Field field = new Field("bam_data", FieldType.nullable(new ArrowType.Binary()), null);
        Schema schema = new Schema(Collections.singletonList(field));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        VarBinaryVector vector = (VarBinaryVector) root.getVector("bam_data");
        vector.allocateNew();
        vector.setSafe(0, mergedBamBytes);
        vector.setValueCount(1);
        root.setRowCount(1);

        System.out.println("BAM converted.");
        printMemory();

        // 4. 写入 socket 的 Arrow Stream
        try (Socket socket = new Socket("0.0.0.0", port);
             ArrowFileWriter arrowFileWriter = new ArrowFileWriter(root, null, Channels.newChannel(socket.getOutputStream()))) {
            arrowFileWriter.start();
            arrowFileWriter.writeBatch();
            arrowFileWriter.end();
        }

        System.out.println("Merged BAM in memory, sent as Arrow to socket.");
    }
}
