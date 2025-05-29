package com.example;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.spark.sql.Row;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.Iterator;

import java.util.List;

public class ArrowPartitionHandler {

    private static Socket socket;

//    public ArrowPartitionHandler(){
//
//    }

    public static void processPartition(Iterator<Row> partitionIterator, int port) {
        // 收集分区内的所有行数据
//        List<Row> rows = new ArrayList<>();
//        partitionIterator.forEachRemaining(rows::add);
//        if (rows.isEmpty()) return;
//
//        // 创建Arrow Schema和内存分配器
//        try {
//            RootAllocator allocator = new RootAllocator();
//            Field field = new Field("csv_data", FieldType.nullable(new ArrowType.Binary()), null);
//            Schema schema = new Schema(Collections.singletonList(field));
//            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
//
//            // 填充数据到Arrow向量
////            populateVectors(root, rows);
//
//            // 将VectorSchemaRoot写入二进制流
//            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(outputStream))) {
//                writer.start();
//                writer.writeBatch();
//                writer.end();
//            }
//
//            // 获取二进制数据并发送到服务器
//            byte[] arrowData = outputStream.toByteArray();
//            sendToFlightServer(arrowData);
//
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to process partition", e);
//        }
    }

    private static void iterateAndPopulate(VectorSchemaRoot root, List<Row> rows) {
//        for (int i = 0; i < rows.size(); i++) {
//            Row row = rows.get(i);
//            for (int j = 0; j < root.getFieldVectors().size(); j++) {
//                FieldVector vector = root.getVector(j);
//                Object value = row.get(j);
//                // 具体类型处理（参考问题中的示例）
//                if (vector instanceof VarCharVector) {
//                    ((VarCharVector) vector).setSafe(i, value.toString().getBytes(StandardCharsets.UTF_8));
//                } else if (vector instanceof IntVector) {
//                    ((IntVector) vector).set(i, (Integer) value);
//                } // 其他类型处理...
//            }
//        }
//        root.setRowCount(rows.size());
    }

    private static void collectAndPopulate(VectorSchemaRoot root, List<Row> rows) {


    }

    public static void startSocket(int port) throws IOException {
        socket = new Socket("0.0.0.0", port);
    }

    public static void closeSocket(int port) throws IOException {
        socket.close();
    }

    public static void sendToFlightServer(byte[] bytes) {
        VectorSchemaRoot root = convertToArrowBinary(bytes);
        try (
             ArrowFileWriter arrowFileWriter = new ArrowFileWriter(root, null, Channels.newChannel(socket.getOutputStream()))) {
            arrowFileWriter.start();
            arrowFileWriter.writeBatch();
            arrowFileWriter.end();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 使用Arrow Flight Client发送数据（示例需具体实现）
    }

    public static void saveToLocal(int id, VectorSchemaRoot root) {
        String fileName = String.format("/mnt/output/partition_%d.arrow", id);
        File file = new File(fileName);

        try (
                FileOutputStream fileOutputStream = new FileOutputStream(file);
                ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel())
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            root.close();
            System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + root.getRowCount());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void saveToLocal(byte[] bytes, int id){
        VectorSchemaRoot root = convertToArrowBinary(bytes);
        String fileName = String.format("/mnt/output/partition_%d.arrow", id);
        File file = new File(fileName);

        try (
                FileOutputStream fileOutputStream = new FileOutputStream(file);
                    ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel())
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + root.getRowCount());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static ArrowWriter convertToArrow(StructType schema, String timeZoneId){
        Schema arrowSchema = ArrowUtils.toArrowSchema(schema,timeZoneId);
        BufferAllocator allocator = ArrowUtils.rootAllocator().newChildAllocator("new",0,Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
//        VectorUnloader vectorUnloader = new VectorUnloader(root);
        return ArrowWriter.create(root);
    }



    public static VectorSchemaRoot convertToArrowBinary(byte[] data){
        RootAllocator allocator = new RootAllocator();
        Field field = new Field("csv_data", FieldType.nullable(new ArrowType.Binary()), null);
        Schema schema = new Schema(Collections.singletonList(field));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarBinaryVector vector = (VarBinaryVector) root.getVector("csv_data");
        vector.allocateNew();
        vector.setSafe(0, data);
        vector.setValueCount(1);
        root.setRowCount(1);
        return root;
    }
}
