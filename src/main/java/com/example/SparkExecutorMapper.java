package com.example;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.util.ArrowUtils;



public class SparkExecutorMapper {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("Custom Resource Job")
                .setMaster("local[*]") // 集群模式（如local[*]、spark://host:7077等）

                // Driver 资源配置
                .set("spark.driver.memory", "1g")      // Driver 内存（默认1g）
//                .set("spark.driver.cores", "2")        // Driver 核心数（仅在集群模式下生效）

                // Executor 资源配置
                .set("spark.executor.memory", "3g")    // 每个Executor内存
                .set("spark.executor.cores", "100")      // 每个Executor的核心数
                .set("spark.executor.instances", "8"); // Executor的数量（默认自动分配）
        // 初始化SparkSession (本地模式)
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .option("header", "true")  // 如果包含表头
                .option("inferSchema", "true")  // 自动推断数据类型
                .csv("/mnt/test/*.csv");
        StructType schema = df.schema();
        Broadcast<StructType> schemaBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(schema);
        ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(schema);
        Broadcast<ExpressionEncoder.Serializer<Row>> serializerBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(rowEncoder.createSerializer());
//        JavaRDD<Row> rdd = df.javaRDD();
//        ArrowPartitionHandler.startSocket(8815);
        df.foreachPartition((Iterator<Row> iterator) -> {
            int id = org.apache.spark.TaskContext.getPartitionId();
            try {
//                  int cnt=1;
//                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    StructType partitionSchema = schemaBroadcast.value();
                    Schema arrowSchema = ArrowUtils.toArrowSchema(partitionSchema,"UTC");
                    BufferAllocator allocator = ArrowUtils.rootAllocator().newChildAllocator("new",0,Long.MAX_VALUE);
                    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
                    ArrowWriter arrowWriter = ArrowWriter.create(root);
                    ExpressionEncoder.Serializer<Row> serializer = serializerBroadcast.value();
//                    ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(schema);
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        InternalRow internalRow = serializer.apply(row);
                        arrowWriter.write(internalRow);
//                        for (int i = 0; i < partitionSchema.length(); i++) {
//
//                            DataType dataType = partitionSchema.apply(i).dataType();
//
//                            byte[] fieldBytes = RowToBinaryConverter.convertFieldToBytes(row, i, dataType);
//                            outputStream.write(fieldBytes);
//                        }
//                        outputStream.write(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF });

                        // 可选：添加行分隔符（如4字节0xFFFFFFFF）
//                        ArrowPartitionHandler.sendToFlightServer(outputStream.toByteArray());
                    }
                    arrowWriter.finish();
                System.out.println("arrowWriter succeeded.");
                ArrowPartitionHandler.saveToLocal(id,root);
                    allocator.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
    // 通配符匹配所有CSV
}
