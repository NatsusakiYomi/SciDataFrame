package com.example;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.ForeachPartitionFunction;

public class SparkExecutorMapper {
    public static void main(String[] args) {
//        List<String> list = Arrays.asList("Guangdong", "Zhejiang", "Jiangsu", "Xizang", "Fujian", "Hunan", "Guangxi");
//        list.forEach(s -> System.out.println(s));
        SparkConf conf = new SparkConf()
                .setAppName("Custom Resource Job")
                .setMaster("local[*]") // 集群模式（如local[*]、spark://host:7077等）

                // Driver 资源配置
                .set("spark.driver.memory", "1g")      // Driver 内存（默认1g）
//                .set("spark.driver.cores", "2")        // Driver 核心数（仅在集群模式下生效）

                // Executor 资源配置
                .set("spark.executor.memory", "6g")    // 每个Executor内存
                .set("spark.executor.cores", "100")      // 每个Executor的核心数
                .set("spark.executor.instances", "4"); // Executor的数量（默认自动分配）
        // 初始化SparkSession (本地模式)
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .option("header", "true")  // 如果包含表头
                .option("inferSchema", "true")  // 自动推断数据类型
                .csv("/mnt/kmeans-data5.csv/*.csv");


        JavaRDD<Row> rdd = df.javaRDD();
        df.foreachPartition((Iterator<Row> iterator) -> {
            try {
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    // 可能抛出异常的代码
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
    // 通配符匹配所有CSV
}
