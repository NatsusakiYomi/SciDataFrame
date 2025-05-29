package com.example;

//import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import java.util.List;

public class SparkCsvLoader {
    public static void main(String[] args) {
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

        try {
            // 读取/mnt下所有CSV文件
            Dataset<Row> df = spark.read()
                    .option("header", "true")  // 如果包含表头
                    .option("inferSchema", "true")  // 自动推断数据类型
                    .csv("/mnt/kmeans-data5.csv/*.csv");  // 通配符匹配所有CSV

            // 将数据拉取到Driver端（注意数据量不宜过大）
//            List<Row> collectedData = df.collectAsList();

              df.write()
                    .format("csv")
                    .option("header", "true")
                    .save("/mnt/output_csv");
            // 此处处理数据，例如打印样例
//            System.out.println("Total records loaded: " + collectedData.size());
//            if (!collectedData.isEmpty()) {
//                System.out.println("Sample first row: " + collectedData.get(0));
//            }

        } finally {
            spark.stop(); // 确保关闭SparkSession
        }
    }
}
