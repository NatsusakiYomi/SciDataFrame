package com.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import java.util.Arrays;
import java.util.Iterator;

public class Demo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ForeachPartitionDemo")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建一个 RDD
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        // 使用 foreachPartition
        rdd.foreachPartition(new ForeachPartitionFunction<Integer>() {
            @Override
            public void call(Iterator<Integer> iterator) {
                System.out.println("正在处理新分区:");
                while (iterator.hasNext()) {
                    Integer value = iterator.next();
                    System.out.println(value);
                }
            }
        });

        sc.stop();
    }
}
