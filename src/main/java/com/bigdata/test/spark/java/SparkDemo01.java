package com.bigdata.test.spark.java;

import com.sun.tools.javac.util.ListBuffer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class SparkDemo01 {
    public static void main(String[] args) {

        ListBuffer<String> jars = new ListBuffer<>();
       // args(0).split(',').map(jars += _);

        SparkConf conf = new SparkConf()
                .setAppName("SparkDemo01")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/test.txt");

        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });

        int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("文件总字数是： " + count);

        sc.close();
    }



}
