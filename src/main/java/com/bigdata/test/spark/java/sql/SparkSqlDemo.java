package com.bigdata.test.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;

import java.util.*;

public class SparkSqlDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SparkSqlDemo")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String,List<String>> queryParamMap = new HashMap<>();
        queryParamMap.put("city", Arrays.asList("beijing"));
        queryParamMap.put("platform", Arrays.asList("android"));
        queryParamMap.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));

        final Broadcast<Map<String,List<String>>> queryparamMapBroadcast = sc.broadcast(queryParamMap);

        JavaRDD<String> rawRDD = sc.textFile("I:\\Spark从入门到精通（Scala编程、案例实战、高级特性、Spark内核源码剖析、Hadoop高端）\\第87讲-Spark SQL：与Spark Core整合之每日top3热点搜索词统计案例实战\\代码\\DailyTop3Keyword.java");


        JavaRDD<String> filterRDD = rawRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String log) throws Exception {
                String[] logSplited = log.split("\t");

                String city = logSplited[3];
                String platform = logSplited[4];
                String version = logSplited[5];

                Map<String,List<String>> queryParamMap = queryparamMapBroadcast.value();

                List<String> cities = queryParamMap.get("city");
                if(cities.size() > 0 && !cities.contains(city)) {
                    return false;
                }

                List<String> platforms = queryParamMap.get("platform");
                if(platforms.size() > 0 && !platforms.contains(platform)) {
                    return false;
                }

                List<String> versions = queryParamMap.get("version");
                if(versions.size() > 0 && !versions.contains(version)) {
                    return false;
                }

                return true;
            }
        });

        JavaPairRDD<String,String> dateKeywordUserRDD = filterRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String log) throws Exception {
                String[] logSplited = log.split("\t");

                String date = logSplited[0];
                String user = logSplited[1];
                String keyword = logSplited[2];

                return new Tuple2<String, String>(date + "_" + keyword, user);
            }
        });

        JavaPairRDD<String,Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();


        JavaPairRDD<String, Long> dateKeywordUvRDD = dateKeywordUsersRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeywordUsers) throws Exception {
                String dateKeyword = dateKeywordUsers._1;
                Iterator<String> users = dateKeywordUsers._2.iterator();

                // 对用户进行去重，并统计去重后的数量
                List<String> distinctUsers = new ArrayList<String>();

                while(users.hasNext()) {
                    String user = users.next();
                    if(!distinctUsers.contains(user)) {
                        distinctUsers.add(user);
                    }
                }

                // 获取uv
                long uv = distinctUsers.size();

                return new Tuple2<String, Long>(dateKeyword, uv);
            }
        });

        JavaRDD<Row> dateKeywordUvRowRDD = dateKeywordUvRDD.map(new Function<Tuple2<String, Long>, Row>() {
            @Override
            public Row call(Tuple2<String, Long> dateKeywordUv) throws Exception {
                String date = dateKeywordUv._1.split("_")[0];
                String keyword = dateKeywordUv._1.split("_")[1];
                long uv = dateKeywordUv._2;
                return RowFactory.create(date, keyword, uv);
            }
        });


        List<StructField> structFields = new ArrayList<>();

        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));


        List<String> structs = new ArrayList<>();
        structs.add("name_string");
        structs.add("age_int");
        structs.add("sex_string");


        /*for (String strust:structs) {
            String ss = strust.split("_")[0];
            String ee = strust.split("_")[0];

            if(ee != null && ee.equals("string")){
                structFields.add(DataTypes.createStructField(ss, DataTypes.StringType, true));
            }else if(ee.equals("int")) {
                structFields.add(DataTypes.createStructField(ss, DataTypes.IntegerType, true));
            }
        }*/


        


    }
}
