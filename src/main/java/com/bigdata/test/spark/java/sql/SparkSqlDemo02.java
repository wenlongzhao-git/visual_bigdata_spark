package com.bigdata.test.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkSqlDemo02 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SparkSqlDemo02")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);


        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\students.txt");


        List<String> structs = new ArrayList<>();
        structs.add("name_string");
        structs.add("age_int");
        structs.add("sex_string");


        JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] lineSplited = line.split(",");

                Object[] ate = new Object[structs.size()];
                for(int i=0; i<structs.size();i++){

                    String type = structs.get(i).split("_")[1];
                    if(type != null && type.equals("string")){
                        ate[i] = lineSplited[i];
                    }else if(type.equals("int")){
                        ate[i] = Integer.valueOf(lineSplited[i]);
                    }
                }

                return RowFactory.create(ate);
            }
        });

        List<StructField> structFields = new ArrayList<>();

        for (String strust:structs) {
            String ss = strust.split("_")[0];
            String ee = strust.split("_")[1];

            if(ee != null && ee.equals("string")){
                structFields.add(DataTypes.createStructField(ss, DataTypes.StringType, true));
            }else if(ee.equals("int")) {
                structFields.add(DataTypes.createStructField(ss, DataTypes.IntegerType, true));
            }
        }
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataSet = sqlContext.createDataFrame(studentRDD, structType);

        dataSet.registerTempTable("students");

        Dataset<Row> tennnagerDS = sqlContext.sql("select * from students where age <= 18");

        List<Row> rows = tennnagerDS.javaRDD().collect();

        for(Row row:rows){
            System.out.println(row);
        }


    }
}
