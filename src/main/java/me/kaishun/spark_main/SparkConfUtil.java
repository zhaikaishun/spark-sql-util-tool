package me.kaishun.spark_main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.SparkSession;

public class SparkConfUtil {
//    public static SparkSession getSparkSession() {
//    	// TODO 本地测试需要修改
//        System.setProperty("hadoop.home.dir", "D:\\softdownload\\install_package\\hadoop-2.7.1");
////        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
////                .set("yarn.nodemanager.resource.memory-mb", "8192")
////                .set("yarn.scheduler.maximum-allocation-mb", "8192")
////                .set("spark.driver.maxResultSize", "4g")
////                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
////        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
////        SQLContext sqlContext = new SQLContext(ctx);
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("DataFilter").master("local")
//                .getOrCreate();
////	    	      .config("yarn.nodemanager.resource.memory-mb", "8192")
////	    	      .config("yarn.scheduler.maximum-allocation-mb", "8192")
////	    	      .config("spark.driver.maxResultSize", "4g")
////	    	      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
////	    	      .master("local") //写成可配置的
//
//        // 设置log级别
//        SparkContext sc = spark.sparkContext();
//        sc.setLogLevel("WARN");
//        return spark;
//    }
}
