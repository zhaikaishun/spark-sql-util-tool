package me.kaishun.spark_main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;


public class SparkConfUtil {
    public static SparkSession getSparkSession() {

        String os = System.getProperty("os.name");
        SparkSession spark = null;
        if(os.toLowerCase().startsWith("win")){
            System.setProperty("hadoop.home.dir", "D:\\softdownload\\install_package\\hadoop-2.7.1");
            SparkConf sparkConf = new SparkConf();
            sparkConf.set("fs.defaultFS", "hdfs://192.168.1.37:9000");
            spark = SparkSession
                    .builder()
                    .appName("spark-sql-tool").master("local")
                    .enableHiveSupport()
                    .config(sparkConf)
                    .getOrCreate();
        }else{
            spark = SparkSession
                    .builder()
                    .appName("spark-sql-tool")
//                    .enableHiveSupport()
                    .getOrCreate();
        }
        // 设置log级别
        SparkContext sc = spark.sparkContext();
        sc.setLogLevel("WARN");
        return spark;
    }
}
