package me.kaishun.spark_engines;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SaveRddFuncUtil {

	/**
	 * 保存为文件
	 * @param
	 * @param spark
	 * @param results
	 */
	public static void saveAsTextFile(String path, String splitSng, SparkSession spark, Dataset<Row> results) {
		JavaRDD<Row> javaRDD = results.toJavaRDD();
		final String splitFinal = splitSng;
		//将前后的中括号去掉, 然后保存到指定路径
		javaRDD.flatMap(new FlatMapFunction<Row, String>() {
			@Override
			public Iterator<String> call(Row row) throws Exception {
				String str = row.toString().trim().
						substring(1,row.toString().length()-1).replaceAll(",", splitFinal);
				ArrayList<String> arrayList = new ArrayList<String>();
				arrayList.add(str);
				return arrayList.iterator();
			}
		}).saveAsTextFile(path);
//		spark.stop();
	}

	public static void saveAsQuality(String path, SparkSession spark, Dataset<Row> results){

	}

	public static void saveToHive(String path, SparkSession spark, Dataset<Row> results){

	}
}
