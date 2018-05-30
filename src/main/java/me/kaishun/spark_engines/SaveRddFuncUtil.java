package me.kaishun.spark_engines;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
public class SaveRddFuncUtil {

	/**
	 * 保存为文件
	 * @param
	 * @param
	 * @param results
	 */
	public static void saveAsTextFile(String path,String splitSng, DataFrame results) {
		JavaRDD<Row> javaRDD = results.toJavaRDD();
		//将前后的中括号去掉, 然后保存到指定路径
		final String finalSlitSng = splitSng;
		javaRDD.flatMap(new FlatMapFunction<Row, String>() {
			@Override
			public Iterable<String> call(Row row) throws Exception {
				String str = row.toString().trim().
						substring(1,row.toString().length()-1).replaceAll(",", finalSlitSng);
				ArrayList<String> arrayList = new ArrayList<String>();
				arrayList.add(str);
				return arrayList;
			}
		}).saveAsTextFile(path);
//		spark.stop();
	}
	/**
	 * 保存为parquet文件
	 * @param path
	 * @param spark
	 * @param results
	 */
//	public static void saveAsParquet(String path, SparkSession spark, Dataset<Row> results){
//		results.write().parquet(path);
//	}
//
//	public static void saveToHive(String path, SparkSession spark, Dataset<Row> results){
//
//	}
}
