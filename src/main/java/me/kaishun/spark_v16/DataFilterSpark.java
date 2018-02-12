package me.kaishun.spark_v16;

//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
////import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.types.DataType;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//
//import breeze.signal.fourierShift;

public class DataFilterSpark {
//	public static void main(String[] args) throws Exception {
//		String FieldSplitSign = " ";
//		String tableField = "";
//		String tableAttr = "";
//		String tablenamestr = "";
//		if (args.length != 4) {
//			System.err.println("Usage: JavaWordCount <file>");
//			System.exit(1);
//		}
//		String[] strings = args[0].split("&");
//		tablenamestr = strings[0];
//		tableField = strings[1];
//		tableAttr = strings[2];
//		FieldSplitSign = strings[3];
//		final String[] fieldAttyList = tableAttr.split("#");
//
//		System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.7.1");
//		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL")
//				.setMaster("local");
//		// .set("yarn.nodemanager.resource.memory-mb", "8192")
//		// .set("yarn.scheduler.maximum-allocation-mb", "8192")
//		// .set("spark.driver.maxResultSize", "4g")
//		// .set("spark.serializer",
//		// "org.apache.spark.serializer.KryoSerializer");
//		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//		ctx.setLogLevel("WARN");
//		SQLContext sqlContext = new SQLContext(ctx);
//
//		JavaRDD<String> people = ctx.textFile(args[1]);
//
//		String schemaString = tableField;
//		final DataTypUtils dataTypUtils = new DataTypUtils();
//		DataType[] dataTypesList = dataTypUtils
//				.transformDataTypeUtils(tableAttr);
//		List<StructField> fields = new ArrayList<StructField>();
//		String[] schemaList = schemaString.split("#");
//		final String[] schemaList1 = schemaList;
//
//		for (int i = 0; i < schemaList.length; i++) {
//			fields.add(DataTypes.createStructField(schemaList[i],
//					dataTypesList[i], true));
//
//		}
//
//		final String string = FieldSplitSign;
//		StructType schema = DataTypes.createStructType(fields);
//
//		JavaRDD<Row> rowRDD = people.map(new Function<String, Row>() {
//			public Row call(String record) throws Exception {
//				String[] fields = record.split(string);
//				// TODO maybe cause the memory leak
//
//				Object[] objeck = new Object[fields.length];
//
//				// if(fields.length!=schemaList1.length){
//				// objeck = new Object[schemaList1.length];
//				// }
//				// else{
//				// for(int i=0;i<fields.length;i++){
//				// objeck[i] =
//				// dataTypUtils.arrtToObject(fields[i].trim(),fieldAttyList[i].trim());
//				// }
//				// }
//				if (fields.length <= schemaList1.length) {
//					for (int i = 0; i < fields.length; i++) {
//						objeck[i] = dataTypUtils.arrtToObject(fields[i].trim(),
//								fieldAttyList[i].trim());
//					}
//				}
//				return RowFactory.create(objeck);
//			}
//		});
//
//		DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);
//		peopleDataFrame.registerTempTable(tablenamestr);
//
//		String sqlCondition = "select * from " + tablenamestr + " where "
//				+ args[2];
//		DataFrame results = sqlContext.sql(sqlCondition);
//		JavaRDD<Row> javaRDD = results.toJavaRDD();
//		// ��ǰ���������ȥ��, Ȼ�󱣴浽ָ��·��
//		javaRDD.flatMap(new FlatMapFunction<Row, String>() {
//			@Override
//			public Iterable<String> call(Row row) throws Exception {
//				String str = row.toString().trim()
//						.substring(1, row.toString().length() - 1);
//				ArrayList<String> arrayList = new ArrayList<String>();
//				arrayList.add(str);
//				return arrayList;
//			}
//		}).saveAsTextFile(args[3]);
//		ctx.stop();
//	}
}
