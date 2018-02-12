package me.kaishun.spark_v20;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import me.kaishun.spark_main.FieldAttr;
import me.kaishun.spark_main.FormModel;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.kohsuke.args4j.CmdLineParser;
import testArgs4j.ParseArgs;

public class DataFilterSpark {	
	public static void doSpark(ArrayList<FormModel> formModelLists,String[] args) throws Exception {

	    String FieldSplitSign = ",";
		ParseArgs parseArgs = new ParseArgs();
		CmdLineParser pars = new CmdLineParser(parseArgs);
		if(args.length==0){
			System.out.println("args is 0");
			return;
		}

		//	    System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-2.7.1");
//		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL")
//				.set("yarn.nodemanager.resource.memory-mb", "8192")
//				.set("yarn.scheduler.maximum-allocation-mb", "8192")
//				.set("spark.driver.maxResultSize", "4g")
//				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//	    SQLContext sqlContext = new SQLContext(ctx);
		SparkSession spark = SparkSession
				.builder()
				.appName("DataFilter").master("local")
				.getOrCreate();
//	    	      .config("yarn.nodemanager.resource.memory-mb", "8192")
//	    	      .config("yarn.scheduler.maximum-allocation-mb", "8192")
//	    	      .config("spark.driver.maxResultSize", "4g")
//	    	      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//	    	      .master("local") //写成可配置的

		// 设置log级别
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");


		// 加载几种表结构
		DataTypUtils dataTypUtils = new DataTypUtils();
		for (FormModel formModel:formModelLists) {
			dataTypUtils.transformDataTypeUtils(formModel);
			JavaRDD<String> people = spark.read().textFile(formModel.inputPath).javaRDD();
			List<StructField> fields = new ArrayList<StructField>();
			int maxSplitNum=0;
			for(FieldAttr fieldAttr:formModel.fieldAttrArrayList){
				fields.add(DataTypes.createStructField(fieldAttr.fieldName, fieldAttr.sparkAttr, true));
				if(fieldAttr.posIndex>maxSplitNum){
					maxSplitNum=fieldAttr.posIndex;
				}

			}
			StructType schema = DataTypes.createStructType(fields);
			final String string = FieldSplitSign;
			final int maxClosePackageSplitNum =  maxSplitNum;
			JavaRDD<Row> rowRDD = people.map(
					new Function<String, Row>() {
						public Row call(String record) throws Exception {
							String[] splits = record.split(string,maxClosePackageSplitNum+2);
							//TODO maybe cause the memory leak
							Object[] objeck = new Object[splits.length];
							if(splits.length<=formModel.fieldAttrArrayList.size()){
								for(int i=0;i<formModel.fieldAttrArrayList.size();i++){
									FieldAttr eachFieldAttr = formModel.fieldAttrArrayList.get(i);
									objeck[i] = dataTypUtils.arrtToObject(splits[eachFieldAttr.posIndex].trim(),
											eachFieldAttr.attribute);
								}
							}
							return RowFactory.create(objeck);
						}
					});
			Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
			peopleDataFrame.createOrReplaceTempView(formModel.tableName);

		}


//	    String sqlCondition = "select * from "+tablenamestr+" where "+args[2];
	    String sqlCondition = "select * from teacher";
	    Dataset<Row> results = spark.sql(sqlCondition);
	    results.show();
		Dataset<Row> student = spark.sql("select * from student");
		student.show();
		Dataset<Row> innerJoin = spark.sql("select * from teacher inner join " +
				"student on teacher.studentID = student.userID");
		innerJoin.show();

		if(parseArgs.outputType.toUpperCase().contains("text")){
			JavaRDD<Row> javaRDD = results.toJavaRDD();
			//将前后的中括号去掉, 然后保存到指定路径
			javaRDD.flatMap(new FlatMapFunction<Row, String>() {
				@Override
				public Iterator<String> call(Row row) throws Exception {
					//北京需求
					String str = row.toString().trim().
							substring(1,row.toString().length()-1).replaceAll(",", parseArgs.outputSplit);
					ArrayList<String> arrayList = new ArrayList<String>();
					arrayList.add(str);
					return arrayList.iterator();
				}
			}).saveAsTextFile(parseArgs.outputPath);
			spark.stop();
		}
		if(parseArgs.outputType.toUpperCase().contains("QUALITY")) {

		}
		if(parseArgs.outputType.toUpperCase().contains("HIVE")){

		}
		if(parseArgs.outputType.toUpperCase().contains("TODO")){

		}


	}
}
