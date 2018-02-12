package me.kaishun.spark_v20;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import me.kaishun.spark_main.FieldAttr;
import me.kaishun.spark_main.FormModel;
import me.kaishun.spark_main.SparkConfUtil;
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
		pars.parseArgument(args);

		SparkSession spark = SparkConfUtil.getSparkSession();


		/* 加载所有的表结构 */
		DataTypUtils dataTypUtils = new DataTypUtils();
		for (FormModel formModel:formModelLists) {
			dataTypUtils.transformDataTypeUtils(formModel);
			JavaRDD<String> people = spark.read().textFile(formModel.inputPath).javaRDD();
			List<StructField> fields = new ArrayList<StructField>();
			// 最大分割数，因为有的数据不需要分割那么多
			int maxSplitNum=0;
			for(FieldAttr fieldAttr:formModel.fieldAttrArrayList){
				fields.add(DataTypes.createStructField(fieldAttr.fieldName, fieldAttr.sparkAttr, true));
				if(fieldAttr.posIndex>maxSplitNum){
					maxSplitNum=fieldAttr.posIndex;
				}

			}
			// sparkSql 注册表结构
			StructType schema = DataTypes.createStructType(fields);
			final String string = FieldSplitSign;
			//最大分割数final一下
			final int maxClosePackageSplitNum =  maxSplitNum;
			/* 根据表结构加载数据 */
			JavaRDD<Row> rowRDD = people.map(
					new Function<String, Row>() {
						public Row call(String record) throws Exception {
							String[] splits = record.split(string,maxClosePackageSplitNum+2);
							Object[] objeck = new Object[splits.length];
							if(splits.length<=formModel.fieldAttrArrayList.size()){
								for(int i=0;i<formModel.fieldAttrArrayList.size();i++){
									FieldAttr eachFieldAttr = formModel.fieldAttrArrayList.get(i);
									objeck[i] = dataTypUtils.arrtToObject(splits[eachFieldAttr.posIndex-1].trim(),
											eachFieldAttr.attribute);
								}
							}
							return RowFactory.create(objeck);
						}
					});
			// 根据表数据和表结构，注册DataSet
			Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
			// 给这种数据添加一个表名
			peopleDataFrame.createOrReplaceTempView(formModel.tableName);
		}
		// 测试一下
	    String sqlCondition = "select * from teacher";
	    Dataset<Row> results = spark.sql(sqlCondition);
	    results.show();
		Dataset<Row> student = spark.sql("select * from student");
		student.show();
		Dataset<Row> innerJoin = spark.sql("select * from teacher inner join " +
				"student on teacher.studentID = student.userID");
		innerJoin.show();

		System.out.println("parseArgs.outputPath: "+parseArgs.outputPath);

		if(parseArgs.outputType.toUpperCase().contains("TXT")){
			JavaRDD<Row> javaRDD = results.toJavaRDD();
			//将前后的中括号去掉, 然后保存到指定路径
			javaRDD.flatMap(new FlatMapFunction<Row, String>() {
				@Override
				public Iterator<String> call(Row row) throws Exception {
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
