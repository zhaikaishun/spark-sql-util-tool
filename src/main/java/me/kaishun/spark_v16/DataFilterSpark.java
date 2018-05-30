package me.kaishun.spark_v16;

import jline.console.ConsoleReader;
import me.kaishun.spark_main.FieldAttr;
import me.kaishun.spark_main.FormModel;
import me.kaishun.spark_v20.DataTypUtils;
import me.kaishun.spark_v20.SaveRddFuncUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DataFilterSpark {
    public static void doSpark(ArrayList<FormModel> formModelLists, String[] args) throws Exception {

        String FieldSplitSign = ",";
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spark-sql").setMaster("local");
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(ctx);
		/* 加载所有的表结构 */
        final DataTypUtils dataTypUtils = new DataTypUtils();
        for (final FormModel formModel : formModelLists) {
            dataTypUtils.transformDataTypeUtils(formModel);
            JavaRDD<String> people = ctx.textFile(formModel.inputPath);
            List<StructField> fields = new ArrayList<StructField>();
            // 最大分割数，因为有的数据不需要分割那么多
            int maxSplitNum = 0;
            for (FieldAttr fieldAttr : formModel.fieldAttrArrayList) {
                fields.add(DataTypes.createStructField(fieldAttr.fieldName, fieldAttr.sparkAttr, true));
                if (fieldAttr.posIndex > maxSplitNum) {
                    maxSplitNum = fieldAttr.posIndex;
                }

            }
            // sparkSql 注册表结构
            StructType schema = DataTypes.createStructType(fields);
            final String splitsign = FieldSplitSign;
            // 最大分割数final一下
            final int maxClosePackageSplitNum = maxSplitNum;
			/* 根据表结构加载数据 */
            JavaRDD<Row> rowRDD = people.map(new Function<String, Row>() {
                public Row call(String record) throws Exception {
                    String[] splits = record.split(splitsign, maxClosePackageSplitNum + 2);
                    Object[] objeck = new Object[splits.length];
                    if (splits.length <= formModel.fieldAttrArrayList.size()) {
                        for (int i = 0; i < formModel.fieldAttrArrayList.size(); i++) {
                            FieldAttr eachFieldAttr = formModel.fieldAttrArrayList.get(i);
                            objeck[i] = dataTypUtils.arrtToObject(splits[eachFieldAttr.posIndex - 1].trim(),
                                    eachFieldAttr.attribute);
                        }
                    }
                    return RowFactory.create(objeck);
                }
            });
            // 根据表数据和表结构，注册DataSet
            DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schema);
            // 给这种数据添加一个表名
            dataFrame.registerTempTable(formModel.tableName);
        }
        // 注意，如果是eclipse等IDE测试，需要在VM中加  -Djline.terminal=jline.UnsupportedTerminal
        ConsoleReader reader = new ConsoleReader();
        while(true){
            System.out.println("请输入sql");
            String sqlCondition = reader.readLine("spark-sql sql> ");
            if(sqlCondition.endsWith(";")){
                sqlCondition = sqlCondition.substring(0, sqlCondition.length() - 1);
            }
            System.out.println("请输入保存方式: show, saveAsText, saveToHive, saveAsParquet");
            String saveType = reader.readLine("spark sql type> ");
            DataFrame results = sqlContext.sql(sqlCondition);

            if (saveType.toUpperCase().contains("saveAsText")) {
                System.out.println("请输入路径保存");
                String savePath = reader.readLine("spark sql savePath> ");
                System.out.println("请输入保存的分割符号");
                String splitSng = reader.readLine("spark sql splitSng> ");
                SaveRddFuncUtil.saveAsTextFile(savePath,splitSng, results);
            }
            if (saveType.toUpperCase().contains("saveAsParquet")) {
//                SaveRddFuncUtil.saveAsParquet("path", spark, results);
            }
            if (saveType.toUpperCase().contains("saveToHive")) {
//                SaveRddFuncUtil.saveToHive("path", spark, results);
            }
            if (saveType.toUpperCase().contains("SHOW")) {
                results.show();
            }

        }

    }

}
