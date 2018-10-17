package me.kaishun.spark_engines;
import jline.console.ConsoleReader;
import me.kaishun.conf.OperatorConf;
import me.kaishun.spark_main.FieldAttr;
import me.kaishun.spark_main.FormModel;
import me.kaishun.spark_main.ResultsModel;
import me.kaishun.spark_main.SparkConfUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;
public class DataFilterSpark {

        public ResultsModel doSpark(ArrayList<FormModel> formModelLists, String[] args) throws Exception {
            String FieldSplitSign = ",";

            SparkSession spark = SparkConfUtil.getSparkSession();
            /* 加载所有的表结构 */
            final DataTypUtils dataTypUtils = new DataTypUtils();
            for (final FormModel formModel : formModelLists) {
                dataTypUtils.transformDataTypeUtils(formModel);
                JavaRDD<String> people = spark.read().textFile(formModel.inputPath).javaRDD();
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
                final String splitsign = formModel.splitSign;
                // 最大分割数final一下
                final int maxClosePackageSplitNum = maxSplitNum;
                /* 根据表结构加载数据 */
                JavaRDD<Row> rowRDD = people.map(new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        try{
                            String[] splits = record.split(splitsign, maxClosePackageSplitNum + 2);
                            Object[] objeck = new Object[splits.length];
                            for (int i = 0; i < formModel.fieldAttrArrayList.size(); i++) {
                                FieldAttr eachFieldAttr = formModel.fieldAttrArrayList.get(i);
                                objeck[i] = dataTypUtils.arrtToObject(splits[eachFieldAttr.posIndex - 1].trim(),
                                        eachFieldAttr.attribute);
                            }
                            return RowFactory.create(objeck);
                        }catch (Exception e){
                            e.printStackTrace();
                            return null;
                        }
                    }
                });
                JavaRDD<Row> filterRDD = rowRDD.filter(new Function<Row, Boolean>() {
                    @Override
                    public Boolean call(Row row) throws Exception {
                        if (row != null) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

                // 根据表数据和表结构，注册DataSet
                Dataset<Row> dataFrame = spark.createDataFrame(filterRDD, schema);
                // 给这种数据添加一个表名
                dataFrame.createOrReplaceTempView(formModel.tableName);
            }
            ArrayList<Dataset<Row>> rowDatasetLists = new ArrayList<>();
            // 注意，如果是eclipse等IDE测试，需要在VM中加  -Djline.terminal=jline.UnsupportedTerminal
            if(OperatorConf.returnRow){

                for (int i = 0; i < OperatorConf.sqlList.size(); i++) {
                    String sql = OperatorConf.sqlList.get(i);
                    Dataset<Row> rowDataset = spark.sql(sql);
                    String operator = OperatorConf.operatorList.get(i);
                    String splitSng = "|";
                    if (operator.contains("save")){
                        String[] opLists = operator.split("&&");
                        String operatorSavePath = opLists[0].replace("save:","");
                        splitSng = opLists[0].replace("splitSng:","");
                        SaveRddFuncUtil.saveAsTextFile(operatorSavePath,splitSng, spark, rowDataset);
                    }
                    if(operator.contains("return")){
                        rowDatasetLists.add(rowDataset);
                    }
                }
                return new ResultsModel(rowDatasetLists,spark);
            }else{
                ConsoleReader reader = new ConsoleReader();
                while(true){
                    System.out.println("请输入sql");
                    String sqlCondition = reader.readLine("spark-sql sql> ");
                    if(sqlCondition.endsWith(";")){
                        sqlCondition = sqlCondition.substring(0, sqlCondition.length() - 1);
                    }
                    System.out.println("请输入保存方式: show, saveAsText, saveToHive, saveAsParquet");
                    String saveType = reader.readLine("spark sql type> ");
                    Dataset<Row> results = spark.sql(sqlCondition);

                    if (saveType.toLowerCase().contains("saveastext")) {
                        System.out.println("请输入路径保存");
                        String savePath = reader.readLine("spark sql savePath> ");
                        System.out.println("请输入保存的分割符号");
                        String splitSng = reader.readLine("spark sql splitSng> ");
                        SaveRddFuncUtil.saveAsTextFile(savePath,splitSng, spark, results);
                    }
                    if (saveType.toLowerCase().contains("saveasparquet")) {
                        SaveRddFuncUtil.saveAsQuality("path", spark, results);
                    }
                    if (saveType.toLowerCase().contains("savetohive")) {
                        SaveRddFuncUtil.saveToHive("path", spark, results);
                    }
                    if (saveType.toLowerCase().contains("show")) {
                        results.show();
                    }
                }

            }

        }
}
