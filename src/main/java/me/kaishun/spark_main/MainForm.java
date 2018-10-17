package me.kaishun.spark_main;

import me.kaishun.conf.OperatorConf;
import me.kaishun.spark_engines.DataFilterSpark;
import org.dom4j.DocumentException;

import java.util.ArrayList;

public class MainForm  implements java.io.Serializable{
    /**
     *
     * @return an arrayList tableList
     * @throws DocumentException
     */
    public static void main(String[] args) {
        GetRowResults(args);
    }

    public static ResultsModel GetRowResults(String[] args) {
        FilterReadFromXml filterReadFromXml = new FilterReadFromXml();
        ArrayList<String> tableNames = filterReadFromXml.getTableName();
        /*加载conf参数*/
        try {
            OperatorConf.setConfFromConfig();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            ArrayList<FormModel> formModelLists = new ArrayList<>();
            /* 把所有的表的字段说明和数据路径等存放到FormModel中 */
            for (String tableName : tableNames) {
                FormModel formModel = filterReadFromXml.getFormModelFromTableName(tableName);
                formModelLists.add(formModel);
            }
            DataFilterSpark dataFilterSpark = new DataFilterSpark();
            // spark sql核心代码
            ResultsModel resultsModel = dataFilterSpark.doSpark(formModelLists, args);
            return resultsModel;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
