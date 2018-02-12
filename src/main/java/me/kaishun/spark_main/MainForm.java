package me.kaishun.spark_main;

import me.kaishun.spark_v20.DataFilterSpark;
import org.dom4j.DocumentException;

import java.util.ArrayList;

public class MainForm  implements java.io.Serializable{
    /**
     *
     * @return an arrayList tableList
     * @throws DocumentException
     */
    public static void main(String[] args) {
        FilterReadFromXml filterReadFromXml = new FilterReadFromXml();
        ArrayList<String> tableNames = filterReadFromXml.getTableName();
        try {
            ArrayList<FormModel> formModelLists = new ArrayList<>();
            /* 把所有的表的字段说明和数据路径等存放到FormModel中 */
            for (String tableName : tableNames) {
                FormModel formModel = filterReadFromXml.getFormModelFromTableName(tableName);
                formModelLists.add(formModel);
            }

            DataFilterSpark dataFilterSpark = new DataFilterSpark();
            // spark sql核心代码
            dataFilterSpark.doSpark(formModelLists,args);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
