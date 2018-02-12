package me.kaishun.spark_main;

import java.util.ArrayList;

public class FormModel implements java.io.Serializable{
    // 数据存放路径
    public String inputPath;
    //表名
    public String tableName;
    //数据库分隔符
    public String splitSign;

    public String splitOutSign;
    // 字段和属性的list
    public ArrayList<FieldAttr> fieldAttrArrayList;

    public FormModel() {
        fieldAttrArrayList = new ArrayList<>();
//        inputPath = "F:\\sparksqlutil\\resources\\people.txt";
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setSplitSign(String splitSign) {
        this.splitSign = splitSign;
    }

    public void setSplitOutSign(String splitOutSign) {
        this.splitOutSign = splitOutSign;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }
}

