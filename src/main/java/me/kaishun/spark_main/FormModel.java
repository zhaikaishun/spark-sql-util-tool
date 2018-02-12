package me.kaishun.spark_main;

import java.util.ArrayList;

public class FormModel implements java.io.Serializable{
    public String inputPath;
    public String tableName;
    public String splitSign;
    public String splitOutSign;
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

