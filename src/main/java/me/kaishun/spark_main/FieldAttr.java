package me.kaishun.spark_main;

import org.apache.spark.sql.types.DataType;

public class FieldAttr  implements java.io.Serializable{

    // 字段名
    public String fieldName;
    // 属性
    public String attribute;
    //第几个位置
    public int posIndex;
    // spark属性
    public DataType sparkAttr;

    public FieldAttr(String fieldName, String attribute,String posIndex) {
        this.fieldName = fieldName;
        this.posIndex = Integer.parseInt(posIndex);
        this.attribute = attribute;
    }
}
