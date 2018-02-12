package me.kaishun.spark_main;

import org.apache.spark.sql.types.DataType;

public class FieldAttr  implements java.io.Serializable{

    public String fieldName;
    public String attribute;
    public int posIndex;
    public DataType sparkAttr;

    public FieldAttr(String fieldName, String attribute,String posIndex) {
        this.fieldName = fieldName;
        this.posIndex = Integer.parseInt(posIndex);
        this.attribute = attribute;
    }
}
