package me.kaishun.spark_v20;

import java.util.Date;

//import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
//import org.apache.mina.util.byteaccess.IoAbsoluteReader;
import me.kaishun.spark_main.FieldAttr;
import me.kaishun.spark_main.FormModel;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
//import org.junit.Test;

/**
 * 数据类型转换工具, 将基本数据类型转换为DataType类型, 将基本数据类型转换为其包装类
 * @author Zhaikaishun
 *
 */
public class DataTypUtils implements java.io.Serializable {
	/**
	 * @return	return a DataType[]
	 */
	
	public void transformDataTypeUtils(FormModel formModel){
		for (FieldAttr fieldAttr:formModel.fieldAttrArrayList) {
			attrToDataType(fieldAttr);
		}
	}
	/**
	 * @return DataType
	 */
	public void attrToDataType(FieldAttr fieldAttr){

		fieldAttr.sparkAttr = DataTypes.StringType;
		
		switch(fieldAttr.attribute.toLowerCase()){
		case "short" :
			fieldAttr.sparkAttr = DataTypes.StringType;
			break;
		case "char" :
			fieldAttr.sparkAttr = DataTypes.StringType;
			break;
		case "int" :
			fieldAttr.sparkAttr = DataTypes.IntegerType;
			break;
		case "integer" :
			fieldAttr.sparkAttr = DataTypes.IntegerType;
			break;
		case "float" :
			fieldAttr.sparkAttr = DataTypes.FloatType;
			break;
		case "double" :
			fieldAttr.sparkAttr = DataTypes.DoubleType;
			break;
		case "string" :
			fieldAttr.sparkAttr = DataTypes.StringType;
			break;
		case "varchar":
			fieldAttr.sparkAttr = DataTypes.StringType;
			break;
		case "long":
			fieldAttr.sparkAttr = DataTypes.LongType;
			break;
		case "bigint":
			fieldAttr.sparkAttr = DataTypes.LongType;
			break;
		case "boolean":
			fieldAttr.sparkAttr = DataTypes.BooleanType;
			break;
		case "date":
			fieldAttr.sparkAttr = DataTypes.DateType;
			break;
		}
	}
	/**
	 * @param fields field name
	 * @param attrstr field attribute
	 * @return an object -- String,Integer,Long and etc
	 */
	public Object arrtToObject(String fields,String attrstr){
		Object fieldObject = fields;
		switch(attrstr.toLowerCase()){
		case "short" :
			fieldObject = Short.parseShort(fields);
			break;		
		case "char" :
			fieldObject = fields;
			break;
		case "int" :
			fieldObject = Integer.parseInt(fields);
			break;
		case "integer" :
			fieldObject = Integer.parseInt(fields);
			break;
		case "float" :
			fieldObject = Float.parseFloat(fields);
			break;
		case "double" :
			fieldObject = Double.parseDouble(fields);
			break;
		case "long":
			fieldObject = Long.parseLong(fields);
			break;
		case "bigint":
			fieldObject = Long.parseLong(fields);
			break;
		case "boolean":
			fieldObject = Boolean.parseBoolean(fields);
			break;
		case "date":
			fieldObject = Date.parse(fields);
			break;
		}
		return fieldObject;
	}
}
