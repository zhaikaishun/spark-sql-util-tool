package me.kaishun.util;

import java.io.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

public class AutoGenerationTable {
    public static void main(String[] args) {
        String savePath="G:\\spark-sql-test\\create-sql\\aatable.txt";
        //文本的那些转table
//        createTableByFile(savePath);
        // hive转table
        createTableByHive(savePath);
        // sqlserver转table
//        createTableBySqlserver(savePath);
        // mysql转table
//        creteTableByMysql(savePath);
    }

    /**
     * 通过mysql来创建xml的table表
     */
    private static void creteTableByMysql(String savePath) {
        String xmlValue = "";
        xmlValue += createXmlHeard();

        DBHelper dbHelper = new DBHelper();
        dbHelper.sqlserverOrMysqlOrHive = "mysql";
        dbHelper.setDBValue("user","passwd","192.168.x.xx","STUDENT",3306);
        dbHelper.GetConn();
        String sql = "show create table STUDENT";
        ResultSet rs = dbHelper.GetResultSet(sql);
        StringBuffer bodyBuffer = new StringBuffer();
        try {
            while (rs.next()){
                String result = rs.getString(2);
                System.out.println(result);
                String[] split = result.split("\n");
                int pos = -1;
                for (String s : split) {
                    if(s.contains("(") && pos==-1){
                        //开始
                        pos = 1;
                        continue;
                    }
                    if(s.contains("=")){
                        //结束
                        break;
                    }
                    if(pos>0){
                        String line = s.replace("`", "");
                        int kuohaoIndex = line.indexOf("(");
                        int end = kuohaoIndex>0?kuohaoIndex:line.length();
                        String fieldAndAttrStr = line.substring(0, end);
                        String[] fieldAndAttrArray = fieldAndAttrStr.trim().split(" ");
                        String field = fieldAndAttrArray[0].trim();
                        String attr = fieldAndAttrArray[1].trim();
                        createXmlBody(bodyBuffer,field,attr,pos);
                        pos++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            dbHelper.CloseConn();
        }
        xmlValue+=bodyBuffer.toString();
        xmlValue+=createXmlEnd("","");
        System.out.println(xmlValue);
        saveToFile(savePath,xmlValue);

    }

    /**
     * 通过sqlserver来创建xml的table表
     */
    private static void createTableBySqlserver(String savePath) {
        DBHelper dbHelper = new DBHelper();
        dbHelper.sqlserverOrMysqlOrHive = "sqlserver";
        dbHelper.setDBValue("user","passed","ip","MBD2_CITY_BEIJING",0);
        dbHelper.GetConn();
        String sql = "SELECT syscolumns.name,systypes.name\n" +
                "FROM syscolumns, systypes \n" +
                "WHERE syscolumns.xusertype = systypes.xusertype \n" +
                "AND syscolumns.id = object_id('MBD2_CITY_BEIJING.dbo.tb_evt_cell_dd_180126')";
        ResultSet rs = dbHelper.GetResultSet(sql);
        StringBuffer bodyBuffer = new StringBuffer();
        try {
            int pos = 1;

            while (rs.next()){
                String field = rs.getString(1);
                String attr = rs.getString(2);
                createXmlBody(bodyBuffer,field,attr,pos);
                pos++;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        String xmlValue = "";
        xmlValue += createXmlHeard();
        xmlValue += bodyBuffer.toString();
        xmlValue +=createXmlEnd("","");
        System.out.println(xmlValue);
        saveToFile(savePath,xmlValue);
    }

    /**
     * 通过hive来创建xml表
     * @param savePath
     */
    private static void createTableByHive(String savePath) {
        StringBuffer bodyBuffer = new StringBuffer();
        String splitSgn="";
        String location = "";
        DBHelper dbHelper = new DBHelper();
        dbHelper.sqlserverOrMysqlOrHive = "hive";
        dbHelper.setDBValue("user","passed","192.168.xx.xx","default",10000);

        String querySql = "show create TABLE tb_mr_cell_yd_dd_180125_2";
        ResultSet rs = dbHelper.GetResultSet(querySql);
        int pos = -1;
        try {
            while (rs.next()){
                String line = rs.getString(1);
                if(line.contains("(") && pos==-1){
                    //开始
                    pos=1;
                    continue;
                }

                if(pos>0){
                    String templine = line.replace("`", "").replace(",", "").replace(")","").trim();
                    String[] split = templine.split(" ");
                    String field = split[0];
                    String attr = split[1];
                    createXmlBody(bodyBuffer,field,attr,pos);
                    pos++;
                }
                if(line.contains(")")&&pos>0){
                    //读取结束
                    pos=0;
                }

                if(line.contains("FIELDS TERMINATED BY")){
                    splitSgn = line.replace("FIELDS TERMINATED BY", "").replace("'","").trim();
                }
                if(line.contains("LOCATION")){
                    rs.next();
                    location = rs.getString(1).replace("`","").replace("'","").trim();
                    break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String xmlValue="";
        xmlValue += createXmlHeard();
        xmlValue += bodyBuffer.toString();
        xmlValue += createXmlEnd(splitSgn,location);
        System.out.println(xmlValue);
        saveToFile(savePath,xmlValue);

    }


    /**
     * 通过createble的文件来创建xml表
     * @param savePath
     */
    private static void createTableByFile(String savePath) {
        String sourcePath = "G:\\spark-sql-test\\create-sql\\aa.txt";
        String xmlValue = createBySQLFile(sourcePath);
        System.out.println(xmlValue);
        saveToFile(savePath,xmlValue);
    }

    /**
     *
     * @param path 传入文件路径
     * @return 返回xml的组成的字符串，后续需要保存
     */
    private static String createBySQLFile(String path) {
        String result = "";
        result += createXmlHeard();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(path);
            InputStreamReader in = new InputStreamReader(fis);
            BufferedReader br = new BufferedReader(in);
            String line="";
            int pos=-1;
            StringBuffer bodyBuffer = new StringBuffer();
            while ((line=br.readLine())!=null){
                if(line.toUpperCase().contains("CREATE TABLE")){
                    pos=1;
                    continue;
                }
                //请吧 )另起一行，不要和字段相同了
                if(line.toUpperCase().contains(")")){
                    break;
                }
                if(pos>0){
                    line.split("\t");
                    line = line.replace("[", "");
                    line = line.replace("]", "");
                    line = line.replace(",", "");
                    if(line.startsWith("\t")){
                        line = line.substring(1);
                    }
                    line = line.trim();
                    String[] split = line.split(" ");
                    String field = split[0];
                    String type = split[1];
                    createXmlBody(bodyBuffer,field,type,pos);
                    pos++;
                }
            }
            result += bodyBuffer.toString();
            result  += createXmlEnd("","");

        } catch (Exception e) {
            e.printStackTrace();
        }


        return result;
    }

    /**
     * 创建xml表头
     * @return
     */
    private static String createXmlHeard(){
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?> "+"\n"+"<table>"+"\n";
    }

    /**
     * 创建xml表body
     * @param field
     * @param fieldName
     * @param type
     * @param pos
     * @return
     */
    private static StringBuffer createXmlBody(StringBuffer field, String fieldName,String type, int pos){
        String model = "<fieldname fieldname=\"%1$s\" type=\"%2$s\" pos=\"%3$s\"/>";
        String body = String.format(model, fieldName, type, pos);
        field.append("    "+body).append("\n");
        return field;
    }

    /**
     * 创建xml表尾
     * @return
     */
    private static String createXmlEnd(String splitsgn,String location){
        String result = "    <splitsign>"+splitsgn+"</splitsign>"+"\n";
        result +="    <inputPath>"+location+"</inputPath>"+"\n";
        result +="</table>";
        return result;
    }

    /**
     * 保存到文件
     * @param path
     * @param value
     */
    public static void saveToFile(String path,String value){

        try{
            File file=new File(path);
            if(!file.exists()){
                file.createNewFile();
            }
            FileOutputStream out= null;
            out = new FileOutputStream(file,true);
            out.write(value.toString().getBytes("utf-8"));
            out.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
