package me.kaishun.util;

import java.io.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class AutoGenerationTable {
    public static void main(String[] args) {
        //文本的那些转table
//        createTableByFile();
        // hive转table
        createTableByHive();
        // sqlserver转table
        createTableBySqlserver();
        // mysql转table
        creteTableByMysql();
    }

    private static void creteTableByMysql() {
        String xmlValue = "";
        xmlValue += createXmlHeard();

        DBHelper dbHelper = new DBHelper();
        dbHelper.setDBValue("root","root","111.230.28.12","STUDENT",32768);
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
        xmlValue+=createXmlEnd();
        System.out.println(xmlValue);

    }

    private static void createTableBySqlserver() {
    }

    private static void createTableByHive() {

    }


    private static void createTableByFile() {
        String sourcePath = "G:\\spark-sql-test\\create-sql\\aa.txt";
        String savePath = "G:\\spark-sql-test\\create-sql\\aatable.txt";
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
            result  += createXmlEnd();

        } catch (Exception e) {
            e.printStackTrace();
        }


        return result;
    }

    private static String createXmlHeard(){
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?> "+"\n"+"<table>"+"\n";
    }
    private static StringBuffer createXmlBody(StringBuffer field, String fieldName,String type, int pos){
        String model = "<fieldname fieldname=\"%1$s\" type=\"%2$s\" pos=\"%3$s\"/>";
        String body = String.format(model, fieldName, type, pos);
        field.append("    "+body).append("\n");
        return field;
    }
    private static String createXmlEnd(){
        String result = "    <splitsign></splitsign>"+"\n";
        result +="    <inputPath></inputPath>"+"\n";
        result +="</table>";
        return result;
    }

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
