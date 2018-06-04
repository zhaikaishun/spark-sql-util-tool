package me.kaishun.conf;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class OperatorConf {
    public static HashMap<String,Boolean> operatorMap = new HashMap<>();

    //是否返回row
    public static final String IFRETURNROW = "ifreturnrow";
    public static final String SQL_CHAIN="sql.chain";
    public static ArrayList<String> sqlList = new ArrayList<>();
    public static boolean returnRow = false;
    public static void setConfFromConfig() throws Exception {
        String path = "conf_table/operator.conf"; // 路径
        Properties prop = new Properties();
        File file = new File(path);
        if(file.exists()){
            FileInputStream in = new FileInputStream(path);
            prop.load(in);
            in.close();
        }
        String rowInterface = (String) prop.get(IFRETURNROW);
        if(rowInterface!=null && rowInterface.toLowerCase().contains("yes")||rowInterface.contains("true")){
            returnRow = true;
        }
        String chain = (String) prop.get(SQL_CHAIN);
        if(chain==null){
            return;
        }
        String[] sqlIndexArrays = chain.split(",");

        for (String sqlIndexList : sqlIndexArrays) {
            String sql = prop.getProperty(sqlIndexList.trim()).replace(";","");
            if(sql!=null){
                sqlList.add(sql);
            }
        }

    }

    public static void main(String[] args) {
        try {
            setConfFromConfig();
            System.out.println(returnRow);
            for (String sql : sqlList) {
                System.out.println("sql: "+sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
