package me.kaishun.conf;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class OperatorConf {
    // sql的列表
    public static boolean returnRow = false;
    public static ArrayList<String> sqlList = new ArrayList<>();
    public static ArrayList<String> operatorList = new ArrayList<>();

    public static void setConfFromConfig() throws Exception {
        String path = "conf_table/operator.conf"; // 路径
        Properties prop = new Properties();
        File file = new File(path);
        if(file.exists()){
            FileInputStream in = new FileInputStream(path);
            prop.load(in);
            in.close();
        }
        returnRow = Boolean.parseBoolean(prop.getProperty("ifreturnrow", "false"));

        String sqlchain = (String) prop.get("sql.chain");
        String[] sqlIndexArrays = sqlchain.split("###");
        for (String sqlIndexList : sqlIndexArrays) {
            String sql = prop.getProperty(sqlIndexList.trim()).replace(";","");
            if(sql!=null){
                sqlList.add(sql);
            }
        }
        String operatorChain = (String) prop.get("operator.chain");
        String[] operatorArray = operatorChain.split("###");
        for (String operatorStr : operatorArray) {
            operatorList.add(operatorStr);
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
