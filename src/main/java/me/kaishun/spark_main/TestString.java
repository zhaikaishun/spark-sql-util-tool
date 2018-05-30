package me.kaishun.spark_main;

import jline.console.ConsoleReader;

import java.io.IOException;

public class TestString {
    public static void main(String[] args) throws IOException {
        ConsoleReader reader = new ConsoleReader();
        while(true){
            System.out.println("请输入sql");
            String sqlCondition = reader.readLine("spark-sql sql> ");
            System.out.println("请输入保存方式: show, saveAsText, saveToHive, saveAsParquet");
            String saveType = reader.readLine("spark sql type> ");
            System.out.println("line1: "+sqlCondition);
            System.out.println("line2: "+saveType);
        }
    }
}
