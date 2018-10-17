## 1. 功能  
spark sql工具，自己配置spark表，就可以类似于hive命令行的操作，
也可以配置sql语句，直接执行

## 2. 使用方法  
### 2.1 配置表结构  
jar包当前路径下创建conf_table目录  
在当前目录下创建表，配置表名称和字段属性，其中
文件名为:表名.xml  
一个文件代表一张表
里面的内容如下例子  
student.xml
```$xslt
<?xml version="1.0" encoding="UTF-8"?>
<table>
		<fieldname fieldname="userID" type="string" pos="1"/>
		<fieldname fieldname="name" type="string" pos="2"/>
		<fieldname fieldname="age" type="int" pos="3"/>
		<splitsign>,</splitsign>
		<inputPath>hdfs://192.168.1.31:9000/tmp/zks1/student.txt</inputPath>
</table>
```
其中
fieldname为字段名  
type为属性  
pos为第几个字段，从1开始
inputPath为hdfs的路径  

### 2.2 运行方式  
目前支持2中运行方式  
1. 将sql写到文件中，执行sql语句
2. 直接运行，类似于hive的方式运行  

1. 将sql写到文件中如下  
conf_table/operator.conf文件  
```
ifreturnrow=true
sql1=select * from aa
sql2 = sql * from bb
sql3 = select * from cc
sql.chain = sql1###sql2###sql3
operator.chain=save:path1&&splitSng:,###save:path2###save:path3
```
ifreturnrow为true时才会走第一种方法  
接下来写sql语句，每个sql写一行  
sql.chain将执行哪些sql写成链表，使用###分割  
operator.chain填写sql.chain的各个sql的保存方式，用###分割，其中save:后接保存路径,splitSng接分隔符  

2. 使用交互的方式执行sql  
con_table/operator.conf中的ifreturnrow写成false  
操作根据提示即可  


### 3. 调试
IDEA或者ecipse调试,如果用到交互的方式，  
需要在VM中加  -Djline.terminal=jline.UnsupportedTerminal

