## 目的  
TODO  一个spark sql工具，虽然有了hive sql工具，不过我还是希望能做个这个,目前正在开发中，敬请期待
## 功能
 
- 配置数据库功能
-  使用XML来配置数据库表和字段的对应属性
-  xml文件 应该再提供一个数字，说明是第几个字段


**读数据**  

本地：  
- hdfs结构化数据
- hive的数据
- json数据
- csv

**hdfs：** 

- hdfs结构化数据
- hive的数据
- json数据
- xml数据
一次只能有一种格式

**查询功能**

- 支持普通的sql查询
- 支持多表的关联查询

**保存功能**

- 保存到hdfs文本结构化数据，支持保存的分隔符
- 支持压缩
- 支持保存为paquarty
- 支持输出到控制台

**其他**

-  同时支持1.6到2.x的版本
- 交互式的控制台输出命令： 只要在控制台引入一下package，那么就可以交互式查询了
- 中间应该有日志的吐出等功能
- showtables
- testshwtables


**输入路径和输出路径**  

既可以配置文件，也可以指定输入
支持不用sparksql的方式，直接本地java的读取的方式

**-- help的功能 ** 

在启动spark-sql时，如果不指定master，则以local的方式运行，master既可以指定standalone的地址，也可以指定yarn；  

具体代码： 还有点小bug，先暂不放出来