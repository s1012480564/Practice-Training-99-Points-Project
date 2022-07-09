carOriginData项目生成了随机的原始数据
并在carOriginData中通过mapreduce进行统计插入到了carinfo数据库的com_sale、product、sale_cost、total这三个表中
carOriginData项目生成的随机数据剪切到了data/car_origin_data文件夹下
其他数据表，company、com_relation、target、tar_relation，维护的是名称代号和关系等，这部分是手动编的，导出的txt在data/other table文件夹下

整个carinfo数据库可以直接导入.sql文件，忽略掉上面说的全部原始数据生成和处理的部分

mysql数据表通过sqoop离线同步数据到hdfs

可以直接把data/carinfo下的文件上传到hdfs，忽略掉上面说的sqoop同步的部分
路径就是hdfs根目录下carinfo，hdfs://192.168.17.10:9000/carinfo

pmml_py是python机器学习的部分，简单地做了模型选择，并将训练好的模型参数通过pmml包保存留给Java使用
其中由于版本对应问题，pmml文件中，要手动把版本4_4改成4_3
这里我已经把改成4_3的pmml放在了可视化maven项目的resources下了，可以直接忽略掉上面说的所有部分

图染色算法原型.cpp是批量删除有向图依赖结构的指标时设计的O(n)的图染色算法的C++算法原型，为了看着方便，放在了这里。

carFinaceAnalyseSystem是主项目，war包部署在tomcat上运行即可

连接使用的mysql用户是username:root，pwd:root，路径jdbc:mysql://192.168.17.10:3306/carinfo?characterEncoding=utf-8




另外需要注意的一个小问题是，pmml_py项目下用到的income.csv、profit.csv是求和过后的，sale.csv的款项名称是中文名称离散化标记成1,2,3,4,5,6之后的。
通过mapreduce将carinfo中的表求和、处理成income.csv、profit.csv、sale.csv这三个表的代码被我用完就误删了。需要重现这部分处理过程的话，要自己再手动实现一下。

