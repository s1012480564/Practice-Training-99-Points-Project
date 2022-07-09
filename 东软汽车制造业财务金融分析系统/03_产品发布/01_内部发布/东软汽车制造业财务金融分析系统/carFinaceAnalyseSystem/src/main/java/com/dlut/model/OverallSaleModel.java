package com.dlut.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

public class OverallSaleModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static double[][] a;//按照月报报告内嵌表02的对应位置填写二维数组
    private static TreeMap<String,Integer> sale;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class OverallSaleMapper extends Mapper<Object, Text, Text,IntWritable> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            int num=Integer.parseInt(strs[3]);
            context.write(new Text(strs[2]+" "+(year*100+month)),new IntWritable(num));
        }
    }
    private static class OverallSaleReducer extends Reducer<Text,IntWritable,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            for(IntWritable val:values) {
                sale.put(key.toString(),val.get()+sale.getOrDefault(key.toString(),0));
            }
        }
        public void cleanup(Context context) throws IOException,InterruptedException {
            String[] name={"一汽集团","一汽丰田销售","一汽-大众","吉林汽车","轿车公司",
                        "解放公司","天津夏利","客车公司","通用公司","红旗事业部","一汽海马"};
            jsonmap.put("name",name);
            int ym=y*100+m;
            int yPreM=y,mPreM=m-1;//上个月的年月
            if(mPreM==0){
                yPreM--;
                mPreM=12;
            }
            int ymPreM=yPreM*100+mPreM;
            for(int i=0;i<11;i++){
                int sum=0,sumPre=0;
                for(int j=1;j<=12;j++) {
                    sum+=sale.get(name[i]+" "+(y*100+j));
                    sumPre+=sale.get(name[i]+" "+((y-1)*100+j));
                }
                a[i][0]=sale.get(name[i]+" "+ym);
                a[i][1]=sale.get(name[i]+" "+ymPreM);
                a[i][2]=a[i][0]-a[i][1];
                //环比，百分数，保留两位小数
                a[i][3]=Math.round((a[i][0]-a[i][1])/a[i][0]*10000.0)/100.0;
                a[i][4]=sum;
                a[i][5]=sumPre;
                a[i][6]=sum-sumPre;
                a[i][7]=Math.round((a[i][4]-a[i][5])/a[i][4]*10000.0)/100.0;
            }
            context.write(new Text("单位"),new Text("本月 上月 增减量 增减率 本期 同期 增减量 增减率"));
            for(int i=0;i<11;i++){
                String val=""+a[i][0];
                for(int j=1;j<8;j++) val+=" "+a[i][j];
                context.write(new Text(name[i]),new Text(val));
            }
        }
    }
    public static HashMap overallSaleCount() throws Exception{
        jsonmap=new HashMap();
        a=new double[11][8];
        sale=new TreeMap<String,Integer>();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countOverallSale";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(OverallSaleModel.class); //指定作业类
        job.setMapperClass(OverallSaleMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(IntWritable.class); //指定Map输出Value的数据类型
        job.setReducerClass(OverallSaleReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/product"; //实验数据目录
        String outputDir="/carresult/overallsale/"+(y*100+m); //实验输出目录
        Path inPath=new Path(hdfs+dataDir);
        Path outPath=new Path(hdfs+outputDir);
        FileInputFormat.addInputPath(job,inPath); //为作业添加输入路径
        FileOutputFormat.setOutputPath(job,outPath); //为作业添加输出路径
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(outPath)){ //如果输出目录已存在则删除
            fs.delete(outPath,true);
        }
        // 4.运行作业
        System.out.println("Job: "+jobName+" is running...");
        if(job.waitForCompletion(true)){
            System.out.println("success!");
        }
        else{
            System.out.println("failed!");
            System.exit(1);
        }
        jsonmap.put("dataset",a);
        return jsonmap;
    }
}
