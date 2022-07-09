package com.dlut.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class OverallEconomyModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static double[][] a;//按照月报报告内嵌表01的对应位置填写二维数组
    private static TreeMap<Integer,Double> yield;
    private static TreeMap<Integer,Double> sale;
    private static TreeMap<Integer,Double> stock;
    private static TreeMap<Integer,Double> asset;
    private static TreeMap<Integer,Double> debt;
    private static TreeMap<Integer,Double> interest;
    private static TreeMap<Integer,Double> income;
    private static TreeMap<Integer,Double> profit;
    private static TreeMap<Integer,Double> fasset;
    private static TreeMap<Integer,Double> fdebt;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class TotalCount{
        private static class TotalMapper extends Mapper<Object, Text, IntWritable,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                String s=strs[2];
                for(int i=3;i<=7;i++) s+=" "+strs[i];
                //yield stock debt interest fasset fdebt
                context.write(new IntWritable(year*100+month),new Text(s));
            }
        }
        private static class TotalReducer extends Reducer<IntWritable,Text,Text,Text> {
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException{
                for(Text val:values) {
                    String[] strs=val.toString().split(" ");
                    int yieldNum=Integer.parseInt(strs[0]);
                    int stockNum=Integer.parseInt(strs[1]);
                    double debtMoney=Double.parseDouble(strs[2]);
                    double interestMoney=Double.parseDouble(strs[3]);
                    double fassetMoney=Double.parseDouble(strs[4]);
                    double fdebtMoney=Double.parseDouble(strs[5]);
                    yield.put(key.get(),yieldNum+yield.getOrDefault(key.get(),0.0));
                    stock.put(key.get(),stockNum+stock.getOrDefault(key.get(),0.0));
                    debt.put(key.get(),debtMoney+debt.getOrDefault(key.get(),0.0));
                    interest.put(key.get(),interestMoney+interest.getOrDefault(key.get(),0.0));
                    fasset.put(key.get(),fassetMoney+fasset.getOrDefault(key.get(),0.0));
                    fdebt.put(key.get(),fdebtMoney+fdebt.getOrDefault(key.get(),0.0));
                }
            }
        }
        private static void countTotal() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countTotal";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(TotalCount.class); //指定作业类
            job.setMapperClass(TotalMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(TotalReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/total"; //实验数据目录
            String outputDir="/carresult/overalleconomy/"+(y*100+m); //实验输出目录
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
        }
    }
    private static class ProductCount{
        private static class ProductMapper extends Mapper<Object, Text, IntWritable,IntWritable> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                int num=Integer.parseInt(strs[3]);
                context.write(new IntWritable(year*100+month),new IntWritable(num));
            }
        }
        private static class ProductReducer extends Reducer<IntWritable,IntWritable,Text,Text> {
            @Override
            public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException{
                for(IntWritable val:values) {
                    sale.put(key.get(),val.get()+sale.getOrDefault(key.get(),0.0));
                }
            }
        }
        private static void countProduct() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countProduct";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(ProductCount.class); //指定作业类
            job.setMapperClass(ProductMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(IntWritable.class); //指定Map输出Value的数据类型
            job.setReducerClass(ProductReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/product"; //实验数据目录
            String outputDir="/carresult/overalleconomy/"+(y*100+m); //实验输出目录
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
        }
    }
    private static class ComSaleCount{
        private static class ComSaleMapper extends Mapper<Object, Text, IntWritable,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                String s=strs[3]+" "+strs[4]+" "+strs[5];
                //profit asset income
                context.write(new IntWritable(year*100+month),new Text(s));
            }
        }
        private static class ComSaleReducer extends Reducer<IntWritable,Text,Text,Text> {
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException{
                for(Text val:values) {
                    String[] strs=val.toString().split(" ");
                    double profitMoney=Double.parseDouble(strs[0]);
                    double assetMoney=Double.parseDouble(strs[1]);
                    double incomeMoney=Double.parseDouble(strs[2]);
                    profit.put(key.get(),profitMoney+profit.getOrDefault(key.get(),0.0));
                    asset.put(key.get(),assetMoney+asset.getOrDefault(key.get(),0.0));
                    income.put(key.get(),incomeMoney+income.getOrDefault(key.get(),0.0));
                }
            }
            public void calNum(TreeMap<Integer,Double> map,int k){
                int ym=y*100+m;
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                double sum=0,sumPre=0;
                for(int i=1;i<=12;i++) {
                    sum+=map.get(y*100+i);
                    sumPre+=map.get((y-1)*100+i);
                }
                //车辆数万辆，保留两位小数
                a[k][0]=Math.round(map.get(ym)/100.0)/100.0;
                a[k][1]=Math.round(map.get(ymPreM)/100.0)/100.0;
                a[k][2]=Math.round((a[k][0]-a[k][1])*100.0)/100.0;
                a[k][3]=Math.round(sum/100.0)/100.0;
                a[k][4]=Math.round(sumPre/100.0)/100.0;
                a[k][5]=Math.round((a[k][3]-a[k][4])*100.0)/100.0;
            }
            public void calMoney(TreeMap<Integer,Double> map,int k){
                int ym=y*100+m;
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                double sum=0,sumPre=0;
                for(int i=1;i<=12;i++) {
                    sum+=map.get(y*100+i);
                    sumPre+=map.get((y-1)*100+i);
                }
                //金额数单位亿元，保留两位小数
                a[k][0]=Math.round(map.get(ym)/1000000.0)/100.0;
                a[k][1]=Math.round(map.get(ymPreM)/1000000.0)/100.0;
                a[k][2]=Math.round((a[k][0]-a[k][1])*100.0)/100.0;
                a[k][3]=Math.round(sum/1000000.0)/100.0;
                a[k][4]=Math.round(sumPre/1000000.0)/100.0;
                a[k][5]=Math.round((a[k][3]-a[k][4])*100.0)/100.0;
            }
            public void calRatio(int k,int k1,int k2){
                //百分数，且保留两位小数
                a[k][0]=Math.round(a[k2][0]/a[k1][0]*10000.0)/100.0;
                a[k][1]=Math.round(a[k2][1]/a[k1][1]*10000.0)/100.0;
                a[k][2]=Math.round((a[k][0]-a[k][1])*100.0)/100.0;
                a[k][3]=Math.round(a[k2][3]/a[k1][3]*10000.0)/100.0;
                a[k][4]=Math.round(a[k2][4]/a[k1][4]*10000.0)/100.0;
                a[k][5]=Math.round((a[k][3]-a[k][4])*100.0)/100.0;
            }
            public void calFund(){
                //营运资金=流动资产-流动负债
                int ym=y*100+m;
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                double sum=0,sumPre=0;
                for(int i=1;i<=12;i++) {
                    sum+=fasset.get(y*100+i)-fdebt.get(y*100+i);
                    sumPre+=fasset.get((y-1)*100+i)+fdebt.get((y-1)*100+i);
                }
                //金额数单位亿元，保留两位小数
                a[9][0]=Math.round((fasset.get(ym)-fdebt.get(ym))/1000000.0)/100.0;
                a[9][1]=Math.round((fasset.get(ymPreM)-fdebt.get(ymPreM))/1000000.0)/100.0;
                a[9][2]=Math.round((a[9][0]-a[9][1])*100.0)/100.0;
                a[9][3]=Math.round(sum/1000000.0)/100.0;
                a[9][4]=Math.round(sumPre/1000000.0)/100.0;
                a[9][5]=Math.round((a[9][3]-a[9][4])*100.0)/100.0;
            }
            public void ContextWrite(Context context,String name,int k) throws IOException,InterruptedException {
                String val=""+a[k][0];
                for(int i=1;i<6;i++) val+=" "+a[k][i];
                context.write(new Text(name),new Text(val));
            }
            public void cleanup(Context context) throws IOException,InterruptedException {
                calNum(yield,0);
                calNum(sale,1);
                calNum(stock,2);
                calMoney(asset,3);
                calMoney(debt,4);
                calMoney(interest,5);
                calMoney(income,6);
                calMoney(profit,7);
                calRatio(8,3,4);
                calRatio(10,6,7);
                calFund();
                context.write(new Text("项目"),new Text("本月 上月 增减 本年 上年 增减"));
                ContextWrite(context,"产量",0);
                ContextWrite(context,"销量",1);
                ContextWrite(context,"库存量",2);
                ContextWrite(context,"资产总额",3);
                ContextWrite(context,"负债合计",4);
                ContextWrite(context,"所有者权益",5);
                ContextWrite(context,"营业收入",6);
                ContextWrite(context,"利润总额",7);
                ContextWrite(context,"资产负债率",8);
                ContextWrite(context,"营运资金",9);
                ContextWrite(context,"利润率",10);
            }
        }
        private static void countComSale() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countComeSale";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(ComSaleCount.class); //指定作业类
            job.setMapperClass(ComSaleMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(ComSaleReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/com_sale"; //实验数据目录
            String outputDir="/carresult/overalleconomy/"+(y*100+m); //实验输出目录
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
        }
    }
    public static HashMap overallEconomyCount() throws Exception{
        jsonmap=new HashMap();
        a=new double[11][6];
        yield=new TreeMap<Integer,Double>();
        sale=new TreeMap<Integer,Double>();
        stock=new TreeMap<Integer,Double>();
        asset=new TreeMap<Integer,Double>();
        debt=new TreeMap<Integer,Double>();
        interest=new TreeMap<Integer,Double>();
        income=new TreeMap<Integer,Double>();
        profit=new TreeMap<Integer,Double>();
        fasset=new TreeMap<Integer,Double>();
        fdebt=new TreeMap<Integer,Double>();
        TotalCount.countTotal();
        ProductCount.countProduct();
        ComSaleCount.countComSale();
        jsonmap.put("dataset",a);
        return jsonmap;
    }
}
