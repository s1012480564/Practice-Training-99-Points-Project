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

public class IndexAmountModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static TreeMap<Integer,Integer> saleTmp;
    private static TreeMap<Integer,Double> sale;
    private static TreeMap<Integer,Integer> yieldTmp;
    private static TreeMap<Integer,Double> yield;
    private static TreeMap<Integer,Integer> stockTmp;
    private static TreeMap<Integer,Double> stock;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class CountSale{
        private static class SaleMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                int num=Integer.parseInt(strs[3]);
                //年月用 year*100+month 后同
                context.write(new IntWritable(year*100+month),new IntWritable(num));
            }
        }
        private static class SaleReducer extends Reducer<IntWritable,IntWritable,Text,DoubleWritable>{
            @Override
            public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException{
                for(IntWritable val:values) {
                    saleTmp.put(key.get(),val.get()+saleTmp.getOrDefault(key.get(),0));
                }
            }
            @Override
            public void cleanup(Context context) throws IOException,InterruptedException{
                int saleSum=0;//当年总量，也就是当前月份后推12个月总量，后同
                for(int i=m+1;i<=12;i++){
                    int ym=(y-1)*100+i;
                    saleSum+=saleTmp.get(ym);
                    //车辆数单位万辆，且保留两位小数，计算方法后同
                    double num=Math.round(saleTmp.get(ym)/100.0)/100.0;
                    sale.put(ym,num);
                    context.write(new Text(ym+" 销量"),new DoubleWritable(num));
                }
                for(int i=1;i<=m;i++){
                    int ym=y*100+i;
                    saleSum+=saleTmp.get(ym);
                    double num=Math.round(saleTmp.get(ym)/100.0)/100.0;
                    sale.put(ym,num);
                    context.write(new Text(ym+" 销量"),new DoubleWritable(num));
                }
                jsonmap.put("sale",sale);
                double currentSale=Math.round(saleSum/100.0)/100.0;
                jsonmap.put("currentSale",currentSale);
                context.write(new Text("当年销量"),new DoubleWritable(currentSale));
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ym=y*100+m,ymPreM=yPreM*100+mPreM;
                double saleRatioM=1.0*(saleTmp.get(ym)-saleTmp.get(ymPreM))/saleTmp.get(ymPreM);
                //化成百分数，且保留两位小数，后同
                saleRatioM=Math.round(saleRatioM*10000)/100.0;
                jsonmap.put("saleRatioM",saleRatioM);
                context.write(new Text("销量环比"),new DoubleWritable(saleRatioM));
                int ymPreY=100*(y-1)+m;//上一年同月的年月
                double saleRatioY=1.0*(saleTmp.get(ym)-saleTmp.get(ymPreY))/saleTmp.get(ymPreY);
                saleRatioY=Math.round(saleRatioY*10000)/100.0;
                jsonmap.put("saleRatioY",saleRatioY);
                context.write(new Text("销量同比"),new DoubleWritable(saleRatioY));
            }
        }
        private static void countSale() throws Exception{
            sale=new TreeMap<Integer,Double>();
            saleTmp=new TreeMap<Integer,Integer>();
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countSale";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountSale.class); //指定作业类
            job.setMapperClass(SaleMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(IntWritable.class); //指定Map输出Value的数据类型
            job.setReducerClass(SaleReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/product"; //实验数据目录
            String outputDir="/carresult/indexamount/"+(y*100+m)+"/sale"; //实验输出目录
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
    private static class CountYieldStock{
        private static class YieldStockMapper extends Mapper<Object,Text,IntWritable,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                context.write(new IntWritable(year*100+month),new Text(strs[2]+" "+strs[3]));
            }
        }
        private static class YieldStockReducer extends Reducer<IntWritable,Text,Text,DoubleWritable>{
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException{
                for(Text val:values) {
                    String[] strs=val.toString().split(" ");
                    int yieldNum=Integer.parseInt(strs[0]),stockNum=Integer.parseInt(strs[1]);
                    yieldTmp.put(key.get(),yieldNum+yieldTmp.getOrDefault(key.get(),0));
                    stockTmp.put(key.get(),stockNum+stockTmp.getOrDefault(key.get(),0));
                }
            }
            @Override
            public void cleanup(Context context) throws IOException,InterruptedException{
                int yieldSum=0,stockSum=0;
                for(int i=m+1;i<=12;i++){
                    int ym=(y-1)*100+i;
                    yieldSum+=yieldTmp.get(ym);
                    stockSum+=stockTmp.get(ym);
                    double yieldNum=Math.round(yieldTmp.get(ym)/100.0)/100.0;
                    double stockNum=Math.round(stockTmp.get(ym)/100.0)/100.0;
                    yield.put(ym,yieldNum);
                    stock.put(ym,stockNum);
                    context.write(new Text(ym+" 产量"),new DoubleWritable(yieldNum));
                    context.write(new Text(ym+" 存量"),new DoubleWritable(stockNum));
                }
                for(int i=1;i<=m;i++){
                    int ym=y*100+i;
                    yieldSum+=yieldTmp.get(ym);
                    stockSum+=stockTmp.get(ym);
                    double yieldNum=Math.round(yieldTmp.get(ym)/100.0)/100.0;
                    double stockNum=Math.round(stockTmp.get(ym)/100.0)/100.0;
                    yield.put(ym,yieldNum);
                    stock.put(ym,stockNum);
                    context.write(new Text(ym+" 产量"),new DoubleWritable(yieldNum));
                    context.write(new Text(ym+" 存量"),new DoubleWritable(stockNum));
                }
                jsonmap.put("yield",yield);
                jsonmap.put("stock",stock);
                double currentYield=Math.round(yieldSum/100.0)/100.0;
                double currentStock=Math.round(stockSum/100.0)/100.0;
                jsonmap.put("currentYield",currentYield);
                jsonmap.put("currentStock",currentStock);
                context.write(new Text("当年产量"),new DoubleWritable(currentYield));
                context.write(new Text("当年存量"),new DoubleWritable(currentStock));
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ym=y*100+m,ymPreM=yPreM*100+mPreM;
                double yieldRatioM=1.0*(yieldTmp.get(ym)-yieldTmp.get(ymPreM))/yieldTmp.get(ymPreM);
                double stockRatioM=1.0*(stockTmp.get(ym)-stockTmp.get(ymPreM))/stockTmp.get(ymPreM);
                yieldRatioM=Math.round(yieldRatioM*10000)/100.0;
                stockRatioM=Math.round(stockRatioM*10000)/100.0;
                jsonmap.put("yieldRatioM",yieldRatioM);
                jsonmap.put("stockRatioM",stockRatioM);
                context.write(new Text("产量环比"),new DoubleWritable(yieldRatioM));
                context.write(new Text("存量环比"),new DoubleWritable(stockRatioM));
                int ymPreY=100*(y-1)+m;//上一年同月的年月
                double yieldRatioY=1.0*(yieldTmp.get(ym)-yieldTmp.get(ymPreY))/yieldTmp.get(ymPreY);
                double stockRatioY=1.0*(stockTmp.get(ym)-stockTmp.get(ymPreY))/stockTmp.get(ymPreY);
                yieldRatioY=Math.round(yieldRatioY*10000)/100.0;
                stockRatioY=Math.round(stockRatioY*10000)/100.0;
                jsonmap.put("yieldRatioY",yieldRatioY);
                jsonmap.put("stockRatioY",stockRatioY);
                context.write(new Text("产量同比"),new DoubleWritable(yieldRatioY));
                context.write(new Text("存量同比"),new DoubleWritable(stockRatioY));
            }
        }
        private static void countYieldStock() throws Exception{
            yield=new TreeMap<Integer,Double>();
            stock=new TreeMap<Integer,Double>();
            yieldTmp=new TreeMap<Integer,Integer>();
            stockTmp=new TreeMap<Integer,Integer>();
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countYieldStock";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountYieldStock.class); //指定作业类
            job.setMapperClass(YieldStockMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(YieldStockReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/total"; //实验数据目录
            String outputDir="/carresult/indexamount/"+(y*100+m)+"/yieldstock"; //实验输出目录
            Path inPath=new Path(hdfs+dataDir);
            Path outPath=new Path(hdfs+outputDir);
            FileInputFormat.addInputPath(job,inPath); //为作业添加输入路径
            FileOutputFormat.setOutputPath(job,outPath); //为作业添加输出路径
            FileSystem fs=FileSystem.get(conf);
            //如果输出目录已存在则删除
            if (fs.exists(outPath)) fs.delete(outPath, true);
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
    public static HashMap indexAmountCount() throws Exception{
        jsonmap=new HashMap();
        CountSale.countSale();
        CountYieldStock.countYieldStock();
        return jsonmap;
    }
}
