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
import java.util.*;

public class OverallStructureModel {
    private static int y;
    private static int m;
    private static boolean inc;
    private static int num;
    private static HashMap jsonmap;
    private static HashMap<String,Double> fee;
    private static TreeMap<Double,String> feeRev;
    private static ArrayList<Double> xdata;
    private static ArrayList<String> ydata;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    public static void setInc(boolean increase){inc=increase;}
    public static void setNum(int number){num=number;}
    private static class OverallStructureMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            String name=strs[2];
            double money=Double.parseDouble(strs[3]);
            context.write(new Text((year*100+month)+" "+name),new DoubleWritable(money));
        }
    }
    private static class OverallStructureReducer extends Reducer<Text,DoubleWritable,Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException{
            for(DoubleWritable val:values) {
                String[] strs=key.toString().split(" ");
                int ym=Integer.parseInt(strs[0]);
                String name=strs[1];
                int lowym=(m==12?y*100+1:(y-1)*100+m+1);
                int highym=y*100+m;
                if(ym>=lowym&&ym<=highym){
                    fee.put(name,val.get()+fee.getOrDefault(name,0.0));
                }
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            for(String name:fee.keySet()){
                feeRev.put(fee.get(name),name);
            }
            Set<Double> keys=inc?feeRev.keySet():feeRev.descendingKeySet();
            int cnt=0;
            for(double money:keys){
                cnt++;
                String name=feeRev.get(money);
                ydata.add(name);
                //单位万元，且保留两位小数
                money=Math.round(money/100.0)/100.0;
                xdata.add(money);
                String s="过去一年总量";
                s+=inc?"升序":"降序";
                s+="第"+cnt+"名： "+name;
                context.write(new Text(s),new DoubleWritable(money));
                if(cnt==num) break;
            }
            jsonmap.put("xdata",xdata);
            jsonmap.put("ydata",ydata);
        }
    }
    public static HashMap overallStructureCount() throws Exception{
        jsonmap=new HashMap();
        fee=new HashMap<String,Double>();
        feeRev=new TreeMap<Double,String>();
        xdata=new ArrayList<Double>();
        ydata=new ArrayList<String>();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countOverallStructure";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(OverallStructureModel.class); //指定作业类
        job.setMapperClass(OverallStructureMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(DoubleWritable.class); //指定Map输出Value的数据类型
        job.setReducerClass(OverallStructureReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/sale_cost"; //实验数据目录
        String outputDir="/carresult/overallstructure/"+(y*100+m); //实验输出目录
        outputDir+=inc?"/inc":"/dec";
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
        return jsonmap;
    }
}
