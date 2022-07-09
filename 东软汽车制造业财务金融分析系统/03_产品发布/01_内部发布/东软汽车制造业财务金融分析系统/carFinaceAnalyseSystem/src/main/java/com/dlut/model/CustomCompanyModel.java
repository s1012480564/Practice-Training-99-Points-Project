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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Vector;

public class CustomCompanyModel {
    private static int y1,m1,y2,m2;
    private static String root="东软汽车销售总部";
    private static boolean inc;
    private static boolean bLayer;//false->son,true->leaf
    private static int num;
    private static HashMap jsonmap;
    private static ArrayList a,atmp;
    private static HashMap<String,String> names;//code->name
    private static HashMap<String, Vector<String>> tree;//relations，动态邻接表
    private static HashMap<String,Double> profit;
    private static TreeMap<Double,String> profitRev;
    private static ArrayList<String> xdataTmp,xdata;
    private static ArrayList<Double> ydata;
    public static void setYM(int year1,int month1,int year2,int month2){
        y1=year1;
        m1=month1;
        y2=year2;
        m2=month2;
    }
    public static void setInc(boolean increase){inc=increase;}
    public static void setbLayer(boolean sonorleaf){bLayer=sonorleaf;}
    public static void setNum(int number){num=number;}
    private static class CompanyNames{
        private static class NamesMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                names.put(strs[0],strs[1]);
                tree.put(strs[1],new Vector<String>());
            }
        }
        private static class NamesReducer extends Reducer<Text,Text,Text,Text> {
            @Override
            public void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException{
            }
        }
        public static void getNamesByCode() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="getNamesByCode";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CompanyNames.class); //指定作业类
            job.setMapperClass(NamesMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(NamesReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/company"; //实验数据目录
            String outputDir="/carresult/customcompany/"+(y1*100+m1)+"-"+(y2*100+m2); //实验输出目录
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
    private static class CompanyRelations{
        private static class RelationsMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                tree.get(names.get(strs[0])).add(names.get(strs[1]));
            }
        }
        private static class RelationsReducer extends Reducer<Text,Text,Text,Text> {
            @Override
            public void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException{
            }
        }
        public static void getRelations() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="getRelations";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CompanyRelations.class); //指定作业类
            job.setMapperClass(RelationsMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(RelationsReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/com_relation"; //实验数据目录
            String outputDir="/carresult/customcompany/"+(y1*100+m1)+"-"+(y2*100+m2); //实验输出目录
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
    private static class CustomCompanyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            String name=names.get(strs[2]);
            double money=Double.parseDouble(strs[3]);
            context.write(new Text((year*100+month)+" "+name),new DoubleWritable(money));
        }
    }
    private static class CustomCompanyReducer extends Reducer<Text,DoubleWritable,Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException{
            for(DoubleWritable val:values) {
                String strs[]=key.toString().split(" ");
                int nowym=Integer.parseInt(strs[0]);
                String name=strs[1];
                int ym1=y1*100+m1,ym2=y2*100+m2;
                if(nowym>=ym1&&nowym<=ym2){
                    profit.put(name,val.get()+profit.getOrDefault(name,0.0));
                }
            }
        }
        private static void dfs(String rt){
            if(tree.get(rt).size()==0){
                if(bLayer) xdataTmp.add(rt);
                return;
            }
            for(String son:tree.get(rt)){
                if(!bLayer&&rt.equals(root)) xdataTmp.add(son);
                dfs(son);
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            xdataTmp.add(root);
            dfs(root);
            for(String name:xdataTmp){
                profitRev.put(profit.get(name),name);
            }
            Set<Double> keys=inc?profitRev.keySet():profitRev.descendingKeySet();
            int cnt=0;
            for(double money:keys){
                cnt++;
                String name=profitRev.get(money);
                //单位亿元，且保留两位小数
                money=Math.round(money/1000000.0)/100.0;
                xdata.add(name);
                ydata.add(money);
                String s=inc?"升序":"降序";
                s+="第"+cnt+"名： "+name;
                context.write(new Text(s),new DoubleWritable(money));
                if(cnt==num) break;
            }
            atmp.add("name");
            atmp.add("利润总额");
            a.add(atmp.clone());
            atmp.clear();
            for(int i=0;i<xdata.size();i++){
                atmp.add(xdata.get(i));
                atmp.add(ydata.get(i));
                a.add(atmp.clone());
                atmp.clear();
            }
            jsonmap.put("dataset",a);
        }
    }
    public static HashMap customCompanyCount() throws Exception{
        jsonmap=new HashMap();
        profit=new HashMap<String,Double>();
        profitRev=new TreeMap<Double,String>();
        a=new ArrayList();
        atmp=new ArrayList();
        names=new HashMap<String,String>();
        tree=new HashMap<String,Vector<String>>();
        xdataTmp=new ArrayList<String>();
        xdata=new ArrayList<String>();
        ydata=new ArrayList<Double>();
        CompanyNames.getNamesByCode();
        CompanyRelations.getRelations();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countCustomCompany";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(CustomCompanyModel.class); //指定作业类
        job.setMapperClass(CustomCompanyMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(DoubleWritable.class); //指定Map输出Value的数据类型
        job.setReducerClass(CustomCompanyReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/com_sale"; //实验数据目录
        String outputDir="/carresult/customcompany/"+(y1*100+m1)+"-"+(y2*100+m2); //实验输出目录
        outputDir+=(inc?"/inc":"/dec");
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
