package com.dlut.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

public class CustomMultiTargetModel {
    private static int y1,m1,y2,m2;
    private static ArrayList<String> target,targetCmp;
    private static HashMap jsonmap;
    private static ArrayList a1,a2,atmp1,atmp2,atmp;
    private static ArrayList<String> xnames;
    private static HashMap<String,String> names;//code->name
    private static HashMap<String,Double> profit,asset,income;
    public static void setYM(int year1,int month1,int year2,int month2){
        y1=year1;
        m1=month1;
        y2=year2;
        m2=month2;
    }
    public static void setTarget(String sTarget){
        target=new ArrayList<String>(Arrays.asList(sTarget.split(",")));
    }
    public static void setTargetCmp(String sTargetCmp){
        targetCmp=new ArrayList<String>(Arrays.asList(sTargetCmp.split(",")));
    }
    private static class CompanyNames{
        private static class NamesMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                names.put(strs[0],strs[1]);
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
            String outputDir="/carresult/custommultitarget/"+(y1*100+m1)+"-"+(y2*100+m2); //实验输出目录
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
    private static class CustomMultiTargetMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            String name=names.get(strs[2]);
            context.write(new Text((year*100+month)+" "+name),new Text(strs[3]+" "+strs[4]+" "+strs[5]));
        }
    }
    private static class CustomMultiTargetReducer extends Reducer<Text,Text,Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val:values) {
                String strs[]=key.toString().split(" ");
                int nowym=Integer.parseInt(strs[0]);
                String name=strs[1];
                strs=val.toString().split(" ");
                double profitMoney=Double.parseDouble(strs[0]);
                double assetMoney=Double.parseDouble(strs[1]);
                double incomeMoney=Double.parseDouble(strs[2]);
                int ym1=y1*100+m1,ym2=y2*100+m2;
                if(nowym>=ym1&&nowym<=ym2){
                    profit.put(name,profitMoney+profit.getOrDefault(name,0.0));
                    asset.put(name,assetMoney+profit.getOrDefault(name,0.0));
                    income.put(name,incomeMoney+profit.getOrDefault(name,0.0));
                }
            }
        }
        void targetAdd(ArrayList a,String name,double num){
            atmp.add(name);
            atmp.add(num);
            a.add(atmp.clone());
            atmp.clear();
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            for(String name:names.values()){
                xnames.add(name);
                double profitMoney=profit.get(name);
                double assetMoney=asset.get(name);
                double incomeMoney=income.get(name);
                //利润率=利润总额/营业收入
                //存货周转率=营业收入/资产总额
                //净资产收益率=利润总额/资产总额
                double ratioPI=profitMoney/incomeMoney;
                double ratioIA=incomeMoney/assetMoney;
                double ratioPA=profitMoney/assetMoney;
                //money单位亿元，且保留两位小数
                profitMoney=Math.round(profitMoney/1000000.0)/100.0;
                assetMoney=Math.round(assetMoney/1000000.0)/100.0;
                incomeMoney=Math.round(incomeMoney/1000000.0)/100.0;
                //ratio百分数，且保留两位小数
                ratioPI=Math.round(ratioPI*10000.0)/100.0;
                ratioIA=Math.round(ratioIA*10000.0)/100.0;
                ratioPA=Math.round(ratioPA*10000.0)/100.0;
                context.write(new Text(name+" 利润总额"),new DoubleWritable(profitMoney));
                context.write(new Text(name+" 资产总额"),new DoubleWritable(assetMoney));
                context.write(new Text(name+" 营业总收入"),new DoubleWritable(incomeMoney));
                context.write(new Text(name+" 利润率"),new DoubleWritable(ratioPI));
                context.write(new Text(name+" 存货周转率"),new DoubleWritable(ratioIA));
                context.write(new Text(name+" 净资产收益率"),new DoubleWritable(ratioPA));
                if(target.contains("profit")) targetAdd(atmp1,"利润总额",profitMoney);
                if(target.contains("asset")) targetAdd(atmp1,"资产总额",assetMoney);
                if(target.contains("income")) targetAdd(atmp1,"营业总收入",incomeMoney);
                if(target.contains("ratioPI")) targetAdd(atmp1,"利润率",ratioPI);
                if(target.contains("ratioIA")) targetAdd(atmp1,"存货周转率",ratioIA);
                if(target.contains("ratioPA")) targetAdd(atmp1,"净资产收益率",ratioPA);
                a1.add(atmp1.clone());
                atmp1.clear();
                if(targetCmp.contains("profit")) targetAdd(atmp2,"利润总额",profitMoney);
                if(targetCmp.contains("asset")) targetAdd(atmp2,"资产总额",assetMoney);
                if(targetCmp.contains("income")) targetAdd(atmp2,"营业总收入",incomeMoney);
                if(targetCmp.contains("ratioPI")) targetAdd(atmp2,"利润率",ratioPI);
                if(targetCmp.contains("ratioIA")) targetAdd(atmp2,"存货周转率",ratioIA);
                if(targetCmp.contains("ratioPA")) targetAdd(atmp2,"净资产收益率",ratioPA);
                a2.add(atmp2.clone());
                atmp2.clear();
            }
            jsonmap.put("names",xnames);
            jsonmap.put("dataset1",a1);
            jsonmap.put("dataset2",a2);
        }
    }
    public static HashMap customMultiTargetCount() throws Exception{
        jsonmap=new HashMap();
        a1=new ArrayList();
        a2=new ArrayList();
        atmp1=new ArrayList();
        atmp2=new ArrayList();
        atmp=new ArrayList();
        xnames=new ArrayList<String>();
        names=new HashMap<String,String>();
        profit=new HashMap<String,Double>();
        asset=new HashMap<String,Double>();
        income=new HashMap<String,Double>();
        CompanyNames.getNamesByCode();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countCustomMultiTarget";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(CustomMultiTargetModel.class); //指定作业类
        job.setMapperClass(CustomMultiTargetMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
        job.setReducerClass(CustomMultiTargetReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/com_sale"; //实验数据目录
        String outputDir="/carresult/custommultitarget/"+(y1*100+m1)+"-"+(y2*100+m2); //实验输出目录
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
