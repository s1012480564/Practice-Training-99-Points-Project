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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeMap;

public class ProfitabilityCompanyEnsembleModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static double[][] a;//按照月报报告内嵌表05的对应位置填写二维数组a
    private static HashMap<String,String> names;
    private static HashMap<String,HashMap<Integer,Double>> profit;
    private static HashMap<String,HashMap<Integer,Double>> asset;
    private static HashMap<String,HashMap<Integer,Double>> income;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class CompanyNames{
        private static class NamesMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                names.put(strs[0],strs[1]);
                profit.put(strs[1],new HashMap<Integer,Double>());
                asset.put(strs[1],new HashMap<Integer,Double>());
                income.put(strs[1],new HashMap<Integer,Double>());
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
            String outputDir="/carresult/profitabilitycompanyensemble/"+(y*100+m); //实验输出目录
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
    private static class ProfitabilityCompanyEnsembleMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            String name=names.get(strs[2]);
            String money=strs[3]+" "+strs[4]+" "+strs[5];
            context.write(new Text((year*100+month)+" "+name),new Text(money));
        }
    }
    private static class ProfitabilityCompanyEnsembleReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val:values) {
                String strs[]=key.toString().split(" ");
                int ym=Integer.parseInt(strs[0]);
                String name=strs[1];
                strs=val.toString().split(" ");
                double profitMoney=Double.parseDouble(strs[0]);
                double assetMoney=Double.parseDouble(strs[1]);
                double incomeMoney=Double.parseDouble(strs[2]);
                profit.get(name).put(ym,profitMoney+profit.get(name).getOrDefault(ym,0.0));
                asset.get(name).put(ym,assetMoney+asset.get(name).getOrDefault(ym,0.0));
                income.get(name).put(ym,incomeMoney+income.get(name).getOrDefault(ym,0.0));
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            String s1="公司名称 公司代码";
            String s2="本月(利润率) 上月 比上月 本年累计 上年累计 比上年"+
                      "本月(净资产收益率) 上月 比上月 本年累计 上年累计 比上年";
            context.write(new Text(s1),new Text(s2));
            int i=0;
            for(String code:names.keySet()){
                String name=names.get(code);
                int ym=y*100+m;
                //上个月的年月
                int yPreM=y,mPreM=m-1;
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                double profitMoney=profit.get(name).get(ym);
                double assetMoney=asset.get(name).get(ym);
                double incomeMoney=income.get(name).get(ym);
                double profitMoneyPre=profit.get(name).get(ymPreM);
                double assetMoneyPre=asset.get(name).get(ymPreM);
                double incomeMoneyPre=income.get(name).get(ymPreM);
                //本年累计
                double profitSum=0,assetSum=0,incomeSum=0;
                for(int k=1;k<=12;k++){
                    profitSum+=profit.get(name).get(y*100+k);
                    assetSum+=asset.get(name).get(y*100+k);
                    incomeSum+=income.get(name).get(y*100+k);
                }
                //上年累计
                double profitSumPre=0,assetSumPre=0,incomeSumPre=0;
                for(int k=1;k<=12;k++){
                    profitSumPre+=profit.get(name).get((y-1)*100+k);
                    assetSumPre+=asset.get(name).get((y-1)*100+k);
                    incomeSumPre+=income.get(name).get((y-1)*100+k);
                }
                //利润率=利润总额/营业收入
                double profitRatio=profitMoney/incomeMoney;
                double profitRatioPre=profitMoneyPre/incomeMoneyPre;
                double profitRatioInc=profitRatio-profitRatioPre;
                double profitSumRatio=profitSum/incomeSum;
                double profitSumRatioPre=profitSumPre/incomeSumPre;
                double profitSumRatioInc=profitSumRatio-profitSumRatioPre;
                //净资产收益率=利润总额/资产总额
                double assetRatio=profitMoney/assetMoney;
                double assetRatioPre=profitMoneyPre/assetMoneyPre;
                double assetRatioInc=assetRatio-assetRatioPre;
                double assetSumRatio=profitSum/assetSum;
                double assetSumRatioPre=profitSumPre/assetSumPre;
                double assetSumRatioInc=assetSumRatio-assetSumRatioPre;
                //百分数且保留两位小数
                profitRatio=calPercent(profitRatio);
                profitRatioPre=calPercent(profitRatioPre);
                profitRatioInc=calPercent(profitRatioInc);
                profitSumRatio=calPercent(profitSumRatio);
                profitSumRatioPre=calPercent(profitSumRatioPre);
                profitSumRatioInc=calPercent(profitSumRatioInc);
                assetRatio=calPercent(assetRatio);
                assetRatioPre=calPercent(assetRatioPre);
                assetRatioInc=calPercent(assetRatioInc);
                assetSumRatio=calPercent(assetSumRatio);
                assetSumRatioPre=calPercent(assetSumRatioPre);
                assetSumRatioInc=calPercent(assetSumRatioInc);
                a[i][0]=profitRatio;
                a[i][1]=profitRatioPre;
                a[i][2]=profitRatioInc;
                a[i][3]=profitSumRatio;
                a[i][4]=profitSumRatioPre;
                a[i][5]=profitSumRatioInc;
                a[i][6]=assetRatio;
                a[i][7]=assetRatioPre;
                a[i][8]=assetRatioInc;
                a[i][9]=assetSumRatio;
                a[i][10]=assetSumRatioPre;
                a[i][11]=assetSumRatioInc;
                s1=name+" "+code;
                s2=""+a[i][0];
                for(int j=1;j<12;j++) s2+=" "+a[i][j];
                context.write(new Text(s1),new Text(s2));
                i++;
            }
            jsonmap.put("cnt",i);
            jsonmap.put("names",names);
            jsonmap.put("dataset",a);
        }
        private static double calPercent(double x){
            return Math.round(x*10000.0)/100.0;
        }
    }
    public static HashMap profitabilityCompanyEnsembleCount() throws Exception{
        jsonmap=new HashMap();
        names=new HashMap<String,String>();
        profit=new HashMap<String,HashMap<Integer,Double>>();
        asset=new HashMap<String,HashMap<Integer,Double>>();
        income=new HashMap<String,HashMap<Integer,Double>>();
        a=new double[11][12];
        CompanyNames.getNamesByCode();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countProfitabilityCompanyEnsemble";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(ProfitabilityCompanyEnsembleModel.class); //指定作业类
        job.setMapperClass(ProfitabilityCompanyEnsembleMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
        job.setReducerClass(ProfitabilityCompanyEnsembleReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/com_sale"; //实验数据目录
        String outputDir="/carresult/prfitabilitycompanyensemble/"+(y*100+m); //实验输出目录
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