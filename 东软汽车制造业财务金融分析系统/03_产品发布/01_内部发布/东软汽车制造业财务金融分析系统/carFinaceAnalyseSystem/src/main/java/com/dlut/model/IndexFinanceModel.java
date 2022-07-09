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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

public class IndexFinanceModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static TreeMap<Integer,Double> debtTmp;
    private static TreeMap<Integer,Double> profitTmp;
    private static TreeMap<Integer,Double> assetTmp;
    private static TreeMap<Integer,Double> incomeTmp;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class CountDebt{
        private static class DebtMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                double money=Double.parseDouble(strs[4]);
                context.write(new IntWritable(year*100+month),new DoubleWritable(money));
            }
        }
        private static class DebtReducer extends Reducer<IntWritable,DoubleWritable,Text, DoubleWritable> {
            @Override
            public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                    throws IOException, InterruptedException{
                for(DoubleWritable val:values) {
                    debtTmp.put(key.get(),val.get()+debtTmp.getOrDefault(key.get(),0.0));
                }
            }
        }
        private static void countDebt() throws Exception{
            debtTmp=new TreeMap<Integer,Double>();
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countDebt";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountDebt.class); //指定作业类
            job.setMapperClass(DebtMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(DoubleWritable.class); //指定Map输出Value的数据类型
            job.setReducerClass(DebtReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/total"; //实验数据目录
            String outputDir="/carresult/indexfinance/"+(y*100+m); //实验输出目录
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
    private static class CountProfitAssetIncome{
        private static class ProfitAssetIncomeMapper extends Mapper<Object,Text,IntWritable,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                context.write(new IntWritable(year*100+month),new Text(strs[3]+" "+strs[4]+" "+strs[5]));
            }
        }
        private static class ProfitAssetIncomeReducer extends Reducer<IntWritable,Text,Text,DoubleWritable>{
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException{
                for(Text val:values) {
                    String[] strs=val.toString().split(" ");
                    double profitMoney=Double.parseDouble(strs[0]);
                    double assetMoney=Double.parseDouble(strs[1]);
                    double incomeMoney=Double.parseDouble(strs[2]);
                    profitTmp.put(key.get(),profitMoney+profitTmp.getOrDefault(key.get(),0.0));
                    assetTmp.put(key.get(),assetMoney+assetTmp.getOrDefault(key.get(),0.0));
                    incomeTmp.put(key.get(),incomeMoney+incomeTmp.getOrDefault(key.get(),0.0));
                }
            }
            @Override
            public void cleanup(Context context) throws IOException,InterruptedException{
                int ym=y*100+m;
                //百分比数，保留两位小数
                //资产负债率=负债总额/资产总额
                //利润率=利润总额/营业收入
                //存货周转率=营业收入/资产总额
                double debtRatio=Math.round(debtTmp.get(ym)/assetTmp.get(ym)*10000)/100.0;
                double profitRatio=Math.round(profitTmp.get(ym)/incomeTmp.get(ym)*10000)/100.0;
                double turnoverRatio=Math.round(incomeTmp.get(ym)/assetTmp.get(ym)*10000)/100.0;
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                //上个月的各率
                double debtRatioPreM=Math.round(debtTmp.get(ymPreM)/assetTmp.get(ymPreM)*10000)/100.0;
                double profitRatioPreM=Math.round(profitTmp.get(ymPreM)/incomeTmp.get(ymPreM)*10000)/100.0;
                double turnoverRatioPreM=Math.round(incomeTmp.get(ymPreM)/assetTmp.get(ymPreM)*10000)/100.0;
                int ymJan=100*y+1;//年初即一月的年月
                //年初的各率
                double debtRatioPreJan=Math.round(debtTmp.get(ymJan)/assetTmp.get(ymJan)*10000)/100.0;
                double profitRatioPreJan=Math.round(profitTmp.get(ymJan)/incomeTmp.get(ymJan)*10000)/100.0;
                double turnoverRatioPreJan=Math.round(incomeTmp.get(ymJan)/assetTmp.get(ymJan)*10000)/100.0;
                //同比
                double debtRatioM=Math.round((debtRatio-debtRatioPreM)/debtRatioPreM*10000.0)/100.0;
                double profitRatioM=Math.round((profitRatio-profitRatioPreM)/profitRatioPreM*10000.0)/100.0;
                double turnoverRatioM=Math.round((turnoverRatio-turnoverRatioPreM)/turnoverRatioPreM*10000.0)/100.0;
                //环比
                double debtRatioJan=Math.round((debtRatio-debtRatioPreJan)/debtRatioPreJan*10000.0)/100.0;
                double profitRatioJan=Math.round((profitRatio-profitRatioPreJan)/profitRatioPreJan*10000.0)/100.0;
                double turnoverRatioJan=Math.round((turnoverRatio-turnoverRatioPreJan)/turnoverRatioPreJan*10000.0)/100.0;
                jsonmap.put("debtRatio",debtRatio);
                jsonmap.put("profitRatio",profitRatio);
                jsonmap.put("turnoverRatio",turnoverRatio);
                jsonmap.put("debtRatioM",debtRatioM);
                jsonmap.put("profitRatioM",profitRatioM);
                jsonmap.put("turnoverRatioM",turnoverRatioM);
                jsonmap.put("debtRatioJan",debtRatioJan);
                jsonmap.put("profitRatioJan",profitRatioJan);
                jsonmap.put("turnoverRatioJan",turnoverRatioJan);
                context.write(new Text("资产负债率"),new DoubleWritable(debtRatio));
                context.write(new Text("利润率"),new DoubleWritable(profitRatio));
                context.write(new Text("存货周转率"),new DoubleWritable(turnoverRatio));
                context.write(new Text("资产负债率同比"),new DoubleWritable(debtRatioM));
                context.write(new Text("利润率同比"),new DoubleWritable(profitRatioM));
                context.write(new Text("存货周转率同比"),new DoubleWritable(turnoverRatioM));
                context.write(new Text("资产负债率比年初"),new DoubleWritable(debtRatioJan));
                context.write(new Text("利润率比年初"),new DoubleWritable(profitRatioJan));
                context.write(new Text("存货周转率比年初"),new DoubleWritable(turnoverRatioJan));
            }
        }
        private static void countProfitAssetIncome() throws Exception{
            profitTmp=new TreeMap<Integer,Double>();
            assetTmp=new TreeMap<Integer,Double>();
            incomeTmp=new TreeMap<Integer,Double>();
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countProfitAssetIncome";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountProfitAssetIncome.class); //指定作业类
            job.setMapperClass(ProfitAssetIncomeMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(ProfitAssetIncomeReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/com_sale"; //实验数据目录
            String outputDir="/carresult/indexfinance/"+(y*100+m); //实验输出目录
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
    public static HashMap indexFinanceCount() throws Exception{
        jsonmap=new HashMap();
        CountDebt.countDebt();
        CountProfitAssetIncome.countProfitAssetIncome();
        return jsonmap;
    }
}
