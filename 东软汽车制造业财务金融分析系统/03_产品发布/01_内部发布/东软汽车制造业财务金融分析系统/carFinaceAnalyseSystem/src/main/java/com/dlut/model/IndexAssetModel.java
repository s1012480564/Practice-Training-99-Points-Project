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

public class IndexAssetModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static TreeMap<Integer,Double> assetTmp;
    private static TreeMap<Integer,Double> asset;
    private static TreeMap<Integer,Double> debtTmp;
    private static TreeMap<Integer,Double> debt;
    private static TreeMap<Integer,Double> interestTmp;
    private static TreeMap<Integer,Double> interest;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class CountAsset{
        private static class AssetMapper extends Mapper<Object, Text, IntWritable,DoubleWritable> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                double money=Double.parseDouble(strs[4]);
                context.write(new IntWritable(year*100+month),new DoubleWritable(money));
            }
        }
        private static class AssetReducer extends Reducer<IntWritable,DoubleWritable,Text, DoubleWritable> {
            @Override
            public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                    throws IOException, InterruptedException{
                for(DoubleWritable val:values) {
                    assetTmp.put(key.get(),val.get()+assetTmp.getOrDefault(key.get(),0.0));
                }
            }
            @Override
            public void cleanup(Context context) throws IOException,InterruptedException{
                double assetSum=0;//当年总量，也就是当前月份后推12个月总量，后同
                for(int i=m+1;i<=12;i++){
                    int ym=(y-1)*100+i;
                    assetSum+=assetTmp.get(ym);
                    //单位亿元，且保留两位小数，计算方法后同
                    double assetMoney=Math.round(assetTmp.get(ym)/1000000.0)/100.0;
                    asset.put(ym,assetMoney);
                    context.write(new Text(ym+" 资产"),new DoubleWritable(assetMoney));
                }
                for(int i=1;i<=m;i++){
                    int ym=y*100+i;
                    assetSum+=assetTmp.get(ym);
                    double assetMoney=Math.round(assetTmp.get(ym)/1000000.0)/100.0;
                    asset.put(ym,assetMoney);
                    context.write(new Text(ym+" 资产"),new DoubleWritable(assetMoney));
                }
                jsonmap.put("asset",asset);
                double currentAsset=Math.round(assetSum/1000000.0)/100.0;
                jsonmap.put("currentAsset",currentAsset);
                context.write(new Text("当年资产"),new DoubleWritable(currentAsset));
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ym=y*100+m,ymPreM=yPreM*100+mPreM;
                double assetRatioM=(assetTmp.get(ym)-assetTmp.get(ymPreM))/assetTmp.get(ymPreM);
                //化成百分数，且保留两位小数，后同
                assetRatioM=Math.round(assetRatioM*10000)/100.0;
                jsonmap.put("assetRatioM",assetRatioM);
                context.write(new Text("资产环比"),new DoubleWritable(assetRatioM));
                int ymJan=100*y+1;//年初即一月的年月
                double assetRatioJan=(assetTmp.get(ym)-assetTmp.get(ymJan))/assetTmp.get(ymJan);
                assetRatioJan=Math.round(assetRatioJan*10000)/100.0;
                jsonmap.put("assetRatioJan",assetRatioJan);
                context.write(new Text("资产比年初"),new DoubleWritable(assetRatioJan));
            }
        }
        private static void countAsset() throws Exception{
            asset=new TreeMap<Integer,Double>();
            assetTmp=new TreeMap<Integer,Double>();
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countAsset";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountAsset.class); //指定作业类
            job.setMapperClass(AssetMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(DoubleWritable.class); //指定Map输出Value的数据类型
            job.setReducerClass(AssetReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/com_sale"; //实验数据目录
            String outputDir="/carresult/indexasset/"+(y*100+m)+"/asset"; //实验输出目录
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
    private static class CountDebtInterest{
        private static class DebtInterestMapper extends Mapper<Object,Text,IntWritable,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                context.write(new IntWritable(year*100+month),new Text(strs[4]+" "+strs[5]));
            }
        }
        private static class DebtInterestReducer extends Reducer<IntWritable,Text,Text,DoubleWritable>{
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException{
                for(Text val:values) {
                    String[] strs=val.toString().split(" ");
                    double debtMoney=Double.parseDouble(strs[0]),interestMoney=Double.parseDouble(strs[1]);
                    debtTmp.put(key.get(),debtMoney+debtTmp.getOrDefault(key.get(),0.0));
                    interestTmp.put(key.get(),interestMoney+interestTmp.getOrDefault(key.get(),0.0));
                }
            }
            @Override
            public void cleanup(Context context) throws IOException,InterruptedException{
                double debtSum=0,interestSum=0;
                for(int i=m+1;i<=12;i++){
                    int ym=(y-1)*100+i;
                    debtSum+=debtTmp.get(ym);
                    interestSum+=interestTmp.get(ym);
                    double debtMoney=Math.round(debtTmp.get(ym)/1000000.0)/100.0;
                    double interestMoney=Math.round(interestTmp.get(ym)/1000000.0)/100.0;
                    debt.put(ym,debtMoney);
                    interest.put(ym,interestMoney);
                    context.write(new Text(ym+" 负债"),new DoubleWritable(debtMoney));
                    context.write(new Text(ym+" 所有者权益"),new DoubleWritable(interestMoney));
                }
                for(int i=1;i<=m;i++){
                    int ym=y*100+i;
                    debtSum+=debtTmp.get(ym);
                    interestSum+=interestTmp.get(ym);
                    double debtMoney=Math.round(debtTmp.get(ym)/1000000.0)/100.0;
                    double interestMoney=Math.round(interestTmp.get(ym)/1000000.0)/100.0;
                    debt.put(ym,debtMoney);
                    interest.put(ym,interestMoney);
                    context.write(new Text(ym+" 负债"),new DoubleWritable(debtMoney));
                    context.write(new Text(ym+" 所有者权益"),new DoubleWritable(interestMoney));
                }
                jsonmap.put("debt",debt);
                jsonmap.put("interest",interest);
                double currentDebt=Math.round(debtSum/1000000.0)/100.0;
                double currentInterest=Math.round(interestSum/1000000.0)/100.0;
                jsonmap.put("currentDebt",currentDebt);
                jsonmap.put("currentInterest",currentInterest);
                context.write(new Text("当年负债"),new DoubleWritable(currentDebt));
                context.write(new Text("当年所有者权益"),new DoubleWritable(currentInterest));
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ym=y*100+m,ymPreM=yPreM*100+mPreM;
                double debtRatioM=(debtTmp.get(ym)-debtTmp.get(ymPreM))/debtTmp.get(ymPreM);
                double interestRatioM=(interestTmp.get(ym)-interestTmp.get(ymPreM))/interestTmp.get(ymPreM);
                debtRatioM=Math.round(debtRatioM*10000)/100.0;
                interestRatioM=Math.round(interestRatioM*10000)/100.0;
                jsonmap.put("debtRatioM",debtRatioM);
                jsonmap.put("interestRatioM",interestRatioM);
                context.write(new Text("负债环比"),new DoubleWritable(debtRatioM));
                context.write(new Text("所有者权益环比"),new DoubleWritable(interestRatioM));
                int ymJan=100*y+1;//年初即一月的年月
                double debtRatioJan=(debtTmp.get(ym)-debtTmp.get(ymJan))/debtTmp.get(ymJan);
                double interestRatioJan=(interestTmp.get(ym)-interestTmp.get(ymJan))/interestTmp.get(ymJan);
                debtRatioJan=Math.round(debtRatioJan*10000)/100.0;
                interestRatioJan=Math.round(interestRatioJan*10000)/100.0;
                jsonmap.put("debtRatioJan",debtRatioJan);
                jsonmap.put("interestRatioJan",interestRatioJan);
                context.write(new Text("负债比年初"),new DoubleWritable(debtRatioJan));
                context.write(new Text("所有者权益比年初"),new DoubleWritable(interestRatioJan));
            }
        }
        private static void countDebtInterest() throws Exception{
            debt=new TreeMap<Integer,Double>();
            interest=new TreeMap<Integer,Double>();
            debtTmp=new TreeMap<Integer,Double>();
            interestTmp=new TreeMap<Integer,Double>();
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countDebtInterest";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountDebtInterest.class); //指定作业类
            job.setMapperClass(DebtInterestMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(DebtInterestReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/total"; //实验数据目录
            String outputDir="/carresult/indexamount/"+(y*100+m)+"/debtinterest"; //实验输出目录
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
    public static HashMap indexAssetCount() throws Exception{
        jsonmap=new HashMap();
        CountAsset.countAsset();
        CountDebtInterest.countDebtInterest();
        return jsonmap;
    }
}
