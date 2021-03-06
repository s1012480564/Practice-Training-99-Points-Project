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

public class IndexAssetComposeModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static TreeMap<Integer,Double> assetTmp;
    private static TreeMap<Integer,Double> assetRatio;
    private static TreeMap<Integer,Double> debtTmp;
    private static TreeMap<Integer,Double> debtRatio;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class CountAsset{
        private static class AssetMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
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
                double assetSum=0;
                for(int i=m+1;i<=12;i++){
                    int ym=(y-1)*100+i;
                    assetSum+=assetTmp.get(ym);
                }
                for(int i=1;i<=m;i++){
                    int ym=y*100+i;
                    assetSum+=assetTmp.get(ym);
                }
                //???????????????????????????????????????????????????
                for(int i=m+1;i<=12;i++){
                    int ym=(y-1)*100+i;
                    double ratio=Math.round(assetTmp.get(ym)/assetSum*10000.0)/100.0;
                    assetRatio.put(ym,ratio);
                    context.write(new Text(ym+"????????????"),new DoubleWritable(ratio));
                }
                for(int i=1;i<=m;i++){
                    int ym=y*100+i;
                    double ratio=Math.round(assetTmp.get(ym)/assetSum*10000.0)/100.0;
                    assetRatio.put(ym,ratio);
                    context.write(new Text(ym+"????????????"),new DoubleWritable(ratio));
                }
                jsonmap.put("assetRatio",assetRatio);
                //?????????????????????????????????
                double currentAsset=Math.round(assetSum/1000000.0)/100.0;
                jsonmap.put("currentAsset",currentAsset);
                context.write(new Text("????????????"),new DoubleWritable(currentAsset));
                int yPreM=y,mPreM=m-1;//??????????????????
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ym=y*100+m,ymPreM=yPreM*100+mPreM;
                double assetRatioM=(assetTmp.get(ym)-assetTmp.get(ymPreM))/assetTmp.get(ymPreM);
                //????????????????????????????????????????????????
                assetRatioM=Math.round(assetRatioM*10000)/100.0;
                jsonmap.put("assetRatioM",assetRatioM);
                context.write(new Text("????????????"),new DoubleWritable(assetRatioM));
                int ymJan=100*y+1;//????????????????????????
                double assetRatioJan=(assetTmp.get(ym)-assetTmp.get(ymJan))/assetTmp.get(ymJan);
                assetRatioJan=Math.round(assetRatioJan*10000)/100.0;
                jsonmap.put("assetRatioJan",assetRatioJan);
                context.write(new Text("???????????????"),new DoubleWritable(assetRatioJan));
            }
        }
        private static void countAsset() throws Exception{
            assetTmp=new TreeMap<Integer,Double>();
            assetRatio=new TreeMap<Integer,Double>();
            //1.?????? HDFS???MapReduce ??? Yarn ????????????
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.??????MapReduce??????????????????
            String jobName="countAsset";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountAsset.class); //???????????????
            job.setMapperClass(AssetMapper.class); //??????Mapper???
            job.setMapOutputKeyClass(IntWritable.class); //??????Map??????Key???????????????
            job.setMapOutputValueClass(DoubleWritable.class); //??????Map??????Value???????????????
            job.setReducerClass(AssetReducer.class); //??????Reducer???
            job.setOutputKeyClass(Text.class); //??????Reduce??????Key???????????????
            job.setOutputValueClass(DoubleWritable.class); //??????Reduce??????Value???????????????
            job.setNumReduceTasks(1); //???????????????????????????????????????Reduce????????????????????????????????????
            // 3.?????????????????????????????????
            String dataDir="/carinfo/com_sale"; //??????????????????
            String outputDir="/carresult/indexassetcompose/"+(y*100+m)+"/asset"; //??????????????????
            Path inPath=new Path(hdfs+dataDir);
            Path outPath=new Path(hdfs+outputDir);
            FileInputFormat.addInputPath(job,inPath); //???????????????????????????
            FileOutputFormat.setOutputPath(job,outPath); //???????????????????????????
            FileSystem fs=FileSystem.get(conf);
            if (fs.exists(outPath)){ //????????????????????????????????????
                fs.delete(outPath,true);
            }
            // 4.????????????
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
            @Override
            public void cleanup(Context context) throws IOException,InterruptedException{
                double debtSum=0;
                for(int i=m+1;i<=12;i++){
                    int ym=(y-1)*100+i;
                    debtSum+=debtTmp.get(ym);
                }
                for(int i=1;i<=m;i++){
                    int ym=y*100+i;
                    debtSum+=debtTmp.get(ym);
                }
                for(int i=m+1;i<=12;i++){
                    int ym=(y-1)*100+i;
                    double ratio=Math.round(debtTmp.get(ym)/debtSum*10000.0)/100.0;
                    debtRatio.put(ym,ratio);
                    context.write(new Text(ym+"????????????"),new DoubleWritable(ratio));
                }
                for(int i=1;i<=m;i++){
                    int ym=y*100+i;
                    double ratio=Math.round(debtTmp.get(ym)/debtSum*10000.0)/100.0;
                    debtRatio.put(ym,ratio);
                    context.write(new Text(ym+"????????????"),new DoubleWritable(ratio));
                }
                jsonmap.put("debtRatio",debtRatio);
                double currentDebt=Math.round(debtSum/1000000.0)/100.0;
                jsonmap.put("currentDebt",currentDebt);
                context.write(new Text("????????????"),new DoubleWritable(currentDebt));
                int yPreM=y,mPreM=m-1;//??????????????????
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ym=y*100+m,ymPreM=yPreM*100+mPreM;
                double debtRatioM=(debtTmp.get(ym)-debtTmp.get(ymPreM))/debtTmp.get(ymPreM);
                //????????????????????????????????????????????????
                debtRatioM=Math.round(debtRatioM*10000)/100.0;
                jsonmap.put("debtRatioM",debtRatioM);
                context.write(new Text("????????????"),new DoubleWritable(debtRatioM));
                int ymJan=100*y+1;//????????????????????????
                double debtRatioJan=(debtTmp.get(ym)-debtTmp.get(ymJan))/debtTmp.get(ymJan);
                debtRatioJan=Math.round(debtRatioJan*10000)/100.0;
                jsonmap.put("debtRatioJan",debtRatioJan);
                context.write(new Text("???????????????"),new DoubleWritable(debtRatioJan));
            }
        }
        private static void countDebt() throws Exception{
            debtTmp=new TreeMap<Integer,Double>();
            debtRatio=new TreeMap<Integer,Double>();
            //1.?????? HDFS???MapReduce ??? Yarn ????????????
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.??????MapReduce??????????????????
            String jobName="countDebt";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountDebt.class); //???????????????
            job.setMapperClass(DebtMapper.class); //??????Mapper???
            job.setMapOutputKeyClass(IntWritable.class); //??????Map??????Key???????????????
            job.setMapOutputValueClass(DoubleWritable.class); //??????Map??????Value???????????????
            job.setReducerClass(DebtReducer.class); //??????Reducer???
            job.setOutputKeyClass(Text.class); //??????Reduce??????Key???????????????
            job.setOutputValueClass(DoubleWritable.class); //??????Reduce??????Value???????????????
            job.setNumReduceTasks(1); //???????????????????????????????????????Reduce????????????????????????????????????
            // 3.?????????????????????????????????
            String dataDir="/carinfo/total"; //??????????????????
            String outputDir="/carresult/indexassetcompose/"+(y*100+m)+"/debt"; //??????????????????
            Path inPath=new Path(hdfs+dataDir);
            Path outPath=new Path(hdfs+outputDir);
            FileInputFormat.addInputPath(job,inPath); //???????????????????????????
            FileOutputFormat.setOutputPath(job,outPath); //???????????????????????????
            FileSystem fs=FileSystem.get(conf);
            if (fs.exists(outPath)){ //????????????????????????????????????
                fs.delete(outPath,true);
            }
            // 4.????????????
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
    public static HashMap indexAssetComposeCount() throws Exception {
        jsonmap = new HashMap();
        CountAsset.countAsset();
        CountDebt.countDebt();
        return jsonmap;
    }
}
