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

public class IndexCostModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static TreeMap<Integer,Double> costTmp;
    private static TreeMap<Integer,Double> cost;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class IndexCostMapper extends Mapper<Object, Text, IntWritable,DoubleWritable> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            //总营业成本=主营业务成本+其他业务成本
            double money=Double.parseDouble(strs[9])+Double.parseDouble(strs[12]);
            context.write(new IntWritable(year*100+month),new DoubleWritable(money));
        }
    }
    private static class IndexCostReducer extends Reducer<IntWritable,DoubleWritable,Text, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException{
            for(DoubleWritable val:values) {
                double money=val.get();
                costTmp.put(key.get(),money+costTmp.getOrDefault(key.get(),0.0));
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            double costSum=0;
            for(int i=m+1;i<=12;i++){
                int ym=(y-1)*100+i;
                costSum+=costTmp.get(ym);
                //单位亿元，且保留两位小数
                double money=Math.round(costTmp.get(ym)/1000000.0)/100.0;
                cost.put(ym,money);
                context.write(new Text(ym+" 营业成本"),new DoubleWritable(money));
            }
            for(int i=1;i<=m;i++){
                int ym=y*100+i;
                costSum+=costTmp.get(ym);
                double money=Math.round(costTmp.get(ym)/1000000.0)/100.0;
                cost.put(ym,money);
                context.write(new Text(ym+" 营业成本"),new DoubleWritable(money));
            }
            jsonmap.put("cost",cost);
            double currentCost=Math.round(costSum/1000000.0)/100.0;
            jsonmap.put("currentCost",currentCost);
            context.write(new Text("本年营业总成本"),new DoubleWritable(currentCost));
            int yPreM=y,mPreM=m-1;//上个月的年月
            if(mPreM==0){
                yPreM--;
                mPreM=12;
            }
            int ym=y*100+m,ymPreM=yPreM*100+mPreM;
            double costRatioM=(costTmp.get(ym)-costTmp.get(ymPreM))/costTmp.get(ymPreM);
            costRatioM=Math.round(costRatioM*10000)/100.0;
            jsonmap.put("costRatioM",costRatioM);
            context.write(new Text("营业成本环比"),new DoubleWritable(costRatioM));
            int ymPreY=100*(y-1)+m;//上一年同月的年月
            double costRatioY=(costTmp.get(ym)-costTmp.get(ymPreY))/costTmp.get(ymPreY);
            costRatioY=Math.round(costRatioY*10000)/100.0;
            jsonmap.put("costRatioY",costRatioY);
            context.write(new Text("营业成本同比"),new DoubleWritable(costRatioY));
        }
    }
    public static HashMap indexCostCount() throws Exception {
        jsonmap=new HashMap();
        costTmp=new TreeMap<Integer,Double>();
        cost=new TreeMap<Integer,Double>();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countIndexCost";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(IndexCostModel.class); //指定作业类
        job.setMapperClass(IndexCostMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(DoubleWritable.class); //指定Map输出Value的数据类型
        job.setReducerClass(IndexCostReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/total"; //实验数据目录
        String outputDir="/carresult/indexcost/"+(y*100+m); //实验输出目录
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
