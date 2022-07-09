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
import java.net.DatagramPacket;
import java.util.HashMap;
import java.util.TreeMap;

public class ProfitabilityTotalModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static TreeMap<Integer,Double> profit;
    private static TreeMap<Integer,Double> income;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class ProfitabilityTotalMapper extends Mapper<Object, Text, IntWritable,Text> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            context.write(new IntWritable(year*100+month),new Text(strs[3]+" "+strs[5]));
        }
    }
    private static class ProfitabilityTotalReducer extends Reducer<IntWritable,Text,Text, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val:values) {
                String[] strs=val.toString().split(" ");
                double profitMoney=Double.parseDouble(strs[0]),incomeMoney=Double.parseDouble(strs[1]);
                profit.put(key.get(),profitMoney+profit.getOrDefault(key.get(),0.0));
                income.put(key.get(),incomeMoney+income.getOrDefault(key.get(),0.0));
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            //本年月
            int ym=y*100+m;
            //上个月的年月
            int yPreM=y,mPreM=m-1;
            if(mPreM==0){
                yPreM--;
                mPreM=12;
            }
            int ymPreM=yPreM*100+mPreM;
            //上年同月的年月
            int ymPreY=100*(y-1)+m;
            //单位亿元，且保留两位小数，计算方法后同
            double profitMoney=Math.round(profit.get(ym)/1000000.0)/100.0;
            double profitMoneyPreM=Math.round(profit.get(ymPreM)/1000000.0)/100.0;
            double profitMoneyPreY=Math.round(profit.get(ymPreY)/1000000.0)/100.0;
            double incomeMoney=Math.round(income.get(ym)/1000000.0)/100.0;
            double incomeMoneyPreM=Math.round(income.get(ymPreM)/1000000.0)/100.0;
            double incomeMoneyPreY=Math.round(income.get(ymPreY)/1000000.0)/100.0;
            //金额本年累计数和上年同期累计数
            double profitSum=0,profitSumPreY=0,incomeSum=0,incomeSumPreY=0;
            for(int i=m+1;i<=12;i++){
                int nowym=(y-1)*100+i,nowymPreY=(y-2)*100+i;
                profitSum+=profit.get(nowym);
                incomeSum+=income.get(nowym);
                profitSumPreY+=profit.get(nowymPreY);
                incomeSumPreY+=income.get(nowymPreY);
            }
            for(int i=1;i<=m;i++){
                int nowym=y*100+i,nowymPreY=(y-1)*100+i;
                profitSum+=profit.get(nowym);
                incomeSum+=income.get(nowym);
                profitSumPreY+=profit.get(nowymPreY);
                incomeSumPreY+=income.get(nowymPreY);
            }
            profitSum=Math.round(profitSum/1000000.0)/100.0;
            incomeSum=Math.round(incomeSum/1000000.0)/100.0;
            profitSumPreY=Math.round(profitSumPreY/1000000.0)/100.0;
            incomeSumPreY=Math.round(incomeSumPreY/1000000.0)/100.0;
            //利润增减量
            double profitIncM=profitMoney-profitMoneyPreM;
            double profitIncY=profitMoney-profitMoneyPreY;
            double profitSumIncY=profitSum-profitSumPreY;
            profitIncM=Math.round(profitIncM*100.0)/100.0;
            profitIncY=Math.round(profitIncY*100.0)/100.0;
            profitSumIncY=Math.round(profitSumIncY*100.0)/100.0;
            //累计同比增减率，百分数，且保留两位小数，后同
            double profitSumIncRatioY=profitSumIncY/profitSumPreY;
            profitSumIncRatioY=Math.round(profitSumIncRatioY*10000)/100.0;
            //利润率
            double ratio=profitMoney/incomeMoney;
            double ratioPreM=profitMoneyPreM/incomeMoneyPreM;
            double ratioPreY=profitMoneyPreY/incomeMoneyPreY;
            double ratioIncM=ratio-ratioPreM;
            double ratioIncY=ratio-ratioPreY;
            double sumRatio=profitSum/incomeSum;
            double sumRatioY=profitSumPreY/incomeSumPreY;
            double sumRatioIncY=sumRatio-sumRatioY;
            ratio=Math.round(ratio*10000)/100.0;
            ratioPreM=Math.round(ratioPreM*10000)/100.0;
            ratioPreY=Math.round(ratioPreY*10000)/100.0;
            ratioIncM=Math.round(ratioIncM*10000)/100.0;
            ratioIncY=Math.round(ratioIncY*10000)/100.0;
            sumRatio=Math.round(sumRatio*10000)/100.0;
            sumRatioY=Math.round(sumRatioY*10000)/100.0;
            sumRatioIncY=Math.round(sumRatioIncY*10000)/100.0;
            context.write(new Text("利润总额本月数"),new DoubleWritable(profitMoney));
            context.write(new Text("利润总额上月数"),new DoubleWritable(profitMoneyPreM));
            context.write(new Text("利润总额上年同期数"),new DoubleWritable(profitMoneyPreY));
            context.write(new Text("利润总额本月环比增减量"),new DoubleWritable(profitIncM));
            context.write(new Text("利润总额本月同比增减量"),new DoubleWritable(profitIncY));
            context.write(new Text("利润总额本年累计数"),new DoubleWritable(profitSum));
            context.write(new Text("利润总额上年同期累计数"),new DoubleWritable(profitSumPreY));
            context.write(new Text("利润总额累计同比增减量"),new DoubleWritable(profitSumIncY));
            context.write(new Text("利润总额累计同比增减率"),new DoubleWritable(profitSumIncRatioY));
            context.write(new Text("利润率本月数"),new DoubleWritable(ratio));
            context.write(new Text("利润率上月数"),new DoubleWritable(ratioPreM));
            context.write(new Text("利润率上年同期数"),new DoubleWritable(ratioPreY));
            context.write(new Text("利润率本月环比增减量"),new DoubleWritable(ratioIncM));
            context.write(new Text("利润率本月同比增减量"),new DoubleWritable(ratioIncY));
            context.write(new Text("利润率本年累计数"),new DoubleWritable(sumRatio));
            context.write(new Text("利润率上年同期数"),new DoubleWritable(sumRatioY));
            context.write(new Text("利润率累计同比增减量"),new DoubleWritable(sumRatioIncY));
            jsonmap.put("profitMoney",profitMoney);
            jsonmap.put("profitMoneyPreM",profitMoneyPreM);
            jsonmap.put("profitMoneyPreY",profitMoneyPreY);
            jsonmap.put("profitIncM",profitIncM);
            jsonmap.put("profitIncY",profitIncY);
            jsonmap.put("profitSum",profitSum);
            jsonmap.put("profitSumPreY",profitSumPreY);
            jsonmap.put("profitSumIncY",profitSumIncY);
            jsonmap.put("profitSumIncRatioY",profitSumIncRatioY);
            jsonmap.put("ratio",ratio);
            jsonmap.put("ratioPreM",ratioPreM);
            jsonmap.put("ratioPreY",ratioPreY);
            jsonmap.put("ratioIncM",ratioIncM);
            jsonmap.put("ratioIncY",ratioIncY);
            jsonmap.put("sumRatio",sumRatio);
            jsonmap.put("sumRatioY",sumRatioY);
            jsonmap.put("sumRatioIncY",sumRatioIncY);
        }
    }
    public static HashMap profitabilityTotalCount() throws Exception{
        jsonmap=new HashMap();
        profit=new TreeMap<Integer,Double>();
        income=new TreeMap<Integer,Double>();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countProfitabilityTotal";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(ProfitabilityTotalModel.class); //指定作业类
        job.setMapperClass(ProfitabilityTotalMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
        job.setReducerClass(ProfitabilityTotalReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/com_sale"; //实验数据目录
        String outputDir="/carresult/profitabilitytotal/"+(y*100+m); //实验输出目录
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
