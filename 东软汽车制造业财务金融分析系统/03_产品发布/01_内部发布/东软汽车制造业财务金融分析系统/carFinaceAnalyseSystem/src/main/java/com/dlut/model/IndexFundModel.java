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

public class IndexFundModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static TreeMap<Integer,Double> fund;
    private static TreeMap<Integer,Double> fassetTmp;
    private static TreeMap<Integer,Double> fdebtTmp;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class IndexFundMapper extends Mapper<Object, Text, IntWritable,Text> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            context.write(new IntWritable(year*100+month),new Text(strs[6]+" "+strs[7]));
        }
    }
    private static class IndexFundReducer extends Reducer<IntWritable,Text,Text, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val:values) {
                String[] strs=val.toString().split(" ");
                double fassetMoney=Double.parseDouble(strs[0]),fdebtMoney=Double.parseDouble(strs[1]);
                fassetTmp.put(key.get(),fassetMoney+fassetTmp.getOrDefault(key.get(),0.0));
                fdebtTmp.put(key.get(),fdebtMoney+fdebtTmp.getOrDefault(key.get(),0.0));
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            double fundSum=0,fassetSum=0,fdebtSum=0;
            for(int i=m+1;i<=12;i++){
                int ym=(y-1)*100+i;
                fassetSum+=fassetTmp.get(ym);
                fdebtSum+=fdebtTmp.get(ym);
                //????????????=????????????-???????????????????????????
                double fundMoney=Math.round((fassetTmp.get(ym)-fdebtTmp.get(ym))/1000000.0)/100.0;
                fund.put(ym,fundMoney);
                context.write(new Text(ym+" ????????????"),new DoubleWritable(fundMoney));
            }
            for(int i=1;i<=m;i++){
                int ym=y*100+i;
                fassetSum+=fassetTmp.get(ym);
                fdebtSum+=fdebtTmp.get(ym);
                double fundMoney=Math.round((fassetTmp.get(ym)-fdebtTmp.get(ym))/1000000.0)/100.0;
                fund.put(ym,fundMoney);
                context.write(new Text(ym+" ????????????"),new DoubleWritable(fundMoney));
            }
            jsonmap.put("fund",fund);
            fundSum=fassetSum-fdebtSum;
            double currentFund=Math.round(fundSum/1000000.0)/100.0;
            double currentFasset=Math.round(fassetSum/1000000.0)/100.0;
            double currentFdebt=Math.round(fdebtSum/1000000.0)/100.0;
            jsonmap.put("currentFund",currentFund);
            jsonmap.put("currentFasset",currentFasset);
            jsonmap.put("currentFdebt",currentFdebt);
            context.write(new Text("??????????????????"),new DoubleWritable(currentFund));
            context.write(new Text("????????????"),new DoubleWritable(currentFasset));
            context.write(new Text("????????????"),new DoubleWritable(currentFdebt));
            int yPreM=y,mPreM=m-1;//??????????????????
            if(mPreM==0){
                yPreM--;
                mPreM=12;
            }
            int ym=y*100+m,ymPreM=yPreM*100+mPreM;
            double fundRatioM=(fund.get(ym)-fund.get(ymPreM))/fund.get(ymPreM);
            double fassetRatioM=(fassetTmp.get(ym)-fassetTmp.get(ymPreM))/fassetTmp.get(ymPreM);
            double fdebtRatioM=(fdebtTmp.get(ym)-fdebtTmp.get(ymPreM))/fdebtTmp.get(ymPreM);
            fundRatioM=Math.round(fundRatioM*10000)/100.0;
            fassetRatioM=Math.round(fassetRatioM*10000)/100.0;
            fdebtRatioM=Math.round(fdebtRatioM*10000)/100.0;
            jsonmap.put("fundRatioM",fundRatioM);
            jsonmap.put("fassetRatioM",fassetRatioM);
            jsonmap.put("fdebtRatioM",fdebtRatioM);
            context.write(new Text("??????????????????"),new DoubleWritable(fundRatioM));
            context.write(new Text("??????????????????"),new DoubleWritable(fassetRatioM));
            context.write(new Text("??????????????????"),new DoubleWritable(fdebtRatioM));
            int ymJan=100*y+1;//????????????????????????
            double fundRatioJan=(fund.get(ym)-fund.get(ymJan))/fund.get(ymJan);
            double fassetRatioJan=(fassetTmp.get(ym)-fassetTmp.get(ymJan))/fassetTmp.get(ymJan);
            double fdebtRatioJan=(fdebtTmp.get(ym)-fdebtTmp.get(ymJan))/fdebtTmp.get(ymJan);
            fundRatioJan=Math.round(fundRatioJan*10000)/100.0;
            fassetRatioJan=Math.round(fassetRatioJan*10000)/100.0;
            fdebtRatioJan=Math.round(fdebtRatioJan*10000)/100.0;
            jsonmap.put("fundRatioJan",fundRatioJan);
            jsonmap.put("fassetRatioJan",fassetRatioJan);
            jsonmap.put("fdebtRatioJan",fdebtRatioJan);
            context.write(new Text("?????????????????????"),new DoubleWritable(fundRatioJan));
            context.write(new Text("?????????????????????"),new DoubleWritable(fassetRatioJan));
            context.write(new Text("?????????????????????"),new DoubleWritable(fdebtRatioJan));
        }
    }
    public static HashMap indexFundCount() throws Exception{
        jsonmap=new HashMap();
        fund=new TreeMap<Integer,Double>();
        fassetTmp=new TreeMap<Integer,Double>();
        fdebtTmp=new TreeMap<Integer,Double>();
        //1.?????? HDFS???MapReduce ??? Yarn ????????????
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.??????MapReduce??????????????????
        String jobName="countIndexFund";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(IndexFundModel.class); //???????????????
        job.setMapperClass(IndexFundMapper.class); //??????Mapper???
        job.setMapOutputKeyClass(IntWritable.class); //??????Map??????Key???????????????
        job.setMapOutputValueClass(Text.class); //??????Map??????Value???????????????
        job.setReducerClass(IndexFundReducer.class); //??????Reducer???
        job.setOutputKeyClass(Text.class); //??????Reduce??????Key???????????????
        job.setOutputValueClass(DoubleWritable.class); //??????Reduce??????Value???????????????
        job.setNumReduceTasks(1); //???????????????????????????????????????Reduce????????????????????????????????????
        // 3.?????????????????????????????????
        String dataDir="/carinfo/total"; //??????????????????
        String outputDir="/carresult/indexfund/"+(y*100+m); //??????????????????
        Path inPath=new Path(hdfs+dataDir);
        Path outPath=new Path(hdfs+outputDir);
        FileInputFormat.addInputPath(job,inPath); //???????????????????????????
        FileOutputFormat.setOutputPath(job,outPath); //???????????????????????????
        FileSystem fs=FileSystem.get(conf);
        //????????????????????????????????????
        if (fs.exists(outPath)) fs.delete(outPath, true);
        // 4.????????????
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
