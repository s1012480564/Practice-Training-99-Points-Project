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

public class IndexProfitModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static TreeMap<Integer,Double> profitTmp;
    private static TreeMap<Integer,Double> profit;
    private static TreeMap<Integer,Double> incomeTmp;
    private static TreeMap<Integer,Double> income;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class IndexProfitMapper extends Mapper<Object,Text,IntWritable,Text> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            context.write(new IntWritable(year*100+month),new Text(strs[3]+" "+strs[5]));
        }
    }
    private static class IndexProfitReducer extends Reducer<IntWritable,Text,Text,DoubleWritable>{
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val:values) {
                String[] strs=val.toString().split(" ");
                double profitMoney=Double.parseDouble(strs[0]),incomeMoney=Double.parseDouble(strs[1]);
                profitTmp.put(key.get(),profitMoney+profitTmp.getOrDefault(key.get(),0.0));
                incomeTmp.put(key.get(),incomeMoney+incomeTmp.getOrDefault(key.get(),0.0));
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            double profitSum=0,incomeSum=0;
            for(int i=m+1;i<=12;i++){
                int ym=(y-1)*100+i;
                profitSum+=profitTmp.get(ym);
                incomeSum+=incomeTmp.get(ym);
                //?????????????????????????????????????????????????????????
                double profitMoney=Math.round(profitTmp.get(ym)/1000000.0)/100.0;
                double incomeMoney=Math.round(incomeTmp.get(ym)/1000000.0)/100.0;
                profit.put(ym,profitMoney);
                income.put(ym,incomeMoney);
                context.write(new Text(ym+" ??????"),new DoubleWritable(profitMoney));
                context.write(new Text(ym+" ????????????"),new DoubleWritable(incomeMoney));
            }
            for(int i=1;i<=m;i++){
                int ym=y*100+i;
                profitSum+=profitTmp.get(ym);
                incomeSum+=incomeTmp.get(ym);
                double profitMoney=Math.round(profitTmp.get(ym)/1000000.0)/100.0;
                double incomeMoney=Math.round(incomeTmp.get(ym)/1000000.0)/100.0;
                profit.put(ym,profitMoney);
                income.put(ym,incomeMoney);
                context.write(new Text(ym+" ??????"),new DoubleWritable(profitMoney));
                context.write(new Text(ym+" ????????????"),new DoubleWritable(incomeMoney));
            }
            jsonmap.put("profit",profit);
            jsonmap.put("income",income);
            double currentProfit=Math.round(profitSum/1000000.0)/100.0;
            double currentIncome=Math.round(incomeSum/1000000.0)/100.0;
            jsonmap.put("currentProfit",currentProfit);
            jsonmap.put("currentIncome",currentIncome);
            context.write(new Text("??????"),new DoubleWritable(currentProfit));
            context.write(new Text("????????????"),new DoubleWritable(currentIncome));
            int yPreM=y,mPreM=m-1;//??????????????????
            if(mPreM==0){
                yPreM--;
                mPreM=12;
            }
            int ym=y*100+m,ymPreM=yPreM*100+mPreM;
            double profitRatioM=(profitTmp.get(ym)-profitTmp.get(ymPreM))/profitTmp.get(ymPreM);
            double incomeRatioM=(incomeTmp.get(ym)-incomeTmp.get(ymPreM))/incomeTmp.get(ymPreM);
            profitRatioM=Math.round(profitRatioM*10000)/100.0;
            incomeRatioM=Math.round(incomeRatioM*10000)/100.0;
            jsonmap.put("profitRatioM",profitRatioM);
            jsonmap.put("incomeRatioM",incomeRatioM);
            context.write(new Text("????????????"),new DoubleWritable(profitRatioM));
            context.write(new Text("??????????????????"),new DoubleWritable(incomeRatioM));
            int ymPreY=100*(y-1)+m;//????????????????????????
            double profitRatioY=1.0*(profitTmp.get(ym)-profitTmp.get(ymPreY))/profitTmp.get(ymPreY);
            double incomeRatioY=1.0*(incomeTmp.get(ym)-incomeTmp.get(ymPreY))/incomeTmp.get(ymPreY);
            profitRatioY=Math.round(profitRatioY*10000)/100.0;
            incomeRatioY=Math.round(incomeRatioY*10000)/100.0;
            jsonmap.put("profitRatioY",profitRatioY);
            jsonmap.put("incomeRatioY",incomeRatioY);
            context.write(new Text("????????????"),new DoubleWritable(profitRatioY));
            context.write(new Text("??????????????????"),new DoubleWritable(incomeRatioY));
        }
    }
    public static HashMap indexProfitCount() throws Exception{
        jsonmap=new HashMap();
        profit=new TreeMap<Integer,Double>();
        income=new TreeMap<Integer,Double>();
        profitTmp=new TreeMap<Integer,Double>();
        incomeTmp=new TreeMap<Integer,Double>();
        //1.?????? HDFS???MapReduce ??? Yarn ????????????
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.??????MapReduce??????????????????
        String jobName="countIndexProfit";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(IndexProfitModel.class); //???????????????
        job.setMapperClass(IndexProfitMapper.class); //??????Mapper???
        job.setMapOutputKeyClass(IntWritable.class); //??????Map??????Key???????????????
        job.setMapOutputValueClass(Text.class); //??????Map??????Value???????????????
        job.setReducerClass(IndexProfitReducer.class); //??????Reducer???
        job.setOutputKeyClass(Text.class); //??????Reduce??????Key???????????????
        job.setOutputValueClass(DoubleWritable.class); //??????Reduce??????Value???????????????
        job.setNumReduceTasks(1); //???????????????????????????????????????Reduce????????????????????????????????????
        // 3.?????????????????????????????????
        String dataDir="/carinfo/com_sale"; //??????????????????
        String outputDir="/carresult/indexprofit/"+(y*100+m); //??????????????????
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
