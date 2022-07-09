package com.dlut.model;

import jdk.nashorn.internal.objects.NativeRangeError;
import org.apache.hadoop.conf.ConfigRedactor;
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
import java.util.*;

public class OverallCompanyModel {
    private static int y;
    private static int m;
    private static boolean inc;
    private static boolean bSum;
    private static ArrayList<String> addLis;
    private static HashMap jsonmap;
    private static ArrayList a,atmp;
    private static HashMap<String,String> names;//code->name
    private static HashMap<String,Double> profit;
    private static HashMap<String,Double> profitPreM;
    private static HashMap<String,Double> profitPreY;
    private static TreeMap<Double,String> profitRev;
    private static ArrayList<String> xdata;
    private static ArrayList<Double> ydata,ydataPreM,ydataPreY;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    public static void setInc(boolean increase){inc=increase;}
    public static void setbSum(boolean sumornot){bSum=sumornot;}
    public static void setAddtion(String addition){
        addLis=new ArrayList<String>(Arrays.asList(addition.split(",")));
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
            String outputDir="/carresult/overallcompany/"+(y*100+m); //实验输出目录
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
    private static class OverallCompanyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
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
    private static class OverallCompanyReducer extends Reducer<Text,DoubleWritable,Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException{
            for(DoubleWritable val:values) {
                double money=val.get();
                String[] strs=key.toString().split(" ");
                int nowym=Integer.parseInt(strs[0]);
                String name=strs[1];
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
                if(!bSum){//要求本月数
                    if(nowym==ym) profit.put(name,money);
                    else if(nowym==ymPreM) profitPreM.put(name,money);
                    else if(nowym==ymPreY) profitPreY.put(name,money);
                }
                else{//要求全年累计数
                    int lowym=(m==12?y*100+1:(y-1)*100+m+1);//本月全年累计年月下限阈值
                    int lowymPreM=(mPreM==12?yPreM*100+1:(yPreM-1)*100+mPreM+1);//上月全年累计年月下限阈值
                    int lowymPreY=(m==12?(y-1)*100+1:(y-2)*100+m+1);//上月同月全年累计年月下限阈值
                    if(nowym>=lowym&&nowym<=ym)
                        profit.put(name,money+profit.getOrDefault(name,0.0));
                    if(nowym>=lowymPreM&&nowym<=ymPreM)
                        profitPreM.put(name,money+profitPreM.getOrDefault(name,0.0));
                    if(nowym>=lowymPreY&&nowym<=ymPreY)
                        profitPreY.put(name,money+profitPreY.getOrDefault(name,0.0));
                }
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            for(String name:profit.keySet()){
                profitRev.put(profit.get(name),name);
            }
            Set<Double> keys=inc?profitRev.keySet():profitRev.descendingKeySet();
            for(double money:keys){
                String name=profitRev.get(money);
                //单位万元，且保留两位小数
                money=Math.round(money/100.0)/100.0;
                double moneyPreM=profitPreM.get(name);
                moneyPreM=Math.round(moneyPreM/100.0)/100.0;
                double moneyPreY=profitPreY.get(name);
                moneyPreY=Math.round(moneyPreY/100.0)/100.0;
                xdata.add(name);
                ydata.add(money);
                ydataPreM.add(moneyPreM);
                ydataPreY.add(moneyPreY);
                String s=name+" ";
                s+=bSum?"全年累计数":"本月数";
                context.write(new Text(s),new DoubleWritable(money));
                s=name+" ";
                s+=bSum?"上月累计数":"上月数";
                context.write(new Text(s),new DoubleWritable(moneyPreM));
                s=name+" ";
                s+=bSum?"上年同期累计数":"上年同期数";
                context.write(new Text(s),new DoubleWritable(moneyPreY));
            }
            int cnt=1;
            if(addLis.contains("preM")) cnt++;
            if(addLis.contains("preY")) cnt++;
            jsonmap.put("cnt",cnt);
            atmp.add("name");
            atmp.add(bSum?"全年累计数":"本月数");
            if(addLis.contains("preM")) atmp.add(bSum?"上月累计数":"上月数");
            if(addLis.contains("preY")) atmp.add(bSum?"上年同期累计数":"上年同期数");
            a.add(atmp.clone());
            atmp.clear();
            for(int i=0;i<xdata.size();i++){
                atmp.add(xdata.get(i));
                atmp.add(ydata.get(i));
                if(addLis.contains("preM")) atmp.add(ydataPreM.get(i));
                if(addLis.contains("preY")) atmp.add(ydataPreY.get(i));
                a.add(atmp.clone());
                atmp.clear();
            }
            jsonmap.put("dataset",a);
        }
    }
    public static HashMap overallCompanyCount() throws Exception{
        jsonmap=new HashMap();
        a=new ArrayList();
        atmp=new ArrayList();
        names=new HashMap<String,String>();
        profit=new HashMap<String,Double>();
        profitPreM=new HashMap<String,Double>();
        profitPreY=new HashMap<String,Double>();
        profitRev=new TreeMap<Double,String>();
        xdata=new ArrayList<String>();
        ydata=new ArrayList<Double>();
        ydataPreM=new ArrayList<Double>();
        ydataPreY=new ArrayList<Double>();
        CompanyNames.getNamesByCode();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countOverallCompany";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(OverallCompanyModel.class); //指定作业类
        job.setMapperClass(OverallCompanyMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(DoubleWritable.class); //指定Map输出Value的数据类型
        job.setReducerClass(OverallCompanyReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/com_sale"; //实验数据目录
        String outputDir="/carresult/overallcompany/"+(y*100+m); //实验输出目录
        outputDir+=inc?"/inc":"/dec";
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
