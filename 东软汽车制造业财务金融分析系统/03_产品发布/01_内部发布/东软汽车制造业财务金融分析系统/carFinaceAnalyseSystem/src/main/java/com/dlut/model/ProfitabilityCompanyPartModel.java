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
import java.util.*;

public class ProfitabilityCompanyPartModel {
    private static int y;
    private static int m;
    private static String root="东软汽车销售总部";
    private static boolean inc;
    private static boolean bSum;
    private static boolean bLayer;//false->son,true->leaf
    private static ArrayList<String> addLis;
    private static int num;
    private static HashMap jsonmap;
    private static ArrayList a,atmp;
    private static HashMap<String,String> names;//code->name
    private static HashMap<String,Vector<String>> tree;//relations，动态邻接表
    private static HashMap<String,Double> profit,profitPreM,profitPreY;
    private static HashMap<String,Double> income,incomePreM,incomePreY;
    private static TreeMap<Double,String> ratioRev;
    private static ArrayList<String> xdataTmp;
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
    public static void setbLayer(boolean sonorleaf){bLayer=sonorleaf;}
    public static void setAddtion(String addition){
        addLis=new ArrayList<String>(Arrays.asList(addition.split(",")));
    }
    public static void setNum(int number){num=number;}
    private static class CompanyNames{
        private static class NamesMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                names.put(strs[0],strs[1]);
                tree.put(strs[1],new Vector<String>());
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
            job.setMapperClass(CompanyNames.NamesMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(CompanyNames.NamesReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/company"; //实验数据目录
            String outputDir="/carresult/profitabilitycompanypart/"+(y*100+m); //实验输出目录
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
    private static class CompanyRelations{
        private static class RelationsMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                tree.get(names.get(strs[0])).add(names.get(strs[1]));
            }
        }
        private static class RelationsReducer extends Reducer<Text,Text,Text,Text> {
            @Override
            public void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException{
            }
        }
        public static void getRelations() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="getRelations";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CompanyRelations.class); //指定作业类
            job.setMapperClass(RelationsMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(RelationsReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/com_relation"; //实验数据目录
            String outputDir="/carresult/profitabilitycompanypart/"+(y*100+m); //实验输出目录
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
    private static class ProfitabilityCompanyPartMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            String name=names.get(strs[2]);
            context.write(new Text((year*100+month)+" "+name),new Text(strs[3]+" "+strs[5]));
        }
    }
    private static class ProfitabilityCompanyPartReducer extends Reducer<Text,Text,Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val:values) {
                String strs[]=val.toString().split(" ");
                double profitMoney=Double.parseDouble(strs[0]);
                double incomeMoney=Double.parseDouble(strs[1]);
                strs=key.toString().split(" ");
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
                    if(nowym==ym){
                        profit.put(name,profitMoney);
                        income.put(name,incomeMoney);
                    }
                    else if(nowym==ymPreM){
                        profitPreM.put(name,profitMoney);
                        incomePreM.put(name,incomeMoney);
                    }
                    else if(nowym==ymPreY){
                        profitPreY.put(name,profitMoney);
                        incomePreY.put(name,incomeMoney);
                    }
                }
                else{//要求全年累计数
                    int lowym=(m==12?y*100+1:(y-1)*100+m+1);//本月全年累计年月下限阈值
                    int lowymPreM=(mPreM==12?yPreM*100+1:(yPreM-1)*100+mPreM+1);//上月全年累计年月下限阈值
                    int lowymPreY=(m==12?(y-1)*100+1:(y-2)*100+m+1);//上月同月全年累计年月下限阈值
                    if(nowym>=lowym&&nowym<=ym){
                        profit.put(name,profitMoney+profit.getOrDefault(name, 0.0));
                        income.put(name,incomeMoney+income.getOrDefault(name, 0.0));
                    }
                    if(nowym>=lowymPreM&&nowym<=ymPreM){
                        profitPreM.put(name,profitMoney+profitPreM.getOrDefault(name,0.0));
                        incomePreM.put(name,incomeMoney+incomePreM.getOrDefault(name,0.0));
                    }
                    if(nowym>=lowymPreY&&nowym<=ymPreY){
                        profitPreY.put(name,profitMoney+profitPreY.getOrDefault(name,0.0));
                        incomePreY.put(name,incomeMoney+incomePreY.getOrDefault(name,0.0));
                    }
                }
            }
        }
        private static void dfs(String rt){
            if(tree.get(rt).size()==0){
                if(bLayer) xdataTmp.add(rt);
                return;
            }
            for(String son:tree.get(rt)){
                if(!bLayer&&rt.equals(root)) xdataTmp.add(son);
                dfs(son);
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            xdataTmp.add(root);
            dfs(root);
            for(String name:xdataTmp){
                //利润率=利润总额/营业收入
                double ratio=profit.get(name)/income.get(name);
                ratioRev.put(ratio,name);
            }
            Set<Double> keys=inc?ratioRev.keySet():ratioRev.descendingKeySet();
            int cnt=0;
            for(double ratio:keys){
                cnt++;
                String name=ratioRev.get(ratio);
                //百分数，且保留两位小数
                ratio=Math.round(ratio*10000.0)/100.0;
                double ratioPreM=profitPreM.get(name)/incomePreM.get(name);
                ratioPreM=Math.round(ratioPreM*10000.0)/100.0;
                double ratioPreY=profitPreY.get(name)/incomePreY.get(name);
                ratioPreY=Math.round(ratioPreY*10000.0)/100.0;
                xdata.add(name);
                ydata.add(ratio);
                ydataPreM.add(ratioPreM);
                ydataPreY.add(ratioPreY);
                String s=bSum?"全年累计数":"本月数";
                s+=inc?"升序":"降序";
                s+="第"+cnt+"名： "+name;
                context.write(new Text(s),new DoubleWritable(ratio));
                s=bSum?"上月累计数":"上月数";
                s+=inc?"升序":"降序";
                s+="第"+cnt+"名： "+name;
                context.write(new Text(s),new DoubleWritable(ratioPreM));
                s=bSum?"上年同期累计数":"上年同期数";
                s+=inc?"升序":"降序";
                s+="第"+cnt+"名： "+name;
                context.write(new Text(s),new DoubleWritable(ratioPreY));
                if(cnt==num) break;
            }
            cnt=1;
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
    public static HashMap profitabilityCompanyPartCount() throws Exception{
        jsonmap=new HashMap();
        a=new ArrayList();
        atmp=new ArrayList();
        names=new HashMap<String,String>();
        tree=new HashMap<String,Vector<String>>();
        profit=new HashMap<String,Double>();
        profitPreM=new HashMap<String,Double>();
        profitPreY=new HashMap<String,Double>();
        income=new HashMap<String,Double>();
        incomePreM=new HashMap<String,Double>();
        incomePreY=new HashMap<String,Double>();
        ratioRev=new TreeMap<Double,String>();
        xdataTmp=new ArrayList<String>();
        xdata=new ArrayList<String>();
        ydata=new ArrayList<Double>();
        ydataPreM=new ArrayList<Double>();
        ydataPreY=new ArrayList<Double>();
        CompanyNames.getNamesByCode();
        CompanyRelations.getRelations();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countProfitabilityCompanyPart";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(ProfitabilityCompanyPartModel.class); //指定作业类
        job.setMapperClass(ProfitabilityCompanyPartMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
        job.setReducerClass(ProfitabilityCompanyPartReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/com_sale"; //实验数据目录
        String outputDir="/carresult/profitabilitycompanypart/"+(y*100+m); //实验输出目录
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
