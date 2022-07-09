package com.dlut.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class OverallBenefitModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static double[][] a;//按照月报报告内嵌表03的对应位置填写二维数组
    private static TreeMap<Integer,Double> income;//0
    private static TreeMap<Integer,Double> mainIncome;//1
    private static TreeMap<Integer,Double> mainCost;//2
    private static TreeMap<Integer,Double> tax;//3
    private static TreeMap<Integer,Double> otherIncome;//5
    private static TreeMap<Integer,Double> otherCost;//6
    private static TreeMap<Integer,Double> saleSalary;//9
    private static TreeMap<Integer,Double> trans;//10
    private static TreeMap<Integer,Double> promote;//11
    private static TreeMap<Integer,Double> assure;//12
    private static TreeMap<Integer,Double> propagate;//13
    private static TreeMap<Integer,Double> ad;//14
    private static TreeMap<Integer,Double> fixAsset;//16
    private static TreeMap<Integer,Double> techFee;//17
    private static TreeMap<Integer,Double> intangibleAsset;//18
    private static TreeMap<Integer,Double> devFee;//19
    private static TreeMap<Integer,Double> salary;//20
    private static TreeMap<Integer,Double> interestIn;//22
    private static TreeMap<Integer,Double> interestOut;//23
    private static TreeMap<Integer,Double> exchange;//24
    private static TreeMap<Integer,Double> assetDevalue;//26
    private static TreeMap<Integer,Double> fairValue;//27
    private static TreeMap<Integer,Double> invest;//28
    private static TreeMap<Integer,Double> profit;//29
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class ComSaleCount{
        private static class ComSaleMapper extends Mapper<Object, Text, IntWritable,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                //profit income
                context.write(new IntWritable(year*100+month),new Text(strs[3]+" "+strs[5]));
            }
        }
        private static class ComSaleReducer extends Reducer<IntWritable,Text,Text,Text> {
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                for (Text val:values) {
                    String[] strs=val.toString().split(" ");
                    double profitMoney=Double.parseDouble(strs[0]);
                    double incomeMoney=Double.parseDouble(strs[1]);
                    profit.put(key.get(),profitMoney+profit.getOrDefault(key.get(),0.0));
                    income.put(key.get(),incomeMoney+income.getOrDefault(key.get(),0.0));
                }
            }
        }
        private static void countComSale() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countComeSale";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(ComSaleCount.class); //指定作业类
            job.setMapperClass(ComSaleMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(ComSaleReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/com_sale"; //实验数据目录
            String outputDir="/carresult/overallbenefit/"+(y*100+m); //实验输出目录
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
    private static class SaleCostCount{
        private static class SaleCostMapper extends Mapper<Object, Text, IntWritable,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                //sale_column fee
                context.write(new IntWritable(year*100+month),new Text(strs[2]+" "+strs[3]));
            }
        }
        private static class SaleCostReducer extends Reducer<IntWritable,Text,Text,Text> {
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                //saleSalary trans promote assure propagate ad
                for (Text val:values) {
                    String[] strs=val.toString().split(" ");
                    String name=strs[0];
                    double money=Double.parseDouble(strs[1]);
                    if(name.equals("职工薪酬"))
                        saleSalary.put(key.get(),money+saleSalary.getOrDefault(key.get(),0.0));
                    else if(name.equals("运输费"))
                        trans.put(key.get(),money+trans.getOrDefault(key.get(),0.0));
                    else if(name.equals("促销费"))
                        promote.put(key.get(),money+promote.getOrDefault(key.get(),0.0));
                    else if(name.equals("产品质量保证费"))
                        assure.put(key.get(),money+assure.getOrDefault(key.get(),0.0));
                    else if(name.equals("业务宣传费"))
                        propagate.put(key.get(),money+propagate.getOrDefault(key.get(),0.0));
                    else ad.put(key.get(),money+ad.getOrDefault(key.get(),0.0));
                }
            }
        }
        private static void countCostSale() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countSaleCost";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(SaleCostCount.class); //指定作业类
            job.setMapperClass(SaleCostMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(SaleCostReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/sale_cost"; //实验数据目录
            String outputDir="/carresult/overallbenefit/"+(y*100+m); //实验输出目录
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
    private static class TotalCount{
        private static class TotalMapper extends Mapper<Object, Text, IntWritable,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                int year=Integer.parseInt(strs[0]);
                int month=Integer.parseInt(strs[1]);
                String s=strs[8];
                for(int i=9;i<24;i++) s+=" "+strs[i];
                //mainIncome mainCost tax otherIncome otherCost fixAsset techFee intangibleAsset
                //devFee salary interestIn interestOut exchange assetDevalue fairValue invest
                context.write(new IntWritable(year*100+month),new Text(s));
            }
        }
        private static class TotalReducer extends Reducer<IntWritable,Text,Text,Text> {
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException{
                for(Text val:values) {
                    String[] strs=val.toString().split(" ");
                    double mainIncomeMoney=Double.parseDouble(strs[0]);
                    double mainCostMoney=Double.parseDouble(strs[1]);
                    double taxMoney=Double.parseDouble(strs[2]);
                    double otherIncomeMoney=Double.parseDouble(strs[3]);
                    double otherCostMoney=Double.parseDouble(strs[4]);
                    double fixAssetMoney=Double.parseDouble(strs[5]);
                    double techFeeMoney=Double.parseDouble(strs[6]);
                    double intangibleAssetMoney=Double.parseDouble(strs[7]);
                    double devFeeMoney=Double.parseDouble(strs[8]);
                    double salaryMoney=Double.parseDouble(strs[9]);
                    double interestInMoney=Double.parseDouble(strs[10]);
                    double interestOutMoney=Double.parseDouble(strs[11]);
                    double exchangeMoney=Double.parseDouble(strs[12]);
                    double assetDevalueMoney=Double.parseDouble(strs[13]);
                    double fairValueMoney=Double.parseDouble(strs[14]);
                    double investMoney=Double.parseDouble(strs[15]);
                    mainIncome.put(key.get(),mainIncomeMoney+mainIncome.getOrDefault(key.get(),0.0));
                    mainCost.put(key.get(),mainCostMoney+mainCost.getOrDefault(key.get(),0.0));
                    tax.put(key.get(),taxMoney+tax.getOrDefault(key.get(),0.0));
                    otherIncome.put(key.get(),otherIncomeMoney+otherIncome.getOrDefault(key.get(),0.0));
                    otherCost.put(key.get(),otherCostMoney+otherCost.getOrDefault(key.get(),0.0));
                    fixAsset.put(key.get(),fixAssetMoney+fixAsset.getOrDefault(key.get(),0.0));
                    techFee.put(key.get(),techFeeMoney+techFee.getOrDefault(key.get(),0.0));
                    intangibleAsset.put(key.get(),intangibleAssetMoney+intangibleAsset.getOrDefault(key.get(),0.0));
                    devFee.put(key.get(),devFeeMoney+devFee.getOrDefault(key.get(),0.0));
                    salary.put(key.get(),salaryMoney+salary.getOrDefault(key.get(),0.0));
                    interestIn.put(key.get(),interestInMoney+interestIn.getOrDefault(key.get(),0.0));
                    interestOut.put(key.get(),interestOutMoney+interestOut.getOrDefault(key.get(),0.0));
                    exchange.put(key.get(),exchangeMoney+exchange.getOrDefault(key.get(),0.0));
                    assetDevalue.put(key.get(),assetDevalueMoney+assetDevalue.getOrDefault(key.get(),0.0));
                    fairValue.put(key.get(),fairValueMoney+fairValue.getOrDefault(key.get(),0.0));
                    invest.put(key.get(),investMoney+invest.getOrDefault(key.get(),0.0));
                }
            }
            public void calMoney(TreeMap<Integer,Double> map,int k){
                int ym=y*100+m;
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                double sum=0,sumPre=0;
                for(int i=1;i<=12;i++) {
                    sum+=map.get(y*100+i);
                    sumPre+=map.get((y-1)*100+i);
                }
                //金额数单位亿元，保留两位小数
                a[k][0]=Math.round(map.get(ym)/1000000.0)/100.0;
                a[k][1]=Math.round(map.get(ymPreM)/1000000.0)/100.0;
                a[k][2]=Math.round((a[k][0]-a[k][1])*100.0)/100.0;
                a[k][3]=Math.round(sum/1000000.0)/100.0;
                a[k][4]=Math.round(sumPre/1000000.0)/100.0;
                a[k][5]=Math.round((a[k][3]-a[k][4])*100.0)/100.0;
            }
            public void calAdd(int k,int k1,int k2){
                for(int j=0;j<6;j++){
                    double sum=0;
                    for(int i=k1;i<=k2;i++) sum+=a[i][j];
                    a[k][j]=Math.round(sum*100.0)/100.0;
                }
            }
            public void calMinus(int k,int k1,int k2){
                for(int j=0;j<6;j++){
                    double sum=a[k1][j];
                    for(int i=k1+1;i<=k2;i++) sum-=a[i][j];
                    a[k][j]=Math.round(sum*100.0)/100.0;
                }
            }
            public void ContextWrite(Context context,String[] name,int k) throws IOException,InterruptedException {
                String val=""+a[k][0];
                for(int i=1;i<6;i++) val+=" "+a[k][i];
                context.write(new Text(name[k]),new Text(val));
            }
            public void cleanup(Context context) throws IOException,InterruptedException {
                calMoney(income,0);
                calMoney(mainIncome,1);
                calMoney(mainCost,2);
                calMoney(tax,3);
                calMoney(otherIncome,5);
                calMoney(otherCost,6);
                calMoney(saleSalary,9);
                calMoney(trans,10);
                calMoney(promote,11);
                calMoney(assure,12);
                calMoney(propagate,13);
                calMoney(ad,14);
                calMoney(fixAsset,16);
                calMoney(techFee,17);
                calMoney(intangibleAsset,18);
                calMoney(devFee,19);
                calMoney(salary,20);
                calMoney(interestIn,22);
                calMoney(interestOut,23);
                calMoney(exchange,24);
                calMoney(assetDevalue,26);
                calMoney(fairValue,27);
                calMoney(invest,28);
                calMoney(profit,29);
                calAdd(8,9,14);
                calAdd(15,16,20);
                calAdd(21,22,24);
                calAdd(25,26,28);
                calMinus(4,1,3);
                calMinus(7,5,6);
                context.write(new Text("项目"),new Text("本月 上月 增减 本年 上年 增减"));
                String[] name={"营业总收入","主营业务收入","主营业务成本","营业税金及附加",
                            "主营业务利润","其他业务收入","其他业务成本","其他业务利润","销售费用",
                            "其中：职工薪酬","运输费","促销费","产品质量保证费","业务宣传费",
                            "广告费","管理费用","其中：固定资产维修费","技术转让费","无形资产摊销",
                            "研究开发费","职工薪酬","财务费用","其中：利息收入","利息支出",
                            "汇兑损益（收益以“一”填列）","期间费用合计","资产减值损失",
                            "加：公允价值变动收益","投资收益","利润总额"};
                jsonmap.put("name",name);
                for(int i=0;i<30;i++) ContextWrite(context,name,i);
            }
        }
        private static void countTotal() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countTotal";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(TotalCount.class); //指定作业类
            job.setMapperClass(TotalMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(TotalReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
            // 3.设置作业输入和输出路径
            String dataDir="/carinfo/total"; //实验数据目录
            String outputDir="/carresult/overallbenefit/"+(y*100+m); //实验输出目录
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
    public static HashMap overallBenefitCount() throws Exception{
        jsonmap=new HashMap();
        income=new TreeMap<Integer,Double>();
        mainIncome=new TreeMap<Integer,Double>();
        mainCost=new TreeMap<Integer,Double>();
        tax=new TreeMap<Integer,Double>();
        otherIncome=new TreeMap<Integer,Double>();
        otherCost=new TreeMap<Integer,Double>();
        saleSalary=new TreeMap<Integer,Double>();
        trans=new TreeMap<Integer,Double>();
        promote=new TreeMap<Integer,Double>();
        assure=new TreeMap<Integer,Double>();
        propagate=new TreeMap<Integer,Double>();
        ad=new TreeMap<Integer,Double>();
        fixAsset=new TreeMap<Integer,Double>();
        techFee=new TreeMap<Integer,Double>();
        intangibleAsset=new TreeMap<Integer,Double>();
        devFee=new TreeMap<Integer,Double>();
        salary=new TreeMap<Integer,Double>();
        interestIn=new TreeMap<Integer,Double>();
        interestOut=new TreeMap<Integer,Double>();
        exchange=new TreeMap<Integer,Double>();
        assetDevalue=new TreeMap<Integer,Double>();
        fairValue=new TreeMap<Integer,Double>();
        invest=new TreeMap<Integer,Double>();
        profit=new TreeMap<Integer,Double>();
        a=new double[30][6];
        ComSaleCount.countComSale();
        SaleCostCount.countCostSale();
        TotalCount.countTotal();
        jsonmap.put("dataset",a);
        return jsonmap;
    }
}