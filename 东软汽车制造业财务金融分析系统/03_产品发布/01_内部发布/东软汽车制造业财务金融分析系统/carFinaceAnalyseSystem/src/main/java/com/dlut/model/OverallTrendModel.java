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
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.jpmml.model.PMMLUtil;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class OverallTrendModel {
    private static int y;
    private static int m;
    private static HashMap jsonmap;
    private static TreeMap<Integer,Double> profitTmp;
    private static TreeMap<Integer,Double> profit;
    private static TreeMap<Integer,Double> pred;
    private static List<Double> profitPre;
    private static List<Double> incM;
    private static List<Double> incY;
    private static List<Double> incRatioM;
    private static List<Double> incRatioY;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    private static class OverallTrendMapper extends Mapper<Object,Text,IntWritable,DoubleWritable> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            double money=Double.parseDouble(strs[3]);
            context.write(new IntWritable(year*100+month),new DoubleWritable(money));
        }
    }
    private static class OverallTrendReducer extends Reducer<IntWritable,DoubleWritable,Text, DoubleWritable>{
        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException{
            for(DoubleWritable val:values) {
                profitTmp.put(key.get(),val.get()+profitTmp.getOrDefault(key.get(),0.0));
            }
        }
        private static Evaluator loadModel(String model_path){
            PMML pmml=new PMML(); //定义PMML对象
            InputStream inputStream; //定义输入流
            try {
                inputStream=new FileInputStream(model_path); //输入流接到磁盘上的模型文件
                pmml=PMMLUtil.unmarshal(inputStream); //将输入流解析为PMML对象
            }catch (Exception e){
                e.printStackTrace();
            }
            ModelEvaluatorFactory modelEvaluatorFactory=ModelEvaluatorFactory.newInstance(); //实例化一个模型构造工厂
            return modelEvaluatorFactory.newModelEvaluator(pmml);
        }
        private static Object predict(Evaluator evaluator, int x){
            Map<String, Integer> data=new HashMap<String, Integer>(); //定义测试数据Map，存入各元自变量
            data.put("x1", x); //键"x"为自变量的名称，应与训练数据中的自变量名称一致
            List<InputField> inputFieldList=evaluator.getInputFields(); //得到模型各元自变量的属性列表
            Map<FieldName,FieldValue> arguments=new LinkedHashMap<FieldName,FieldValue>();
            for (InputField inputField:inputFieldList) { //遍历各元自变量的属性列表
                FieldName inputFieldName=inputField.getName();
                Object rawValue=data.get(inputFieldName.getValue()); //取出该元变量的值
                FieldValue inputFieldValue=inputField.prepare(rawValue); //将值加入该元自变量属性中
                arguments.put(inputFieldName, inputFieldValue); //变量名和变量值的对加入LinkedHashMap
            }
            Map<FieldName, ?> results=evaluator.evaluate(arguments); //进行预测
            List<TargetField> targetFieldList=evaluator.getTargetFields(); //得到模型各元因变量的属性列表
            FieldName targetFieldName=targetFieldList.get(0).getName(); //第一元因变量名称
            return results.get(targetFieldName);
        }
        private static double get_pred(int ym) throws IOException {
            //获取静态资源
            ClassPathResource classPathResource=new ClassPathResource("profit_model.pmml");
            InputStream inputStream=classPathResource.getInputStream();
            File file=classPathResource.getFile();// 获取文件
            String model_path=file.getPath();// 获取文件路径
            Evaluator model=loadModel(model_path); //加载模型
            Object r=predict(model,ym); //预测
            //保留两位小数
            double res=Double.parseDouble(r.toString());
            res=Math.round(res*100.0)/100.0;
            return res;
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            for(int i=m+1;i<=12;i++){
                int ym=(y-1)*100+i;
                //单位亿元，且保留两位小数，计算方法后同
                double money=Math.round(profitTmp.get(ym)/1000000.0)/100.0;
                profit.put(ym,money);
                pred.put(ym,get_pred(ym));
                context.write(new Text(ym+" 利润"),new DoubleWritable(money));
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                double moneyPreM=Math.round(profitTmp.get(ymPreM)/1000000.0)/100.0;
                profitPre.add(moneyPreM);
                context.write(new Text(ym+" 上月利润"),new DoubleWritable(moneyPreM));
                int ymPreY=100*(y-1)+m;//上一年同月的年月
                double moneyPreY=Math.round(profitTmp.get(ymPreY)/1000000.0)/100.0;
                double moneyIncY=money-moneyPreY;
                moneyIncY=Math.round(moneyIncY*100.0)/100.0;
                incY.add(moneyIncY);
                context.write(new Text(ym+" 同比增长量"),new DoubleWritable(moneyIncY));
                double ratioIncY=moneyIncY/moneyPreY;
                //百分数，且保留两位小数，后同
                ratioIncY=Math.round(ratioIncY*10000)/100.0;
                incRatioY.add(ratioIncY);
                context.write(new Text(ym+" 同比增长率"),new DoubleWritable(ratioIncY));
                double moneyIncM=money-moneyPreM;
                moneyIncM=Math.round(moneyIncM*100.0)/100.0;
                incM.add(moneyIncM);
                context.write(new Text(ym+" 环比增长量"),new DoubleWritable(moneyIncM));
                double ratioIncM=moneyIncM/moneyPreM;
                ratioIncM=Math.round(ratioIncM*10000)/100.0;
                incRatioM.add(ratioIncM);
                context.write(new Text(ym+" 环比增长率"),new DoubleWritable(ratioIncM));
            }
            for(int i=1;i<=m;i++){
                int ym=y*100+i;
                double money=Math.round(profitTmp.get(ym)/1000000.0)/100.0;
                profit.put(ym,money);
                pred.put(ym,get_pred(ym));
                context.write(new Text(ym+" 利润"),new DoubleWritable(money));
                int yPreM=y,mPreM=m-1;//上个月的年月
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                double moneyPreM=Math.round(profitTmp.get(ymPreM)/1000000.0)/100.0;
                profitPre.add(moneyPreM);
                context.write(new Text(ym+" 上月利润"),new DoubleWritable(moneyPreM));
                int ymPreY=100*(y-1)+m;//上一年同月的年月
                double moneyPreY=Math.round(profitTmp.get(ymPreY)/1000000.0)/100.0;
                double moneyIncY=money-moneyPreY;
                moneyIncY=Math.round(moneyIncY*100.0)/100.0;
                incY.add(moneyIncY);
                context.write(new Text(ym+" 同比增长量"),new DoubleWritable(moneyIncY));
                double ratioIncY=moneyIncY/moneyPreY;
                //百分数，且保留两位小数，后同
                ratioIncY=Math.round(ratioIncY*10000)/100.0;
                incRatioY.add(ratioIncY);
                context.write(new Text(ym+" 同比增长率"),new DoubleWritable(ratioIncY));
                double moneyIncM=money-moneyPreM;
                moneyIncM=Math.round(moneyIncM*100.0)/100.0;
                incM.add(moneyIncM);
                context.write(new Text(ym+" 环比增长量"),new DoubleWritable(moneyIncM));
                double ratioIncM=moneyIncM/moneyPreM;
                ratioIncM=Math.round(ratioIncM*10000)/100.0;
                incRatioM.add(ratioIncM);
                context.write(new Text(ym+" 环比增长率"),new DoubleWritable(ratioIncM));
            }
            jsonmap.put("profit",profit);
            jsonmap.put("pred",pred);
            jsonmap.put("profitPre",profitPre);
            jsonmap.put("incM",incM);
            jsonmap.put("incY",incY);
            jsonmap.put("incRatioM",incRatioM);
            jsonmap.put("incRatioY",incRatioY);
        }
    }
    public static HashMap overallTrendCount() throws Exception{
        jsonmap=new HashMap();
        profitTmp=new TreeMap<Integer,Double>();
        profit=new TreeMap<Integer,Double>();
        pred=new TreeMap<Integer,Double>();
        profitPre=new ArrayList<Double>();
        incM=new ArrayList<Double>();
        incY=new ArrayList<Double>();
        incRatioM=new ArrayList<Double>();
        incRatioY=new ArrayList<Double>();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countOverallTrend";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(OverallTrendModel.class); //指定作业类
        job.setMapperClass(OverallTrendMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(DoubleWritable.class); //指定Map输出Value的数据类型
        job.setReducerClass(OverallTrendReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/com_sale"; //实验数据目录
        String outputDir="/carresult/overalltrend/"+(y*100+m); //实验输出目录
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
