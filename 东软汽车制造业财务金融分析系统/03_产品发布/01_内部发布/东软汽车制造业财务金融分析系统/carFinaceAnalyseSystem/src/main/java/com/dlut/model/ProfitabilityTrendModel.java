package com.dlut.model;

import org.apache.commons.beanutils.WrapDynaBean;
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

public class ProfitabilityTrendModel {
    private static int y;
    private static int m;
    private static boolean bSum;
    private static ArrayList<String> addLis;
    private static HashMap jsonmap;
    private static ArrayList a,atmp;
    private static TreeMap<Integer,Double> profit,income;
    private static ArrayList<Integer> xdata;
    private static ArrayList<Double> yProfit,yProfitPred,yProfitPreY,yProfitPreSumY;
    private static ArrayList<Double> yIncome,yIncomePred,yIncomePreY,yIncomePreSumY;
    private static ArrayList<Double> yRatio,yRatioPred,yRatioPreY,yRatioPreSumY;
    public static void setY(int year){
        y=year;
    }
    public static void setM(int month){
        m=month;
    }
    public static void setbSum(boolean sumornot){bSum=sumornot;}
    public static void setAddtion(String addition){
        addLis=new ArrayList<String>(Arrays.asList(addition.split(",")));
    }
    private static class ProfitabilityTrendMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            context.write(new IntWritable(year*100+month),new Text(strs[3]+" "+strs[5]));
        }
    }
    private static class ProfitabilityTrendReducer extends Reducer<IntWritable,Text,Text, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val:values) {
                String strs[]=val.toString().split(" ");
                double profitMoney=Double.parseDouble(strs[0]);
                double incomeMoney=Double.parseDouble(strs[1]);
                profit.put(key.get(),profitMoney+profit.getOrDefault(key.get(),0.0));
                income.put(key.get(),incomeMoney+income.getOrDefault(key.get(),0.0));
            }
        }
        private static Evaluator loadModel(String model_path){
            PMML pmml=new PMML(); //定义PMML对象
            InputStream inputStream; //定义输入流
            try {
                inputStream=new FileInputStream(model_path); //输入流接到磁盘上的模型文件
                pmml= PMMLUtil.unmarshal(inputStream); //将输入流解析为PMML对象
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
            Map<FieldName, FieldValue> arguments=new LinkedHashMap<FieldName,FieldValue>();
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
        private static double get_pred(String yname,int ym) throws IOException{
            //获取静态资源
            ClassPathResource classPathResource=new ClassPathResource(yname+"_model.pmml");
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
            int cnt=0;
            for(int i=m+1;i<=12;i++){
                int ym=(y-1)*100+i;
                double profitMoney=profit.get(ym);
                double incomeMoney=income.get(ym);
                xdata.add(ym);
                if(bSum){
                    double profitSum=profitMoney+(cnt==0?0:yProfit.get(cnt-1));
                    double incomeSum=incomeMoney+(cnt==0?0:yIncome.get(cnt-1));
                    double profitPredSum=get_pred("profit",ym)+(cnt==0?0:yProfitPred.get(cnt-1));
                    double incomePredSum=get_pred("income",ym)+(cnt==0?0:yIncomePred.get(cnt-1));
                    yProfit.add(profitSum);
                    yProfitPred.add(profitPredSum);
                    yIncome.add(incomeSum);
                    yIncomePred.add(incomePredSum);
                }
                else{
                    yProfit.add(profitMoney);
                    yProfitPred.add(get_pred("profit",ym));
                    yIncome.add(incomeMoney);
                    yIncomePred.add(get_pred("income",ym));
                }
                int ymPreY=(y-2)*100+i;
                double profitMoneyPreY=profit.get(ymPreY);
                double profitMoneyPreSumY=profitMoneyPreY+(cnt==0?0:yProfitPreSumY.get(cnt-1));
                double incomeMoneyPreY=income.get(ymPreY);
                double incomeMoneyPreSumY=incomeMoneyPreY+(cnt==0?0:yIncomePreSumY.get(cnt-1));
                yProfitPreY.add(profitMoneyPreY);
                yProfitPreSumY.add(profitMoneyPreSumY);
                yIncomePreY.add(incomeMoneyPreY);
                yIncomePreSumY.add(incomeMoneyPreSumY);
                //利润率=利润总额/营业收入，百分数，且保留两位小数
                double ratio=yProfit.get(cnt)/yIncome.get(cnt);
                double ratioPred=yProfitPred.get(cnt)/yIncomePred.get(cnt);
                double ratioPreY=yProfitPreY.get(cnt)/yIncomePreY.get(cnt);
                double ratioPreSumY=yProfitPreSumY.get(cnt)/yIncomePreSumY.get(cnt);
                ratio=Math.round(ratio*10000.0)/100.0;
                ratioPred=Math.round(ratioPred*10000.0)/100.0;
                ratioPreY=Math.round(ratioPreY*10000.0)/100.0;
                ratioPreSumY=Math.round(ratioPreSumY*10000.0)/100.0;
                yRatio.add(ratio);
                yRatioPred.add(ratioPred);
                yRatioPreY.add(ratioPreY);
                yRatioPreSumY.add(ratioPreSumY);
                String s=ym+(bSum?" 全年累计数":" 本月数");
                context.write(new Text(s),new DoubleWritable(ratio));
                context.write(new Text(s+"预测值"),new DoubleWritable(ratioPred));
                context.write(new Text(ym+" 上年同期数"),new DoubleWritable(ratioPreY));
                context.write(new Text(ym+" 上年同期累计数"),new DoubleWritable(ratioPreSumY));
                cnt++;
            }
            for(int i=1;i<=m;i++){
                int ym=y*100+i;
                double profitMoney=profit.get(ym);
                double incomeMoney=income.get(ym);
                xdata.add(ym);
                if(bSum){
                    double profitSum=profitMoney+(cnt==0?0:yProfit.get(cnt-1));
                    double incomeSum=incomeMoney+(cnt==0?0:yIncome.get(cnt-1));
                    double profitPredSum=get_pred("profit",ym)+(cnt==0?0:yProfitPred.get(cnt-1));
                    double incomePredSum=get_pred("income",ym)+(cnt==0?0:yIncomePred.get(cnt-1));
                    yProfit.add(profitSum);
                    yProfitPred.add(profitPredSum);
                    yIncome.add(incomeSum);
                    yIncomePred.add(incomePredSum);
                }
                else{
                    yProfit.add(profitMoney);
                    yProfitPred.add(get_pred("profit",ym));
                    yIncome.add(incomeMoney);
                    yIncomePred.add(get_pred("income",ym));
                }
                int ymPreY=(y-1)*100+i;
                double profitMoneyPreY=profit.get(ymPreY);
                double profitMoneyPreSumY=profitMoneyPreY+(cnt==0?0:yProfitPreSumY.get(cnt-1));
                double incomeMoneyPreY=income.get(ymPreY);
                double incomeMoneyPreSumY=incomeMoneyPreY+(cnt==0?0:yIncomePreSumY.get(cnt-1));
                yProfitPreY.add(profitMoneyPreY);
                yProfitPreSumY.add(profitMoneyPreSumY);
                yIncomePreY.add(incomeMoneyPreY);
                yIncomePreSumY.add(incomeMoneyPreSumY);
                //利润率=利润总额/营业收入，百分数，且保留两位小数
                double ratio=yProfit.get(cnt)/yIncome.get(cnt);
                double ratioPred=yProfitPred.get(cnt)/yIncomePred.get(cnt);
                double ratioPreY=yProfitPreY.get(cnt)/yIncomePreY.get(cnt);
                double ratioPreSumY=yProfitPreSumY.get(cnt)/yIncomePreSumY.get(cnt);
                ratio=Math.round(ratio*10000.0)/100.0;
                ratioPred=Math.round(ratioPred*10000.0)/100.0;
                ratioPreY=Math.round(ratioPreY*10000.0)/100.0;
                ratioPreSumY=Math.round(ratioPreSumY*10000.0)/100.0;
                yRatio.add(ratio);
                yRatioPred.add(ratioPred);
                yRatioPreY.add(ratioPreY);
                yRatioPreSumY.add(ratioPreSumY);
                String s=ym+(bSum?" 全年累计数":" 本月数");
                context.write(new Text(s),new DoubleWritable(ratio));
                context.write(new Text(s+"预测值"),new DoubleWritable(ratioPred));
                context.write(new Text(ym+" 上年同期数"),new DoubleWritable(ratioPreY));
                context.write(new Text(ym+" 上年同期累计数"),new DoubleWritable(ratioPreSumY));
                cnt++;
            }
            cnt=0;
            if(addLis.contains("preY")) cnt++;
            if(addLis.contains("preSumY")) cnt++;
            jsonmap.put("cnt",cnt);
            atmp.add("date");
            atmp.add(bSum?"全年累计数":"本月数");
            atmp.add("预测值");
            if(addLis.contains("preY")) atmp.add("上年同期数");
            if(addLis.contains("preSumY")) atmp.add("上年同期累计数");
            a.add(atmp.clone());
            atmp.clear();
            for(int i=0;i<xdata.size();i++){
                atmp.add(xdata.get(i));
                atmp.add(yRatio.get(i));
                atmp.add(yRatioPred.get(i));
                if(addLis.contains("preY")) atmp.add(yRatioPreY.get(i));
                if(addLis.contains("preSumY")) atmp.add(yRatioPreSumY.get(i));
                a.add(atmp.clone());
                atmp.clear();
            }
            jsonmap.put("dataset",a);
        }
    }
    public static HashMap profitabilityTrendCount() throws Exception{
        jsonmap=new HashMap();
        profit=new TreeMap<Integer,Double>();
        income=new TreeMap<Integer,Double>();
        a=new ArrayList();
        atmp=new ArrayList();
        xdata=new ArrayList<Integer>();
        yProfit=new ArrayList<Double>();
        yProfitPred=new ArrayList<Double>();
        yProfitPreY=new ArrayList<Double>();
        yProfitPreSumY=new ArrayList<Double>();
        yIncome=new ArrayList<Double>();
        yIncomePred=new ArrayList<Double>();
        yIncomePreY=new ArrayList<Double>();
        yIncomePreSumY=new ArrayList<Double>();
        yRatio=new ArrayList<Double>();
        yRatioPred=new ArrayList<Double>();
        yRatioPreY=new ArrayList<Double>();
        yRatioPreSumY=new ArrayList<Double>();
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countProfitabilityTrend";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(ProfitabilityTrendModel.class); //指定作业类
        job.setMapperClass(ProfitabilityTrendMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
        job.setReducerClass(ProfitabilityTrendReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/com_sale"; //实验数据目录
        String outputDir="/carresult/profitabilitytrend/"+(y*100+m); //实验输出目录
        outputDir+=(bSum?"/sum":"/cur");
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
