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
            PMML pmml=new PMML(); //??????PMML??????
            InputStream inputStream; //???????????????
            try {
                inputStream=new FileInputStream(model_path); //???????????????????????????????????????
                pmml= PMMLUtil.unmarshal(inputStream); //?????????????????????PMML??????
            }catch (Exception e){
                e.printStackTrace();
            }
            ModelEvaluatorFactory modelEvaluatorFactory=ModelEvaluatorFactory.newInstance(); //?????????????????????????????????
            return modelEvaluatorFactory.newModelEvaluator(pmml);
        }
        private static Object predict(Evaluator evaluator, int x){
            Map<String, Integer> data=new HashMap<String, Integer>(); //??????????????????Map????????????????????????
            data.put("x1", x); //???"x"?????????????????????????????????????????????????????????????????????
            List<InputField> inputFieldList=evaluator.getInputFields(); //??????????????????????????????????????????
            Map<FieldName, FieldValue> arguments=new LinkedHashMap<FieldName,FieldValue>();
            for (InputField inputField:inputFieldList) { //????????????????????????????????????
                FieldName inputFieldName=inputField.getName();
                Object rawValue=data.get(inputFieldName.getValue()); //????????????????????????
                FieldValue inputFieldValue=inputField.prepare(rawValue); //????????????????????????????????????
                arguments.put(inputFieldName, inputFieldValue); //?????????????????????????????????LinkedHashMap
            }
            Map<FieldName, ?> results=evaluator.evaluate(arguments); //????????????
            List<TargetField> targetFieldList=evaluator.getTargetFields(); //??????????????????????????????????????????
            FieldName targetFieldName=targetFieldList.get(0).getName(); //????????????????????????
            return results.get(targetFieldName);
        }
        private static double get_pred(String yname,int ym) throws IOException{
            //??????????????????
            ClassPathResource classPathResource=new ClassPathResource(yname+"_model.pmml");
            InputStream inputStream=classPathResource.getInputStream();
            File file=classPathResource.getFile();// ????????????
            String model_path=file.getPath();// ??????????????????
            Evaluator model=loadModel(model_path); //????????????
            Object r=predict(model,ym); //??????
            //??????????????????
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
                //?????????=????????????/????????????????????????????????????????????????
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
                String s=ym+(bSum?" ???????????????":" ?????????");
                context.write(new Text(s),new DoubleWritable(ratio));
                context.write(new Text(s+"?????????"),new DoubleWritable(ratioPred));
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(ratioPreY));
                context.write(new Text(ym+" ?????????????????????"),new DoubleWritable(ratioPreSumY));
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
                //?????????=????????????/????????????????????????????????????????????????
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
                String s=ym+(bSum?" ???????????????":" ?????????");
                context.write(new Text(s),new DoubleWritable(ratio));
                context.write(new Text(s+"?????????"),new DoubleWritable(ratioPred));
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(ratioPreY));
                context.write(new Text(ym+" ?????????????????????"),new DoubleWritable(ratioPreSumY));
                cnt++;
            }
            cnt=0;
            if(addLis.contains("preY")) cnt++;
            if(addLis.contains("preSumY")) cnt++;
            jsonmap.put("cnt",cnt);
            atmp.add("date");
            atmp.add(bSum?"???????????????":"?????????");
            atmp.add("?????????");
            if(addLis.contains("preY")) atmp.add("???????????????");
            if(addLis.contains("preSumY")) atmp.add("?????????????????????");
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
        //1.?????? HDFS???MapReduce ??? Yarn ????????????
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.??????MapReduce??????????????????
        String jobName="countProfitabilityTrend";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(ProfitabilityTrendModel.class); //???????????????
        job.setMapperClass(ProfitabilityTrendMapper.class); //??????Mapper???
        job.setMapOutputKeyClass(IntWritable.class); //??????Map??????Key???????????????
        job.setMapOutputValueClass(Text.class); //??????Map??????Value???????????????
        job.setReducerClass(ProfitabilityTrendReducer.class); //??????Reducer???
        job.setOutputKeyClass(Text.class); //??????Reduce??????Key???????????????
        job.setOutputValueClass(DoubleWritable.class); //??????Reduce??????Value???????????????
        job.setNumReduceTasks(1); //???????????????????????????????????????Reduce????????????????????????????????????
        // 3.?????????????????????????????????
        String dataDir="/carinfo/com_sale"; //??????????????????
        String outputDir="/carresult/profitabilitytrend/"+(y*100+m); //??????????????????
        outputDir+=(bSum?"/sum":"/cur");
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
