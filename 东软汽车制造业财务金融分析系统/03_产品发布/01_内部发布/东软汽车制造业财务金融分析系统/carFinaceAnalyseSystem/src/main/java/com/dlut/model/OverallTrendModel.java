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
            PMML pmml=new PMML(); //??????PMML??????
            InputStream inputStream; //???????????????
            try {
                inputStream=new FileInputStream(model_path); //???????????????????????????????????????
                pmml=PMMLUtil.unmarshal(inputStream); //?????????????????????PMML??????
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
            Map<FieldName,FieldValue> arguments=new LinkedHashMap<FieldName,FieldValue>();
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
        private static double get_pred(int ym) throws IOException {
            //??????????????????
            ClassPathResource classPathResource=new ClassPathResource("profit_model.pmml");
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
            for(int i=m+1;i<=12;i++){
                int ym=(y-1)*100+i;
                //?????????????????????????????????????????????????????????
                double money=Math.round(profitTmp.get(ym)/1000000.0)/100.0;
                profit.put(ym,money);
                pred.put(ym,get_pred(ym));
                context.write(new Text(ym+" ??????"),new DoubleWritable(money));
                int yPreM=y,mPreM=m-1;//??????????????????
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                double moneyPreM=Math.round(profitTmp.get(ymPreM)/1000000.0)/100.0;
                profitPre.add(moneyPreM);
                context.write(new Text(ym+" ????????????"),new DoubleWritable(moneyPreM));
                int ymPreY=100*(y-1)+m;//????????????????????????
                double moneyPreY=Math.round(profitTmp.get(ymPreY)/1000000.0)/100.0;
                double moneyIncY=money-moneyPreY;
                moneyIncY=Math.round(moneyIncY*100.0)/100.0;
                incY.add(moneyIncY);
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(moneyIncY));
                double ratioIncY=moneyIncY/moneyPreY;
                //??????????????????????????????????????????
                ratioIncY=Math.round(ratioIncY*10000)/100.0;
                incRatioY.add(ratioIncY);
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(ratioIncY));
                double moneyIncM=money-moneyPreM;
                moneyIncM=Math.round(moneyIncM*100.0)/100.0;
                incM.add(moneyIncM);
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(moneyIncM));
                double ratioIncM=moneyIncM/moneyPreM;
                ratioIncM=Math.round(ratioIncM*10000)/100.0;
                incRatioM.add(ratioIncM);
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(ratioIncM));
            }
            for(int i=1;i<=m;i++){
                int ym=y*100+i;
                double money=Math.round(profitTmp.get(ym)/1000000.0)/100.0;
                profit.put(ym,money);
                pred.put(ym,get_pred(ym));
                context.write(new Text(ym+" ??????"),new DoubleWritable(money));
                int yPreM=y,mPreM=m-1;//??????????????????
                if(mPreM==0){
                    yPreM--;
                    mPreM=12;
                }
                int ymPreM=yPreM*100+mPreM;
                double moneyPreM=Math.round(profitTmp.get(ymPreM)/1000000.0)/100.0;
                profitPre.add(moneyPreM);
                context.write(new Text(ym+" ????????????"),new DoubleWritable(moneyPreM));
                int ymPreY=100*(y-1)+m;//????????????????????????
                double moneyPreY=Math.round(profitTmp.get(ymPreY)/1000000.0)/100.0;
                double moneyIncY=money-moneyPreY;
                moneyIncY=Math.round(moneyIncY*100.0)/100.0;
                incY.add(moneyIncY);
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(moneyIncY));
                double ratioIncY=moneyIncY/moneyPreY;
                //??????????????????????????????????????????
                ratioIncY=Math.round(ratioIncY*10000)/100.0;
                incRatioY.add(ratioIncY);
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(ratioIncY));
                double moneyIncM=money-moneyPreM;
                moneyIncM=Math.round(moneyIncM*100.0)/100.0;
                incM.add(moneyIncM);
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(moneyIncM));
                double ratioIncM=moneyIncM/moneyPreM;
                ratioIncM=Math.round(ratioIncM*10000)/100.0;
                incRatioM.add(ratioIncM);
                context.write(new Text(ym+" ???????????????"),new DoubleWritable(ratioIncM));
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
        //1.?????? HDFS???MapReduce ??? Yarn ????????????
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.??????MapReduce??????????????????
        String jobName="countOverallTrend";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(OverallTrendModel.class); //???????????????
        job.setMapperClass(OverallTrendMapper.class); //??????Mapper???
        job.setMapOutputKeyClass(IntWritable.class); //??????Map??????Key???????????????
        job.setMapOutputValueClass(DoubleWritable.class); //??????Map??????Value???????????????
        job.setReducerClass(OverallTrendReducer.class); //??????Reducer???
        job.setOutputKeyClass(Text.class); //??????Reduce??????Key???????????????
        job.setOutputValueClass(DoubleWritable.class); //??????Reduce??????Value???????????????
        job.setNumReduceTasks(1); //???????????????????????????????????????Reduce????????????????????????????????????
        // 3.?????????????????????????????????
        String dataDir="/carinfo/com_sale"; //??????????????????
        String outputDir="/carresult/overalltrend/"+(y*100+m); //??????????????????
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
