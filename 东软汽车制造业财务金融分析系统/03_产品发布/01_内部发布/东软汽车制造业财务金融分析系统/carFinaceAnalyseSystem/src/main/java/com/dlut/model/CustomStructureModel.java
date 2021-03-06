package com.dlut.model;

import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
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

public class CustomStructureModel {
    private static int y1,m1,y2,m2;
    private static boolean bSum;
    private static ArrayList<String> addLis;
    private static HashMap jsonmap;
    private static ArrayList a,atmp;
    private static ArrayList<String> name;
    private static HashMap<String,Integer> ids;
    private static HashMap<String,Double> cost,costPred,costPreM,costPreY;
    public static void setYM(int year1,int month1,int year2,int month2){
        y1=year1;
        m1=month1;
        y2=year2;
        m2=month2;
    }
    public static void setbSum(boolean sumornot){bSum=sumornot;}
    public static void setAddtion(String addition){
        addLis=new ArrayList<String>(Arrays.asList(addition.split(",")));
    }
    private static class CustomStructureMapper extends Mapper<Object, Text, IntWritable,Text> {
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] strs=value.toString().split(",");
            int year=Integer.parseInt(strs[0]);
            int month=Integer.parseInt(strs[1]);
            context.write(new IntWritable(year*100+month),new Text(strs[2]+" "+strs[3]));
        }
    }
    private static class CustomStructureReducer extends Reducer<IntWritable,Text,Text, DoubleWritable> {
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
        private static Object predict(Evaluator evaluator, int x1,int x2){
            Map<String, Integer> data=new HashMap<String, Integer>(); //??????????????????Map????????????????????????
            data.put("0", x1); //???"x"?????????????????????????????????????????????????????????????????????
            data.put("1", x2);
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
        private static double get_pred(int ym,int id) throws IOException{
            //??????????????????
            ClassPathResource classPathResource=new ClassPathResource("sale_model.pmml");
            InputStream inputStream=classPathResource.getInputStream();
            File file=classPathResource.getFile();// ????????????
            String model_path=file.getPath();// ??????????????????
            Evaluator model=loadModel(model_path); //????????????
            Object r=predict(model,ym,id); //??????
            //??????????????????
            double res=Double.parseDouble(r.toString());
            res=Math.round(res*100.0)/100.0;
            return res;
        }
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val:values) {
                int nowym=key.get();
                String strs[]=val.toString().split(" ");
                String name=strs[0];
                double money=Double.parseDouble(strs[1]);
                int ym1=y1*100+m1,ym2=y2*100+m2;
                //??????????????????
                int yPreM1=y1,mPreM1=m1-1;
                if(mPreM1==0){
                    yPreM1--;
                    mPreM1=12;
                }
                int yPreM2=y2,mPreM2=m2-1;
                if(mPreM2==0){
                    yPreM2--;
                    mPreM2=12;
                }
                int ymPreM1=yPreM1*100+mPreM1;
                int ymPreM2=yPreM2*100+mPreM2;
                //?????????????????????
                int ymPreY1=(y1-1)*100+m1,ymPreY2=(y2-1)*100+m2;
                if(bSum){//???????????????
                    if(nowym>=ym1&&nowym<=ym2){
                        cost.put(name,money+cost.getOrDefault(name,0.0));
                        costPred.put(name,get_pred(nowym,ids.get(name))+costPred.getOrDefault(name,0.0));
                    }
                    if(nowym>=ymPreM1&&nowym<=ymPreM2)
                        costPreM.put(name,money+costPreM.getOrDefault(name,0.0));
                    if(nowym>=ymPreY1&&nowym<=ymPreY2)
                        costPreY.put(name,money+costPreY.getOrDefault(name,0.0));
                }
                else{//???????????????
                    if(nowym==ym2){
                        cost.put(name,money);
                        costPred.put(name,get_pred(nowym,ids.get(name)));
                    }
                    if(nowym==ymPreM2)
                        costPreM.put(name,money);
                    if(nowym==ymPreY2)
                        costPreY.put(name,money);
                }
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            int cnt=1;
            if(addLis.contains("pred")) cnt++;
            if(addLis.contains("preM")) cnt++;
            if(addLis.contains("preY")) cnt++;
            jsonmap.put("cnt",cnt);
            atmp.add("date");
            atmp.add(bSum?"???????????????":"?????????");
            if(addLis.contains("pred")) atmp.add("?????????");
            if(addLis.contains("preM")) atmp.add(bSum?"???????????????":"?????????");
            if(addLis.contains("preY")) atmp.add(bSum?"?????????????????????":"???????????????");
            a.add(atmp.clone());
            atmp.clear();
            for(String x:name){
                double money=cost.get(x);
                double moneyPred=costPred.get(x);
                double moneyPreM=costPreM.get(x);
                double moneyPreY=costPreY.get(x);
                //????????????????????????????????????
                money=Math.round(money/100.0)/100.0;
                moneyPred=Math.round(moneyPred*100.0)/100.0;
                moneyPreM=Math.round(moneyPreM/100.0)/100.0;
                moneyPreY=Math.round(moneyPreY/100.0)/100.0;
                atmp.add(x);
                atmp.add(money);
                if(addLis.contains("pred")) atmp.add(moneyPred);
                if(addLis.contains("preM")) atmp.add(moneyPreM);
                if(addLis.contains("preY")) atmp.add(moneyPreY);
                a.add(atmp.clone());
                atmp.clear();
                String s=x+(bSum?" ???????????????":" ?????????");
                context.write(new Text(s),new DoubleWritable(money));
                context.write(new Text(s+"?????????"),new DoubleWritable(moneyPred));
                s=x+(bSum?" ???????????????":" ?????????");
                context.write(new Text(s),new DoubleWritable(moneyPreM));
                s=x+(bSum?" ?????????????????????":" ???????????????");
                context.write(new Text(s),new DoubleWritable(moneyPreY));
            }
            jsonmap.put("dataset",a);
        }
    }
    private static void setNameId(){
        name.add("????????????");
        name.add("?????????");
        name.add("?????????");
        name.add("?????????????????????");
        name.add("???????????????");
        name.add("?????????");
        for(int i=0;i<name.size();i++) ids.put(name.get(i),i+1);
    }
    public static HashMap customStructureCount() throws Exception{
        jsonmap=new HashMap();
        a=new ArrayList();
        atmp=new ArrayList();
        name=new ArrayList<String>();
        ids=new HashMap<String,Integer>();
        cost=new HashMap<String,Double>();
        costPred=new HashMap<String,Double>();
        costPreM=new HashMap<String,Double>();
        costPreY=new HashMap<String,Double>();
        setNameId();
        //1.?????? HDFS???MapReduce ??? Yarn ????????????
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.??????MapReduce??????????????????
        String jobName="countCustomStructure";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(CustomStructureModel.class); //???????????????
        job.setMapperClass(CustomStructureMapper.class); //??????Mapper???
        job.setMapOutputKeyClass(IntWritable.class); //??????Map??????Key???????????????
        job.setMapOutputValueClass(Text.class); //??????Map??????Value???????????????
        job.setReducerClass(CustomStructureReducer.class); //??????Reducer???
        job.setOutputKeyClass(Text.class); //??????Reduce??????Key???????????????
        job.setOutputValueClass(DoubleWritable.class); //??????Reduce??????Value???????????????
        job.setNumReduceTasks(1); //???????????????????????????????????????Reduce????????????????????????????????????
        // 3.?????????????????????????????????
        String dataDir="/carinfo/sale_cost"; //??????????????????
        String outputDir="/carresult/customstructure/"+(y1*100+m1)+"-"+(y2*100+m2); //??????????????????
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
