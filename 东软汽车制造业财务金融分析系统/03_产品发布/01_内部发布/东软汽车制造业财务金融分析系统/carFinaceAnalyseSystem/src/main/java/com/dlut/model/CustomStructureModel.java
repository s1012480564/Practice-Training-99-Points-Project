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
        private static Object predict(Evaluator evaluator, int x1,int x2){
            Map<String, Integer> data=new HashMap<String, Integer>(); //定义测试数据Map，存入各元自变量
            data.put("0", x1); //键"x"为自变量的名称，应与训练数据中的自变量名称一致
            data.put("1", x2);
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
        private static double get_pred(int ym,int id) throws IOException{
            //获取静态资源
            ClassPathResource classPathResource=new ClassPathResource("sale_model.pmml");
            InputStream inputStream=classPathResource.getInputStream();
            File file=classPathResource.getFile();// 获取文件
            String model_path=file.getPath();// 获取文件路径
            Evaluator model=loadModel(model_path); //加载模型
            Object r=predict(model,ym,id); //预测
            //保留两位小数
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
                //上个月的年月
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
                //上年同月的年月
                int ymPreY1=(y1-1)*100+m1,ymPreY2=(y2-1)*100+m2;
                if(bSum){//要求累计数
                    if(nowym>=ym1&&nowym<=ym2){
                        cost.put(name,money+cost.getOrDefault(name,0.0));
                        costPred.put(name,get_pred(nowym,ids.get(name))+costPred.getOrDefault(name,0.0));
                    }
                    if(nowym>=ymPreM1&&nowym<=ymPreM2)
                        costPreM.put(name,money+costPreM.getOrDefault(name,0.0));
                    if(nowym>=ymPreY1&&nowym<=ymPreY2)
                        costPreY.put(name,money+costPreY.getOrDefault(name,0.0));
                }
                else{//要求本月数
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
            atmp.add(bSum?"本月累计数":"本月数");
            if(addLis.contains("pred")) atmp.add("预测值");
            if(addLis.contains("preM")) atmp.add(bSum?"上月累计数":"上月数");
            if(addLis.contains("preY")) atmp.add(bSum?"上年同期累计数":"上年同期数");
            a.add(atmp.clone());
            atmp.clear();
            for(String x:name){
                double money=cost.get(x);
                double moneyPred=costPred.get(x);
                double moneyPreM=costPreM.get(x);
                double moneyPreY=costPreY.get(x);
                //单位万元，且保留两位小数
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
                String s=x+(bSum?" 本月累计数":" 本月数");
                context.write(new Text(s),new DoubleWritable(money));
                context.write(new Text(s+"预测值"),new DoubleWritable(moneyPred));
                s=x+(bSum?" 上月累计数":" 上月数");
                context.write(new Text(s),new DoubleWritable(moneyPreM));
                s=x+(bSum?" 上年同期累计数":" 上年同期数");
                context.write(new Text(s),new DoubleWritable(moneyPreY));
            }
            jsonmap.put("dataset",a);
        }
    }
    private static void setNameId(){
        name.add("职工薪酬");
        name.add("运输费");
        name.add("促销费");
        name.add("产品质量保证费");
        name.add("业务宣传费");
        name.add("广告费");
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
        //1.设置 HDFS、MapReduce 和 Yarn 配置信息
        String namenode_ip="192.168.17.10";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",hdfs);
        //2.设置MapReduce作业配置信息
        String jobName="countCustomStructure";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(CustomStructureModel.class); //指定作业类
        job.setMapperClass(CustomStructureMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(IntWritable.class); //指定Map输出Key的数据类型
        job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
        job.setReducerClass(CustomStructureReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
        job.setOutputValueClass(DoubleWritable.class); //指定Reduce输出Value的数据类型
        job.setNumReduceTasks(1); //计算最终结果，只能运行一个Reduce任务，即输出一个结果文件
        // 3.设置作业输入和输出路径
        String dataDir="/carinfo/sale_cost"; //实验数据目录
        String outputDir="/carresult/customstructure/"+(y1*100+m1)+"-"+(y2*100+m2); //实验输出目录
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
