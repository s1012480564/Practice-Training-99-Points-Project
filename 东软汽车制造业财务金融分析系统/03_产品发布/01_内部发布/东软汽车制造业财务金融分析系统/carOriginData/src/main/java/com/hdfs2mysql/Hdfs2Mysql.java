package com.hdfs2mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class Hdfs2Mysql {
    private static String[] saleColumn={"职工薪酬","运输费","促销费","产品质量保证费","业务宣传费","广告费"};
    private static HashMap<String,Total> totalMap=new HashMap<>();//key:year+month
    private static HashMap<String,ComSale> comSaleMap=new HashMap<>();//key:year+month+com_code
    private static HashMap<String,Product> productMap=new HashMap<>();//key:year+month+product_company
    private static HashMap<String,SaleCost> saleCostMap=new HashMap<>();//key:year+month+sale_column
    private static class CountProductDay{
        private static class ProductDayMapper extends Mapper<Object,Text,Text,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                String skey=strs[1]+","+strs[2]+","+strs[0];
                String sval=strs[4]+","+strs[5]+","+strs[6];
                context.write(new Text(skey),new Text(sval));
            }
        }
        private static class ProductDayReducer extends Reducer<Text,Text,Text,Text> {
            @Override
            public void reduce(Text key,Iterable<Text> values,Context context)
                    throws IOException,InterruptedException{
                String[] kstrs=key.toString().split(",");
                if(!productMap.containsKey(kstrs[0]+kstrs[1]+kstrs[2]))
                    productMap.put(kstrs[0]+kstrs[1]+kstrs[2],new Product());
                Product product=productMap.get(kstrs[0]+kstrs[1]+kstrs[2]);
                product.setYear(Integer.parseInt(kstrs[0]));
                product.setMonth(Integer.parseInt(kstrs[1]));
                product.setProduct_company(kstrs[2]);
                if(!totalMap.containsKey(kstrs[0]+kstrs[1]))
                    totalMap.put(kstrs[0]+kstrs[1],new Total());
                Total total=totalMap.get(kstrs[0]+kstrs[1]);
                total.setYear(Integer.parseInt(kstrs[0]));
                total.setMonth(Integer.parseInt(kstrs[1]));
                int n1=0,n2=0,n3=0;
                for(Text val:values) {
                    String[] vstrs=val.toString().split(",");
                    n1+=Integer.parseInt(vstrs[2]);
                    n2+=Integer.parseInt(vstrs[0]);
                    n3+=Integer.parseInt(vstrs[1]);
                }
                product.setSale_num(n1);
                total.setYield(total.getYield()+n2);
                total.setStock(total.getStock()+n3);
            }
        }
        private static void count() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countProductDay";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountProductDay.class); //指定作业类
            job.setMapperClass(ProductDayMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(ProductDayReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            // 3.设置作业输入和输出路径
            String dataDir="/car_origin_data/product_day"; //实验数据目录
            String outputDir="/result_temp"; //实验输出目录
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
    private static class CountSaleDay{
        private static class SaleDayMapper extends Mapper<Object,Text,Text,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                String skey=strs[1]+","+strs[2]+","+strs[0];
                String sval=strs[4]+","+strs[5]+","+strs[6]+","+strs[7];
                context.write(new Text(skey),new Text(sval));
            }
        }
        private static class SaleDayReducer extends Reducer<Text,Text,Text,Text> {
            @Override
            public void reduce(Text key,Iterable<Text> values,Context context)
                    throws IOException,InterruptedException{
                String[] kstrs=key.toString().split(",");
                if(!comSaleMap.containsKey(kstrs[0]+kstrs[1]+kstrs[2]))
                    comSaleMap.put(kstrs[0]+kstrs[1]+kstrs[2],new ComSale());
                ComSale comSale=comSaleMap.get(kstrs[0]+kstrs[1]+kstrs[2]);
                comSale.setYear(Integer.parseInt(kstrs[0]));
                comSale.setMonth(Integer.parseInt(kstrs[1]));
                comSale.setCom_code(kstrs[2]);
                if(!totalMap.containsKey(kstrs[0]+kstrs[1]))
                    totalMap.put(kstrs[0]+kstrs[1],new Total());
                Total total=totalMap.get(kstrs[0]+kstrs[1]);
                total.setYear(Integer.parseInt(kstrs[0]));
                total.setMonth(Integer.parseInt(kstrs[1]));
                double n1=0,n2=0,n3=0,n4=0;
                for(Text val:values) {
                    String[] vstrs=val.toString().split(",");
                    n1+=Double.parseDouble(vstrs[0]);
                    n2+=Double.parseDouble(vstrs[1]);
                    n3+=Double.parseDouble(vstrs[2]);
                    n4+=Double.parseDouble(vstrs[3]);
                }
                comSale.setProfit(n1);
                comSale.setAsset(n2);
                comSale.setIncome(n3);
                total.setDebt(total.getDebt()+n4);
            }
        }
        private static void count() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countSaleDay";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountSaleDay.class); //指定作业类
            job.setMapperClass(SaleDayMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(SaleDayReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            // 3.设置作业输入和输出路径
            String dataDir="/car_origin_data/sale_day"; //实验数据目录
            String outputDir="/result_temp"; //实验输出目录
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
    private static class CountSaleMonth{
        private static class SaleMonthMapper extends Mapper<Object,Text,Text,Text> {
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
                String[] strs=value.toString().split(",");
                String skey=strs[1]+","+strs[2];
                String sval=strs[3];
                for(int i=4;i<=27;i++) sval+=","+strs[i];
                context.write(new Text(skey),new Text(sval));
            }
        }
        private static class SaleMonthReducer extends Reducer<Text,Text,Text,Text> {
            @Override
            public void reduce(Text key,Iterable<Text> values,Context context)
                    throws IOException,InterruptedException{
                String[] kstrs=key.toString().split(",");
                double[] n=new double[25];
                for(int i=0;i<25;i++) n[i]=0;
                if(!totalMap.containsKey(kstrs[0]+kstrs[1]))
                    totalMap.put(kstrs[0]+kstrs[1],new Total());
                Total total=totalMap.get(kstrs[0]+kstrs[1]);
                total.setYear(Integer.parseInt(kstrs[0]));
                total.setMonth(Integer.parseInt(kstrs[1]));
                for(Text val:values) {
                    String[] vstrs=val.toString().split(",");
                    for(int i=0;i<25;i++)
                        n[i]+=Double.parseDouble(vstrs[i]);
                }
                for(int i=0;i<6;i++){
                    if(!saleCostMap.containsKey(kstrs[0]+kstrs[1]+saleColumn[i]))
                        saleCostMap.put(kstrs[0]+kstrs[1]+saleColumn[i],new SaleCost());
                    SaleCost saleCost=saleCostMap.get(kstrs[0]+kstrs[1]+saleColumn[i]);
                    saleCost.setYear(Integer.parseInt(kstrs[0]));
                    saleCost.setMonth(Integer.parseInt(kstrs[1]));
                    saleCost.setSale_column(saleColumn[i]);
                    saleCost.setFee(n[i]);
                }
                total.setOwner_interest(n[6]);
                total.setFloat_asset(n[7]);
                total.setFloat_debt(n[8]);
                total.setMain_income(n[9]);
                total.setMain_cost(n[10]);
                total.setTax(n[11]);
                total.setOther_income(n[12]);
                total.setOther_cost(n[13]);
                total.setFix_asset(n[14]);
                total.setTech_fee(n[15]);
                total.setIntangible_asset(n[16]);
                total.setDev_fee(n[17]);
                total.setSalary(n[18]);
                total.setInterest_in(n[19]);
                total.setInterest_out(n[20]);
                total.setExchange_change(n[21]);
                total.setAsset_devalue(n[22]);
                total.setFair_value(n[23]);
                total.setInvest(n[24]);
            }
        }
        private static void count() throws Exception{
            //1.设置 HDFS、MapReduce 和 Yarn 配置信息
            String namenode_ip="192.168.17.10";
            String hdfs="hdfs://"+namenode_ip+":9000";
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS",hdfs);
            //2.设置MapReduce作业配置信息
            String jobName="countSaleMonth";
            Job job=Job.getInstance(conf,jobName);
            job.setJarByClass(CountSaleMonth.class); //指定作业类
            job.setMapperClass(SaleMonthMapper.class); //指定Mapper类
            job.setMapOutputKeyClass(Text.class); //指定Map输出Key的数据类型
            job.setMapOutputValueClass(Text.class); //指定Map输出Value的数据类型
            job.setReducerClass(SaleMonthReducer.class); //指定Reducer类
            job.setOutputKeyClass(Text.class); //指定Reduce输出Key的数据类型
            job.setOutputValueClass(Text.class); //指定Reduce输出Value的数据类型
            // 3.设置作业输入和输出路径
            String dataDir="/car_origin_data/sale_month"; //实验数据目录
            String outputDir="/result_temp"; //实验输出目录
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
    private static double dot2(double n){
        return Math.round(n*100)/100.0;
    }
    public static void main(String args[]) throws Exception{
        CountProductDay.count();
        CountSaleDay.count();
        CountSaleMonth.count();
        InputStream resourceAsStream = Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        for(Total total:totalMap.values()){
            total.setDebt(dot2(total.getDebt()));
            total.setOwner_interest(dot2(total.getOwner_interest()));
            total.setFloat_asset(dot2(total.getFloat_asset()));
            total.setFloat_debt(dot2(total.getFloat_debt()));
            total.setMain_income(dot2(total.getMain_income()));
            total.setMain_cost(dot2(total.getMain_cost()));
            total.setTax(dot2(total.getTax()));
            total.setOther_income(dot2(total.getOther_income()));
            total.setOther_cost(dot2(total.getOther_cost()));
            total.setFix_asset(dot2(total.getFix_asset()));
            total.setTech_fee(dot2(total.getTech_fee()));
            total.setIntangible_asset(dot2(total.getIntangible_asset()));
            total.setDev_fee(dot2(total.getDev_fee()));
            total.setSalary(dot2(total.getSalary()));
            total.setInterest_in(dot2(total.getInterest_in()));
            total.setInterest_out(dot2(total.getInterest_out()));
            total.setExchange_change(dot2(total.getExchange_change()));
            total.setAsset_devalue(dot2(total.getAsset_devalue()));
            total.setFair_value(dot2(total.getFair_value()));
            total.setInvest(dot2(total.getInvest()));
            sqlSession.insert("carMapper.insertTotal",total);
        }
        for(Product product:productMap.values()){
            sqlSession.insert("carMapper.insertProduct",product);
        }
        for(ComSale comSale:comSaleMap.values()){
            comSale.setProfit(dot2(comSale.getProfit()));
            comSale.setAsset(dot2(comSale.getAsset()));
            comSale.setIncome(dot2(comSale.getIncome()));
            sqlSession.insert("carMapper.insertComSale",comSale);
        }
        for(SaleCost saleCost:saleCostMap.values()){
            saleCost.setFee(dot2(saleCost.getFee()));
            sqlSession.insert("carMapper.insertSaleCost",saleCost);
        }
        sqlSession.commit();
        sqlSession.close();
    }
}
