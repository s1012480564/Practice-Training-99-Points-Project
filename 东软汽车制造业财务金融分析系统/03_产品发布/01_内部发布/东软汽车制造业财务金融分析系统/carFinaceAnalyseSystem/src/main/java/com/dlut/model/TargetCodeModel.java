package com.dlut.model;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

/**
 * 增删改查均使用 mybatis 直接对 mysql 操作
 * 如果使用 mapreduce 进行查询，那么每次增删改后，都要将 mysql 中的数据表同步到 hdfs 中
 * 否则 mapreduce 将查询到的是过期的数据
 * 除非用 kafka 实时消费 mysql 日志，否则实时同步将会在开销上十分不合理
 * 因此指标维护模块的 crud 均直接对 mysql 操作
 * 之后几个子模块不再赘述
 * */
public class TargetCodeModel {
    private static HashMap jsonmap;
    private static ArrayList a,atmp;
    private static HashMap<String,Boolean> exists;//存在为true，反之false
    private static HashMap<String,Vector<String>> g;//关系图 G 的动态邻接表
    private static HashMap<String,Boolean> vis;//dfs中的访问标记
    /**
     * c: 图染色算法中，用于记录染色状态，具体取值解释如下
     * 0: 不删除或不可删除，可从1转移来
     * 1: 待删除状态，可转移到0或2
     * 2: 待删除的确定可以删除，从1转移来
     * */
    private static HashMap<String,Integer> c;
    private static String[] codes;//待批量删除的codes
    public static void setCodes(String strCodes){
        codes=strCodes.split("[,，;；、]");
    }
    public static HashMap retrieve(String code) throws Exception {
        jsonmap=new HashMap();
        a=new ArrayList();
        atmp=new ArrayList();
        StringBuffer pattern=new StringBuffer(code);
        for(int i=0;i<pattern.length();i+=2){
            pattern.insert(i,"%");
        }
        pattern.append("%");
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        List<Target> res=sqlSession.selectList("targetMapper.selByCodePattern",pattern.toString());
        sqlSession.close();
        for(Target target:res){
            StringBuffer sb=new StringBuffer(target.getCode());
            for(int i=0,j=0;i<code.length();i++){
                while(sb.charAt(j)!=code.charAt(i)) j++;
                sb.insert(j+1,"</em>");
                sb.insert(j,"<em>");
                j+=10;
            }
            atmp.add(sb.toString());
            atmp.add(target.getName());
            atmp.add(target.getContent());
            a.add(atmp.clone());
            atmp.clear();
        }
        jsonmap.put("dataset",a);
        return jsonmap;
    }
    public static boolean create(String code,String content) throws Exception{
        if(TargetModel.checkExists(code)) return false;
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        Target target=new Target(code,content);
        sqlSession.insert("targetMapper.insert",target);
        sqlSession.commit();
        sqlSession.close();
        return true;
    }
    /**
     * 一般的暴力算法思想是，一个点是否可删，以这个点为起点，去DFS
     * 路径上的点中，存在一个不删除的点，那么该点就不可删除
     * 对 m 个待删除的点，我们要以每个点为起点，做一个DFS
     * 设数据库 n 条记录，平均和最差的总时间复杂度为 O(mn)，这是不可接受的
     * 因此类比于匈牙利算法二分图染色的思想
     * 这里采用图染色算法，一次性确认哪些可删
     * 平均和最差的总时间复杂度均为 O(n)
     * */
    private static void dfs(String u){
        vis.put(u,true);
        for(String v:g.getOrDefault(u,new Vector<String>())){
            if(vis.getOrDefault(v,false)||c.getOrDefault(v,0)==2) continue;
            if(c.getOrDefault(v,0)==1) dfs(v);
            if(c.getOrDefault(v,0)==0){
                c.put(u,0);
                return;
            }
        }
        c.put(u,2);
    }
    public static HashMap delete() throws Exception{
        exists=TargetModel.getExists();
        g=TargetModel.getRelations();
        jsonmap=new HashMap();
        a=new ArrayList();
        atmp=new ArrayList();
        c=new HashMap<String,Integer>();
        for(String u:codes){
            c.put(u,1);
        }
        for(String u:codes){
            atmp.add(u);
            if(!exists.getOrDefault(u,false)){
                atmp.add("删除失败，该指标不存在");
                a.add(atmp.clone());
                atmp.clear();
                continue;
            }
            if(c.get(u)==1){
                vis=new HashMap<String,Boolean>();
                dfs(u);
            }
            if(c.get(u)==0){
                atmp.add("删除失败，存在未被同时删除的子孙依赖");
                a.add(atmp.clone());
                atmp.clear();
            }
            else{
                InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
                SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
                SqlSession sqlSession=sqlSessionFactory.openSession();
                Target target=new Target(u);
                sqlSession.delete("targetMapper.delCode",target);
                //建表的时候，tar_relation表的外键约束，删除时设定了CASCADE
                //所以删除该 code，tar_relation中以该 code 为外键的记录项都会被自动删除
                //但是以该 code 为 subcode 的记录项不会被自动删除，需要手动删除
                TargetRelation tarRelation=new TargetRelation("",u);
                sqlSession.delete("targetMapper.delRelaBySubcode",tarRelation);
                sqlSession.commit();
                sqlSession.close();
                atmp.add("删除成功");
                a.add(atmp.clone());
                atmp.clear();
            }
        }
        jsonmap.put("dataset",a);
        return jsonmap;
    }
}
