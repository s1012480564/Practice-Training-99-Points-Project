package com.dlut.model;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

public class TargetModel {
    static boolean checkExists(String code) throws Exception{
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        List<Target> res=sqlSession.selectList("targetMapper.selByCode",code);
        sqlSession.close();
        return !res.isEmpty();
    }
    static boolean checkExists(String code,String subcode) throws Exception{
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        List<TargetRelation> res=sqlSession.selectList("targetMapper.selRela",new TargetRelation(code,subcode));
        sqlSession.close();
        return !res.isEmpty();
    }
    static boolean checkBiExists(String code,String subcode) throws Exception{
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        List<TargetRelation> res=sqlSession.selectList("targetMapper.selBiRela",new TargetRelation(code,subcode));
        sqlSession.close();
        return !res.isEmpty();
    }
    static HashMap<String,Boolean> getExists() throws Exception{
        HashMap<String,Boolean> exists=new HashMap<String,Boolean>();
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        List<Target> res=sqlSession.selectList("targetMapper.selAll");
        for(Target target:res){
            exists.put(target.getCode(),true);
        }
        return exists;
    }
    static HashMap<String,Vector<String>> getRelations() throws Exception{
        HashMap<String,Vector<String>> g=new HashMap<String,Vector<String>>();
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        List<TargetRelation> res=sqlSession.selectList("targetMapper.selRelaAll");
        for(TargetRelation tarRela:res){
            String code=tarRela.getCode(),subcode=tarRela.getSubcode();
            if(!g.containsKey(code)) g.put(code,new Vector<String>());
            g.get(code).add(subcode);
        }
        return g;
    }
}
