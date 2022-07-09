package com.dlut.model;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TargetRelationModel {
    private static HashMap jsonmap;
    private static ArrayList a,atmp;
    public static HashMap retrieve(String code) throws Exception {
        jsonmap=new HashMap();
        if(!TargetModel.checkExists(code)){
            jsonmap.put("found",false);
            return jsonmap;
        }
        a=new ArrayList();
        atmp=new ArrayList();
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        List<TargetRelation> res=sqlSession.selectList("targetMapper.selBiRelaByCode",code);
        sqlSession.close();
        jsonmap.put("found",true);
        if(res.isEmpty()){
            jsonmap.put("isnull",true);
            return jsonmap;
        }
        jsonmap.put("isnull",false);
        for(TargetRelation tarRela:res){
            atmp.add(tarRela.getCode());
            atmp.add(tarRela.getSubcode());
            a.add(atmp.clone());
            atmp.clear();
        }
        jsonmap.put("dataset",a);
        return jsonmap;
    }
    public static HashMap create(String code,String subcode) throws Exception{
        jsonmap=new HashMap();
        if(!TargetModel.checkExists(code)){
            jsonmap.put("noCode",true);
            return jsonmap;
        }
        jsonmap.put("noCode",false);
        if(!TargetModel.checkExists(subcode)){
            jsonmap.put("noSubcode",true);
            return jsonmap;
        }
        jsonmap.put("noSubcode",false);
        if(TargetModel.checkBiExists(code,subcode)){
            jsonmap.put("hasBiRela",true);
            return jsonmap;
        }
        jsonmap.put("hasBiRela",false);
        if(TargetModel.checkExists(code,subcode)){
            jsonmap.put("hasRela",true);
            return jsonmap;
        }
        jsonmap.put("hasRela",false);
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        sqlSession.insert("targetMapper.insertRela",new TargetRelation(code,subcode));
        sqlSession.insert("targetMapper.insertRela",new TargetRelation(subcode,code));
        sqlSession.commit();
        sqlSession.close();
        return jsonmap;
    }
    public static Boolean delete(String code,String subcode) throws Exception{
        if(!TargetModel.checkBiExists(code,subcode)) return false;
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        sqlSession.delete("targetMapper.delRela",new TargetRelation(code,subcode));
        sqlSession.delete("targetMapper.delRela",new TargetRelation(subcode,code));
        sqlSession.commit();
        sqlSession.close();
        return true;
    }
}
