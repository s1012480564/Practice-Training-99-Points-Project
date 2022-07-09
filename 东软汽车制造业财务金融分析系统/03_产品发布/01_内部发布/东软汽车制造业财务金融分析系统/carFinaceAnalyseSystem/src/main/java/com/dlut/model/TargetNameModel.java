package com.dlut.model;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TargetNameModel {
    private static HashMap jsonmap;
    private static ArrayList a,atmp;
    public static HashMap retrieve(String name) throws Exception {
        jsonmap=new HashMap();
        a=new ArrayList();
        atmp=new ArrayList();
        StringBuffer pattern=new StringBuffer(name);
        for(int i=0;i<pattern.length();i+=2){
            pattern.insert(i,"%");
        }
        pattern.append("%");
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        List<Target> res=sqlSession.selectList("targetMapper.selByNamePattern",pattern.toString());
        sqlSession.close();
        for(Target target:res){
            StringBuffer sb=new StringBuffer(target.getName());
            for(int i=0,j=0;i<name.length();i++){
                while(sb.charAt(j)!=name.charAt(i)) j++;
                sb.insert(j+1,"</em>");
                sb.insert(j,"<em>");
                j+=10;
            }
            atmp.add(target.getCode());
            atmp.add(sb.toString());
            atmp.add(target.getContent());
            a.add(atmp.clone());
            atmp.clear();
        }
        jsonmap.put("dataset",a);
        return jsonmap;
    }
    public static boolean update(String code,String name) throws Exception{
        if(!TargetModel.checkExists(code)) return false;
        InputStream resourceAsStream=Resources.getResourceAsStream("SqlMapConfig.xml");
        SqlSessionFactory sqlSessionFactory=new SqlSessionFactoryBuilder().build(resourceAsStream);
        SqlSession sqlSession=sqlSessionFactory.openSession();
        Target target=new Target(code);
        target.setName(name);
        sqlSession.update("targetMapper.updateName",target);
        sqlSession.commit();
        sqlSession.close();
        return true;
    }
}
