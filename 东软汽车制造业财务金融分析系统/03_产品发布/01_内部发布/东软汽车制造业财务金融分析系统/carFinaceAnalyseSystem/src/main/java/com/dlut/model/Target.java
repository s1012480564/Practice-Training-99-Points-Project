package com.dlut.model;

import java.io.Serializable;

public class Target implements Serializable {
    private String target_code;
    private String target_name;
    private String content;
    public Target(){}
    public Target(String code){
        target_code=code;
        target_name="无";
        content="无";
    }
    public Target(String code,String content){
        target_code=code;
        target_name="无";
        this.content=content;
    }
    public Target(String code,String name,String content){
        target_code=code;
        target_name=name;
        this.content=content;
    }
    public String getCode(){
        return target_code;
    }
    public String getName(){
        return target_name;
    }
    public String getContent(){
        return content;
    }
    public void setCode(String code){
        target_code=code;
    }
    public void setName(String name){
        target_name=name;
    }
    public void setContent(String content){
        this.content=content;
    }
}
