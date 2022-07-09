package com.dlut.model;

import java.io.Serializable;

public class TargetRelation implements Serializable {
    private String target_code,subtarget_code;
    public TargetRelation(){}
    public TargetRelation(String code,String subcode){
        target_code=code;
        subtarget_code=subcode;
    }
    public String getCode(){
        return target_code;
    }
    public String getSubcode(){
        return subtarget_code;
    }
    public void setCode(String code){
        target_code=code;
    }
    public void setName(String subcode){
        subtarget_code=subcode;
    }
}
