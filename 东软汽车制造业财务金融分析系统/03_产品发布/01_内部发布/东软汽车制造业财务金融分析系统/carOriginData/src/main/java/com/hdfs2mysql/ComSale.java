package com.hdfs2mysql;

import java.io.Serializable;

public class ComSale implements Serializable {
    private int year;
    private int month;
    private String com_code;
    private double profit;
    private double asset;
    private double income;
    ComSale(){
        profit=asset=income=0;
    }
    public void setYear(int year){
        this.year=year;
    }
    public void setMonth(int month){
        this.month=month;
    }
    public void setCom_code(String com_code){
        this.com_code=com_code;
    }
    public void setProfit(double profit){
        this.profit=profit;
    }
    public void setAsset(double asset){
        this.asset=asset;
    }
    public void setIncome(double income){
        this.income=income;
    }
    public int getYear(){
        return year;
    }
    public int getMonth(){
        return month;
    }
    public String getCom_code(){
        return com_code;
    }
    public double getProfit(){
        return profit;
    }
    public double getAsset(){
        return asset;
    }
    public double getIncome(){
        return income;
    }
}
