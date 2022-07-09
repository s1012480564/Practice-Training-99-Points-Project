package com.hdfs2mysql;

import java.io.Serializable;

public class SaleCost implements Serializable {
    private int year;
    private int month;
    private String sale_column;
    private double fee;
    SaleCost(){
        fee=0;
    }
    public void setYear(int year){
        this.year=year;
    }
    public void setMonth(int month){
        this.month=month;
    }
    public void setSale_column(String sale_column){
        this.sale_column=sale_column;
    }
    public void setFee(double fee){
        this.fee=fee;
    }
    public int getYear(){
        return year;
    }
    public int getMonth(){
        return month;
    }
    public String getSale_column(){
        return sale_column;
    }
    public double getFee(){
        return fee;
    }
}
