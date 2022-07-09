package com.hdfs2mysql;

import java.io.Serializable;

public class Product implements Serializable {
    private int year;
    private int month;
    private String product_company;
    private int sale_num;
    Product(){
        sale_num=0;
    }
    public void setYear(int year){
        this.year=year;
    }
    public void setMonth(int month){
        this.month=month;
    }
    public void setProduct_company(String product_company){
        this.product_company=product_company;
    }
    public void setSale_num(int sale_num){
        this.sale_num=sale_num;
    }
    public int getYear(){
        return year;
    }
    public int getMonth(){
        return month;
    }
    public String getProduct_company(){
        return product_company;
    }
    public int getSale_num(){
        return sale_num;
    }
}
