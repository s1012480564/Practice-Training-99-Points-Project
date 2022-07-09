package com.hdfs2mysql;

import java.io.Serializable;

public class Total implements Serializable {
    private int year;
    private int month;
    private int yield;
    private int stock;
    private double debt;
    private double owner_interest;
    private double float_asset;
    private double float_debt;
    private double main_income;
    private double main_cost;
    private double tax;
    private double other_income;
    private double other_cost;
    private double fix_asset;
    private double tech_fee;
    private double intangible_asset;
    private double dev_fee;
    private double salary;
    private double interest_in;
    private double interest_out;
    private double exchange_change;
    private double asset_devalue;
    private double fair_value;
    private double invest;
    Total(){
        yield=stock=0;
        debt=owner_interest=float_asset=float_debt=main_income=main_cost=tax=other_income=other_cost=fix_asset=tech_fee=intangible_asset=dev_fee=salary=interest_in=interest_out=exchange_change=asset_devalue=fair_value=invest=0;
    }
    public void setYear(int year){
        this.year=year;
    }
    public void setMonth(int month){
        this.month=month;
    }
    public void setYield(int yield){
        this.yield=yield;
    }
    public void setStock(int stock){
        this.stock=stock;
    }
    public void setDebt(double debt){
        this.debt=debt;
    }
    public void setOwner_interest(double owner_interest){
        this.owner_interest=owner_interest;
    }
    public void setFloat_asset(double float_asset){
        this.float_asset=float_asset;
    }
    public void setFloat_debt(double float_debt){
        this.float_debt=float_debt;
    }
    public void setMain_income(double main_income){
        this.main_income=main_income;
    }
    public void setMain_cost(double main_cost){
        this.main_cost=main_cost;
    }
    public void setTax(double tax){
        this.tax=tax;
    }
    public void setOther_income(double other_income){
        this.other_income=other_income;
    }
    public void setOther_cost(double other_cost){
        this.other_cost=other_cost;
    }
    public void setFix_asset(double fix_asset){
        this.fix_asset=fix_asset;
    }
    public void setTech_fee(double tech_fee){
        this.tech_fee=tech_fee;
    }
    public void setIntangible_asset(double intangible_asset){
        this.intangible_asset=intangible_asset;
    }
    public void setDev_fee(double dev_fee){
        this.dev_fee=dev_fee;
    }
    public void setSalary(double salary){
        this.salary=salary;
    }
    public void setInterest_in(double interest_in){
        this.interest_in=interest_in;
    }
    public void setInterest_out(double interest_out){
        this.interest_out=interest_out;
    }
    public void setExchange_change(double exchange_change){
        this.exchange_change=exchange_change;
    }
    public void setAsset_devalue(double asset_devalue){
        this.asset_devalue=asset_devalue;
    }
    public void setFair_value(double fair_value){
        this.fair_value=fair_value;
    }
    public void setInvest(double invest){
        this.invest=invest;
    }
    public int getYear(){
        return year;
    }
    public int getMonth(){
        return month;
    }
    public int getYield(){
        return yield;
    }
    public int getStock(){
        return stock;
    }
    public double getDebt(){
        return debt;
    }
    public double getOwner_interest(){
        return owner_interest;
    }
    public double getFloat_asset(){
        return float_asset;
    }
    public double getFloat_debt(){
        return float_debt;
    }
    public double getMain_income(){
        return main_income;
    }
    public double getMain_cost(){
        return main_cost;
    }
    public double getTax(){
        return tax;
    }
    public double getOther_income(){
        return other_income;
    }
    public double getOther_cost(){
        return other_cost;
    }
    public double getFix_asset(){
        return fix_asset;
    }
    public double getTech_fee(){
        return tech_fee;
    }
    public double getIntangible_asset(){
        return intangible_asset;
    }
    public double getDev_fee(){
        return dev_fee;
    }
    public double getSalary(){
        return salary;
    }
    public double getInterest_in(){
        return interest_in;
    }
    public double getInterest_out(){
        return interest_out;
    }
    public double getExchange_change(){
        return exchange_change;
    }
    public double getAsset_devalue(){
        return asset_devalue;
    }
    public double getFair_value(){
        return fair_value;
    }
    public double getInvest(){
        return invest;
    }
}
