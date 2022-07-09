package com.dlut.controller;

import com.dlut.model.*;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;

@Controller
public class OverallController {
    @RequestMapping("overalleconomy.do")
    @ResponseBody
    public HashMap overalleconomy(@RequestParam String year, @RequestParam String month) throws Exception {
        OverallEconomyModel.setY(Integer.parseInt(year));
        OverallEconomyModel.setM(Integer.parseInt(month));
        return OverallEconomyModel.overallEconomyCount();
    }
    @RequestMapping("overallsale.do")
    @ResponseBody
    public HashMap overallsale(@RequestParam String year, @RequestParam String month) throws Exception {
        OverallSaleModel.setY(Integer.parseInt(year));
        OverallSaleModel.setM(Integer.parseInt(month));
        return OverallSaleModel.overallSaleCount();
    }
    @RequestMapping("overallbenefit.do")
    @ResponseBody
    public HashMap overallbenefit(@RequestParam String year, @RequestParam String month) throws Exception {
        OverallBenefitModel.setY(Integer.parseInt(year));
        OverallBenefitModel.setM(Integer.parseInt(month));
        return OverallBenefitModel.overallBenefitCount();
    }
    @RequestMapping("overalltrend.do")
    @ResponseBody
    public HashMap overalltrend(@RequestParam String year, @RequestParam String month) throws Exception {
        OverallTrendModel.setY(Integer.parseInt(year));
        OverallTrendModel.setM(Integer.parseInt(month));
        return OverallTrendModel.overallTrendCount();
    }
    @RequestMapping("overallstructure.do")
    @ResponseBody
    public HashMap overallstructure(@RequestParam String year, @RequestParam String month,@RequestParam String order,@RequestParam String num) throws Exception {
        OverallStructureModel.setY(Integer.parseInt(year));
        OverallStructureModel.setM(Integer.parseInt(month));
        OverallStructureModel.setInc(order.equals("inc"));
        OverallStructureModel.setNum(Integer.parseInt(num));
        return OverallStructureModel.overallStructureCount();
    }
    @RequestMapping("overallcompany.do")
    @ResponseBody
    public HashMap overallcompany(@RequestParam String year, @RequestParam String month,@RequestParam String order,@RequestParam String choice, @RequestParam String addition) throws Exception {
        OverallCompanyModel.setY(Integer.parseInt(year));
        OverallCompanyModel.setM(Integer.parseInt(month));
        OverallCompanyModel.setInc(order.equals("inc"));
        OverallCompanyModel.setbSum(choice.equals("sum"));
        OverallCompanyModel.setAddtion(addition);
        return OverallCompanyModel.overallCompanyCount();
    }
}
