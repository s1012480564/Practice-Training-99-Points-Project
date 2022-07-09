package com.dlut.controller;

import com.dlut.model.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;

@Controller
public class ProfitabilityController {
    @RequestMapping("profitabilitytotal.do")
    @ResponseBody
    public HashMap overalleconomy(@RequestParam String year, @RequestParam String month) throws Exception {
        ProfitabilityTotalModel.setY(Integer.parseInt(year));
        ProfitabilityTotalModel.setM(Integer.parseInt(month));
        return ProfitabilityTotalModel.profitabilityTotalCount();
    }
    @RequestMapping("profitabilitytrend.do")
    @ResponseBody
    public HashMap profitabilitytrend(@RequestParam String year, @RequestParam String month,@RequestParam String choice, @RequestParam String addition) throws Exception {
        ProfitabilityTrendModel.setY(Integer.parseInt(year));
        ProfitabilityTrendModel.setM(Integer.parseInt(month));
        ProfitabilityTrendModel.setbSum(choice.equals("sum"));
        ProfitabilityTrendModel.setAddtion(addition);
        return ProfitabilityTrendModel.profitabilityTrendCount();
    }
    @RequestMapping("profitabilitycompanyensemble.do")
    @ResponseBody
    public HashMap profitabilitycompanyensemble(@RequestParam String year, @RequestParam String month) throws Exception {
        ProfitabilityCompanyEnsembleModel.setY(Integer.parseInt(year));
        ProfitabilityCompanyEnsembleModel.setM(Integer.parseInt(month));
        return ProfitabilityCompanyEnsembleModel.profitabilityCompanyEnsembleCount();
    }
    @RequestMapping("profitabilitycompanypart.do")
    @ResponseBody
    public HashMap profitabilitycompanypart(@RequestParam String year, @RequestParam String month,@RequestParam String order,@RequestParam String choice, @RequestParam String layer,@RequestParam String addition,@RequestParam String num) throws Exception {
        ProfitabilityCompanyPartModel.setY(Integer.parseInt(year));
        ProfitabilityCompanyPartModel.setM(Integer.parseInt(month));
        ProfitabilityCompanyPartModel.setInc(order.equals("inc"));
        ProfitabilityCompanyPartModel.setbSum(choice.equals("sum"));
        ProfitabilityCompanyPartModel.setbLayer(layer.equals("leaf"));
        ProfitabilityCompanyPartModel.setAddtion(addition);
        ProfitabilityCompanyPartModel.setNum(Integer.parseInt(num));
        return ProfitabilityCompanyPartModel.profitabilityCompanyPartCount();
    }
}
