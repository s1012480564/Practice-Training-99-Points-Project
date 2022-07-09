package com.dlut.controller;

import com.dlut.model.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;

@Controller
public class IndexController {
    @RequestMapping("indexamount.do")
    @ResponseBody
    public HashMap indexamount(@RequestParam String year, @RequestParam String month) throws Exception {
        IndexAmountModel.setY(Integer.parseInt(year));
        IndexAmountModel.setM(Integer.parseInt(month));
        return IndexAmountModel.indexAmountCount();
    }
    @RequestMapping("indexasset.do")
    @ResponseBody
    public HashMap indexasset(@RequestParam String year, @RequestParam String month) throws Exception {
        IndexAssetModel.setY(Integer.parseInt(year));
        IndexAssetModel.setM(Integer.parseInt(month));
        return IndexAssetModel.indexAssetCount();
    }
    @RequestMapping("indexprofit.do")
    @ResponseBody
    public HashMap indexprofit(@RequestParam String year, @RequestParam String month) throws Exception {
        IndexProfitModel.setY(Integer.parseInt(year));
        IndexProfitModel.setM(Integer.parseInt(month));
        return IndexProfitModel.indexProfitCount();
    }
    @RequestMapping("indexfund.do")
    @ResponseBody
    public HashMap indexfund(@RequestParam String year, @RequestParam String month) throws Exception {
        IndexFundModel.setY(Integer.parseInt(year));
        IndexFundModel.setM(Integer.parseInt(month));
        return IndexFundModel.indexFundCount();
    }
    @RequestMapping("indexfinance.do")
    @ResponseBody
    public HashMap indexfinance(@RequestParam String year, @RequestParam String month) throws Exception {
        IndexFinanceModel.setY(Integer.parseInt(year));
        IndexFinanceModel.setM(Integer.parseInt(month));
        return IndexFinanceModel.indexFinanceCount();
    }
    @RequestMapping("indexassetcompose.do")
    @ResponseBody
    public HashMap indexassetcompose(@RequestParam String year, @RequestParam String month) throws Exception {
        IndexAssetComposeModel.setY(Integer.parseInt(year));
        IndexAssetComposeModel.setM(Integer.parseInt(month));
        return IndexAssetComposeModel.indexAssetComposeCount();
    }
    @RequestMapping("indexcost.do")
    @ResponseBody
    public HashMap indexcost(@RequestParam String year, @RequestParam String month) throws Exception {
        IndexCostModel.setY(Integer.parseInt(year));
        IndexCostModel.setM(Integer.parseInt(month));
        return IndexCostModel.indexCostCount();
    }
}
