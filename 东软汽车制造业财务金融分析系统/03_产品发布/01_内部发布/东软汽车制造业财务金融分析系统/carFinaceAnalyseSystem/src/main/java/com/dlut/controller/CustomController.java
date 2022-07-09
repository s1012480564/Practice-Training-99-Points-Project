package com.dlut.controller;

import com.dlut.model.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;

@Controller
public class CustomController {
    @RequestMapping("customtrend.do")
    @ResponseBody
    public HashMap customtrend(@RequestParam String year1, @RequestParam String month1,@RequestParam String year2,@RequestParam String month2, @RequestParam String choice, @RequestParam String addition) throws Exception {
        CustomTrendModel.setYM(Integer.parseInt(year1),Integer.parseInt(month1),Integer.parseInt(year2),Integer.parseInt(month2));
        CustomTrendModel.setbSum(choice.equals("sum"));
        CustomTrendModel.setAddtion(addition);
        return CustomTrendModel.customTrendCount();
    }
    @RequestMapping("customstructure.do")
    @ResponseBody
    public HashMap customstructure(@RequestParam String year1, @RequestParam String month1,@RequestParam String year2,@RequestParam String month2, @RequestParam String choice, @RequestParam String addition) throws Exception {
        CustomStructureModel.setYM(Integer.parseInt(year1),Integer.parseInt(month1),Integer.parseInt(year2),Integer.parseInt(month2));
        CustomStructureModel.setbSum(choice.equals("sum"));
        CustomStructureModel.setAddtion(addition);
        return CustomStructureModel.customStructureCount();
    }
    @RequestMapping("customcompany.do")
    @ResponseBody
    public HashMap customcompany(@RequestParam String year1, @RequestParam String month1,@RequestParam String year2,@RequestParam String month2,@RequestParam String order,@RequestParam String layer,@RequestParam String num) throws Exception {
        CustomCompanyModel.setYM(Integer.parseInt(year1),Integer.parseInt(month1),Integer.parseInt(year2),Integer.parseInt(month2));
        CustomCompanyModel.setInc(order.equals("inc"));
        CustomCompanyModel.setbLayer(layer.equals("leaf"));
        CustomCompanyModel.setNum(Integer.parseInt(num));
        return CustomCompanyModel.customCompanyCount();
    }
    @RequestMapping("custommultitarget.do")
    @ResponseBody
    public HashMap custommultitarget(@RequestParam String year1, @RequestParam String month1,@RequestParam String year2,@RequestParam String month2,@RequestParam String target,@RequestParam String targetCmp) throws Exception {
        CustomMultiTargetModel.setYM(Integer.parseInt(year1),Integer.parseInt(month1),Integer.parseInt(year2),Integer.parseInt(month2));
        CustomMultiTargetModel.setTarget(target);
        CustomMultiTargetModel.setTargetCmp(targetCmp);
        return CustomMultiTargetModel.customMultiTargetCount();
    }
}
