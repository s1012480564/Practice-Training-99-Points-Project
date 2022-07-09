package com.dlut.controller;

import com.dlut.model.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;

@Controller
public class TargetController {
    @RequestMapping("retrieve.targetcode.do")
    @ResponseBody
    public HashMap codeRetrieve(@RequestParam String code) throws Exception {
        return TargetCodeModel.retrieve(code);
    }
    @RequestMapping("create.targetcode.do")
    @ResponseBody
    public boolean codeCreate(@RequestParam String code,@RequestParam String content) throws Exception {
        if(content.equals("")) content="无";
        return TargetCodeModel.create(code,content);
    }
    @RequestMapping("delete.targetcode.do")
    @ResponseBody
    public HashMap codeDelete(@RequestParam String codes) throws Exception {
        TargetCodeModel.setCodes(codes);
        return TargetCodeModel.delete();
    }
    @RequestMapping("retrieve.targetname.do")
    @ResponseBody
    public HashMap nameRetrieve(@RequestParam String name) throws Exception {
        return TargetNameModel.retrieve(name);
    }
    @RequestMapping("update.targetname.do")
    @ResponseBody
    public boolean nameUpdate(@RequestParam String code,@RequestParam String name) throws Exception {
        return TargetNameModel.update(code,name);
    }
    @RequestMapping("retrieve.targetstructure.do")
    @ResponseBody
    public HashMap structureRetrieve(@RequestParam String code) throws Exception {
        return TargetStructureModel.retrieve(code);
    }
    @RequestMapping("create.targetstructure.do")
    @ResponseBody
    public HashMap structureCreate(@RequestParam String code,@RequestParam String subcode) throws Exception {
        return TargetStructureModel.create(code,subcode);
    }
    @RequestMapping("delete.targetstructure.do")
    @ResponseBody
    public Boolean structureDelete(@RequestParam String code,@RequestParam String subcode) throws Exception {
        return TargetStructureModel.delete(code,subcode);
    }
    @RequestMapping("retrieve.targetrelation.do")
    @ResponseBody
    public HashMap relationRetrieve(@RequestParam String code) throws Exception {
        return TargetRelationModel.retrieve(code);
    }
    @RequestMapping("create.targetrelation.do")
    @ResponseBody
    public HashMap relationCreate(@RequestParam String code,@RequestParam String subcode) throws Exception {
        return TargetRelationModel.create(code,subcode);
    }
    @RequestMapping("delete.targetrelation.do")
    @ResponseBody
    public Boolean relationDelete(@RequestParam String code,@RequestParam String subcode) throws Exception {
        return TargetRelationModel.delete(code,subcode);
    }
}
