package io.mycat.config.loader.zkprocess.parse.entryparse.rule.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import io.mycat.config.loader.zkprocess.entity.rule.function.Function;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * 进行Function节点的转换
* 源文件名：FunctionJsonParse.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月17日
* 修改作者：liujun
* 修改日期：2016年9月17日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class FunctionJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<Function>> {

    @Override
    public String parseBeanToJson(List<Function> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<Function> parseJsonToBean(String json) {

        // 转换为集合的bean
        Type parseType = new TypeToken<List<Function>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
