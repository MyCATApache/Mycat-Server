package io.mycat.config.loader.zkprocess.parse.entryparse.schema.json;

import com.google.gson.reflect.TypeToken;
import io.mycat.config.loader.zkprocess.entity.schema.schema.Schema;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

import java.lang.reflect.Type;
import java.util.List;

/**
 * 进行schema部分的转换
* 源文件名：SchemaJsonParse.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月17日
* 修改作者：liujun
* 修改日期：2016年9月17日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class SchemaJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<Schema>> {

    @Override
    public String parseBeanToJson(List<Schema> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<Schema> parseJsonToBean(String json) {
        // 转换为集合的bean
        Type parseType = new TypeToken<List<Schema>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
