package io.mycat.config.loader.zkprocess.parse.entryparse.server.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import io.mycat.config.loader.zkprocess.entity.server.user.User;
import io.mycat.config.loader.zkprocess.parse.JsonProcessBase;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;

/**
 * 进行datahost节点的转换
* 源文件名：DataHostJsonParse.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月17日
* 修改作者：liujun
* 修改日期：2016年9月17日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class UserJsonParse extends JsonProcessBase implements ParseJsonServiceInf<List<User>> {

    @Override
    public String parseBeanToJson(List<User> t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public List<User> parseJsonToBean(String json) {

        // 转换为集合的bean
        Type parseType = new TypeToken<List<User>>() {
        }.getType();

        return this.toBeanformJson(json, parseType);
    }

}
