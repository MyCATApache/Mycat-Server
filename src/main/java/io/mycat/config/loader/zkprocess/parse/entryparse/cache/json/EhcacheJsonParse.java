package io.mycat.config.loader.zkprocess.parse.entryparse.cache.json;

import io.mycat.config.loader.zkprocess.entity.cache.Ehcache;
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
public class EhcacheJsonParse extends JsonProcessBase implements ParseJsonServiceInf<Ehcache> {

    @Override
    public String parseBeanToJson(Ehcache t) {
        return this.toJsonFromBean(t);
    }

    @Override
    public Ehcache parseJsonToBean(String json) {
        return this.toBeanformJson(json, Ehcache.class);
    }

}
