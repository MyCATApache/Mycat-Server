package io.mycat.config.loader.zkprocess.parse;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.mycat.config.loader.zkprocess.entity.Schemas;
import io.mycat.config.loader.zkprocess.entity.schema.datanode.DataNode;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * json数据与实体类的类的信息 
* 源文件名：XmlProcessBase.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class JsonProcessBase {

    /**
     * 进行消息转换的类的信息
    * @字段说明 gson
    */
    private Gson gson = new Gson();

    /**
     * 进行json字符串化
    * 方法描述
    * @param obj
    * @return
    * @创建日期 2016年9月17日
    */
    public String toJsonFromBean(Object obj) {
        if (null != obj) {
            return gson.toJson(obj);
        }

        return null;
    }

    /**
     * 将json字符串至类，根据指定的类型信息,一般用于集合的转换
    * 方法描述
    * @param json
    * @param typeSchema
    * @return
    * @创建日期 2016年9月17日
    */
    public <T> T toBeanformJson(String json, Type typeSchema) {
        T result = this.gson.fromJson(json, typeSchema);

        return result;
    }

    /**
     * 将json字符串至类，根据指定的类型信息,用于转换单对象实体
     * 方法描述
     * @param <T>
     * @param json
     * @param typeSchema
     * @return
     * @创建日期 2016年9月17日
     */
    public <T> T toBeanformJson(String json, Class<T> classinfo) {
        T result = this.gson.fromJson(json, classinfo);

        return result;
    }

    public static void main(String[] args) {

        DataNode datanode = new DataNode();

        datanode.setDatabase("db1");
        datanode.setDataHost("os1");
        datanode.setName("dn1");

        JsonProcessBase jsonParse = new JsonProcessBase();

        String jsonStr = jsonParse.toJsonFromBean(datanode);

        System.out.println("单对象当前的json:" + jsonStr);

        // 转换实体
        DataNode node = jsonParse.toBeanformJson(jsonStr, DataNode.class);

        System.out.println("单对象:" + node);

        List<DataNode> listNode = new ArrayList<>();

        listNode.add(datanode);
        listNode.add(datanode);

        String listJson = jsonParse.toJsonFromBean(listNode);

        System.out.println("当前集合的json:" + listJson);

        // 转换为集合的bean
        Type parseType = new TypeToken<List<DataNode>>() {
        }.getType();
        List<DataNode> list = jsonParse.toBeanformJson(listJson, parseType);

        System.out.println("集合对象:" + list);

        // 复杂对象的转换
        Schemas schema = new Schemas();
        schema.setDataNode(listNode);

        String jsonMultStr = jsonParse.toJsonFromBean(schema);

        System.out.println("复杂单对象当前的json:" + jsonMultStr);

        // 转换实体
        Schemas nodeMult = jsonParse.toBeanformJson(jsonMultStr, Schemas.class);

        System.out.println("复杂单对象:" + nodeMult);

    }
}
