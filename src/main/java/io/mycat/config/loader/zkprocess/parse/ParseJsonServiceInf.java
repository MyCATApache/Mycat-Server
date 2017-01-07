package io.mycat.config.loader.zkprocess.parse;

/**
 * json转化服务 
* 源文件名：JsonParseServiceInf.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月16日
* 修改作者：liujun
* 修改日期：2016年9月16日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public interface ParseJsonServiceInf<T> {

    /**
     * 将对象T转换为json字符串
    * 方法描述
    * @param data
    * @return
    * @创建日期 2016年9月16日
    */
    public String parseBeanToJson(T t);

    /**
     * 将json字符串转换为javabean对象
    * 方法描述
    * @param json
    * @return
    * @创建日期 2016年9月16日
    */
    public T parseJsonToBean(String json);

}
