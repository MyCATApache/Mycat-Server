package io.mycat.config.loader.zkprocess.parse.entryparse.cache.xml;

import io.mycat.config.loader.zkprocess.entity.cache.Ehcache;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * rule.xml与javabean之间的转化
* 源文件名：SchemasParseXmlImpl.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月16日
* 修改作者：liujun
* 修改日期：2016年9月16日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class EhcacheParseXmlImpl implements ParseXmlServiceInf<Ehcache> {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger lOG = LoggerFactory.getLogger(EhcacheParseXmlImpl.class);

    /**
     * 基本的转换类的信息
    * @字段说明 parseBean
    */
    private XmlProcessBase parseBean;

    /**
     * 转换的类的信息
    * 构造方法
    * @param parseBase
    */
    public EhcacheParseXmlImpl(XmlProcessBase parseBase) {

        this.parseBean = parseBase;
        // 添加xml的转换的实体类信息
        parseBean.addParseClass(Ehcache.class);
    }

    @Override
    public Ehcache parseXmlToBean(String path) {

        Ehcache schema = null;

        try {
            schema = (Ehcache) this.parseBean.baseParseXmlToBean(path);
        } catch (JAXBException e) {
            e.printStackTrace();
            lOG.error("EhcacheParseXmlImpl parseXmlToBean JAXBException", e);
        } catch (XMLStreamException e) {
            e.printStackTrace();
            lOG.error("EhcacheParseXmlImpl parseXmlToBean XMLStreamException", e);
        }

        return schema;
    }

    @Override
    public void parseToXmlWrite(Ehcache data, String outputFile, String dataName) {
        try {
            // 设置
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put(Marshaller.JAXB_NO_NAMESPACE_SCHEMA_LOCATION, "ehcache.xsd");
            
            this.parseBean.baseParseAndWriteToXml(data, outputFile, dataName, paramMap);
        } catch (IOException e) {
            e.printStackTrace();
            lOG.error("EhcacheParseXmlImpl parseToXmlWrite IOException", e);
        }
    }

}


