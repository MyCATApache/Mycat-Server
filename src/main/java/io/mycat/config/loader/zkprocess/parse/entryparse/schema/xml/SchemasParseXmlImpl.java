package io.mycat.config.loader.zkprocess.parse.entryparse.schema.xml;

import java.io.IOException;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.zkprocess.entity.Schemas;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;

/**
 * schema.xml与javabean之间的转化
* 源文件名：SchemasParseXmlImpl.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月16日
* 修改作者：liujun
* 修改日期：2016年9月16日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class SchemasParseXmlImpl implements ParseXmlServiceInf<Schemas> {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger lOG = LoggerFactory.getLogger(SchemasParseXmlImpl.class);

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
    public SchemasParseXmlImpl(XmlProcessBase parseBase) {

        this.parseBean = parseBase;
        // 添加xml的转换的实体类信息
        parseBean.addParseClass(Schemas.class);
    }

    @Override
    public Schemas parseXmlToBean(String path) {

        Schemas schema = null;

        try {
            schema = (Schemas) this.parseBean.baseParseXmlToBean(path);
        } catch (JAXBException e) {
            e.printStackTrace();
            lOG.error("SchemasParseXmlImpl parseXmlToBean JAXBException", e);
        } catch (XMLStreamException e) {
            e.printStackTrace();
            lOG.error("SchemasParseXmlImpl parseXmlToBean XMLStreamException", e);
        }

        return schema;
    }

    @Override
    public void parseToXmlWrite(Schemas data, String outputFile, String dataName) {
        try {
            this.parseBean.baseParseAndWriteToXml(data, outputFile, dataName);
        } catch (IOException e) {
            e.printStackTrace();
            lOG.error("SchemasParseXmlImpl parseToXmlWrite IOException", e);
        }
    }

}
