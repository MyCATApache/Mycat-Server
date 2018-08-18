package io.mycat.config.loader.zkprocess.parse.entryparse.rule.xml;

import io.mycat.config.loader.zkprocess.entity.Rules;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;

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
public class RuleParseXmlImpl implements ParseXmlServiceInf<Rules> {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger lOG = LoggerFactory.getLogger(RuleParseXmlImpl.class);

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
    public RuleParseXmlImpl(XmlProcessBase parseBase) {

        this.parseBean = parseBase;
        // 添加xml的转换的实体类信息
        parseBean.addParseClass(Rules.class);
    }

    @Override
    public Rules parseXmlToBean(String path) {

        Rules schema = null;

        try {
            schema = (Rules) this.parseBean.baseParseXmlToBean(path);
        } catch (JAXBException e) {
            e.printStackTrace();
            lOG.error("RulesParseXmlImpl parseXmlToBean JAXBException", e);
        } catch (XMLStreamException e) {
            e.printStackTrace();
            lOG.error("RulesParseXmlImpl parseXmlToBean XMLStreamException", e);
        }

        return schema;
    }

    @Override
    public void parseToXmlWrite(Rules data, String outputFile, String dataName) {
        try {
            this.parseBean.baseParseAndWriteToXml(data, outputFile, dataName);
        } catch (IOException e) {
            e.printStackTrace();
            lOG.error("RulesParseXmlImpl parseToXmlWrite IOException", e);
        }
    }

}
