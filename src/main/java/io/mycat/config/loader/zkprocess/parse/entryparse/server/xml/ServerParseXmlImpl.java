package io.mycat.config.loader.zkprocess.parse.entryparse.server.xml;

import java.io.IOException;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.zkprocess.entity.Server;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;

/**
 * schema.xml与javabean之间的转化
* 源文件名：ServerParseXmlImpl.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月16日
* 修改作者：liujun
* 修改日期：2016年9月16日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ServerParseXmlImpl implements ParseXmlServiceInf<Server> {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger lOG = LoggerFactory.getLogger(ServerParseXmlImpl.class);

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
    public ServerParseXmlImpl(XmlProcessBase parseBase) {

        this.parseBean = parseBase;
        // 添加xml的转换的实体类信息
        parseBean.addParseClass(Server.class);
    }

    @Override
    public Server parseXmlToBean(String path) {

        Server server = null;

        try {
            server = (Server) this.parseBean.baseParseXmlToBean(path);
        } catch (JAXBException e) {
            e.printStackTrace();
            lOG.error("ServerParseXmlImpl parseXmlToBean JAXBException", e);
        } catch (XMLStreamException e) {
            e.printStackTrace();
            lOG.error("ServerParseXmlImpl parseXmlToBean XMLStreamException", e);
        }

        return server;
    }

    @Override
    public void parseToXmlWrite(Server data, String outputFile, String dataName) {
        try {
            this.parseBean.baseParseAndWriteToXml(data, outputFile, dataName);
        } catch (IOException e) {
            e.printStackTrace();
            lOG.error("ServerParseXmlImpl parseToXmlWrite IOException", e);
        }
    }

}
