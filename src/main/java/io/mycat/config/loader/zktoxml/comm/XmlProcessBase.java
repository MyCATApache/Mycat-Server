package io.mycat.config.loader.zktoxml.comm;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * xml文件操作转换的类的信息 
* 源文件名：XmlProcessBase.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class XmlProcessBase {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger lOG = LoggerFactory.getLogger(XmlProcessBase.class);

    /**
     * 转换对象
    * @字段说明 jaxContext
    */
    private JAXBContext jaxContext;

    /**
     * 转换的实体对象的class信息
    * @字段说明 parseXmlClass
    */
    @SuppressWarnings("rawtypes")
    public List<Class> parseXmlClass = new ArrayList<Class>();

    /**
     * 添加转换的class信息
    * 方法描述
    * @param parseClass
    * @创建日期 2016年9月15日
    */
    @SuppressWarnings("rawtypes")
    public void addParseClass(Class parseClass) {
        this.parseXmlClass.add(parseClass);
    }

    /**
     * 进行jaxb对象的初始化
    * 方法描述
    * @throws JAXBException
    * @创建日期 2016年9月15日
    */
    @SuppressWarnings("rawtypes")
    public void initJaxbClass() throws JAXBException {

        // 将集合转换为数组
        Class[] classArray = new Class[parseXmlClass.size()];
        parseXmlClass.toArray(classArray);

        try {
            this.jaxContext = JAXBContext.newInstance(classArray, Collections.<String, Object> emptyMap());
        } catch (JAXBException e) {
            lOG.error("ZookeeperProcessListen initJaxbClass  error:Exception info:", e);
            throw e;
        }
    }

    /**
     * 将bean序列化为xml对象信息
    * 方法描述
    * @param user 用户对象
    * @param inputPath
    * @param name 当前的转换的信息
    * @创建日期 2016年9月15日
    */
    public void parseToXml(Object user, String inputPath, String name) throws IOException {
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                    String.format("<!DOCTYPE mycat:%1$s SYSTEM \"%1$s.dtd\">", name));

            Path path = Paths.get(inputPath);

            OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            marshaller.marshal(user, out);

        } catch (JAXBException e) {
            lOG.error("ZookeeperProcessListen parseToXml  error:Exception info:", e);
        } catch (IOException e) {
            lOG.error("ZookeeperProcessListen parseToXml  error:Exception info:", e);
        }

    }

}
