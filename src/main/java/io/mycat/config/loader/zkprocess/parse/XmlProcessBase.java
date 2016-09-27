package io.mycat.config.loader.zkprocess.parse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

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
     * 反序列化xml文件的对象
    * @字段说明 unmarshaller
    */
    private Unmarshaller unmarshaller;

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

        // 创建解反序化对象
        unmarshaller = jaxContext.createUnmarshaller();
    }

    /**
     * 默认将bean序列化为xml对象信息并写入文件
    * 方法描述
    * @param user 用户对象
    * @param inputPath
    * @param name 当前的转换xml的dtd文件的信息
    * @创建日期 2016年9月15日
    */
    public void baseParseAndWriteToXml(Object user, String inputPath, String name) throws IOException {
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            if (null != name) {
                marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                        String.format("<!DOCTYPE mycat:%1$s SYSTEM \"%1$s.dtd\">", name));
            }

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

    /**
     * 默认将bean序列化为xml对象信息并写入文件
     * 方法描述
     * @param user 用户对象
     * @param inputPath
     * @param name 当前的转换xml的dtd文件的信息
     * @创建日期 2016年9月15日
     */
    @SuppressWarnings("restriction")
    public void baseParseAndWriteToXml(Object user, String inputPath, String name, Map<String, Object> map)
            throws IOException {
        try {
            Marshaller marshaller = this.jaxContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            if (null != name) {
                marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                        String.format("<!DOCTYPE mycat:%1$s SYSTEM \"%1$s.dtd\">", name));
            }

            if (null != map && !map.isEmpty()) {
                for (Entry<String, Object> entry : map.entrySet()) {
                    marshaller.setProperty(entry.getKey(), entry.getValue());
                }
            }

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

    /**
     * 默认转换将指定的xml转化为
    * 方法描述
    * @param inputStream
    * @param fileName
    * @return
    * @throws JAXBException
    * @throws XMLStreamException
    * @创建日期 2016年9月16日
    */
    public Object baseParseXmlToBean(String fileName) throws JAXBException, XMLStreamException {
        // 搜索当前转化的文件
        InputStream inputStream = XmlProcessBase.class.getResourceAsStream(fileName);

        // 如果能够搜索到文件
        if (inputStream != null) {
            // 进行文件反序列化信息
            XMLInputFactory xif = XMLInputFactory.newFactory();
            xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            XMLStreamReader xmlRead = xif.createXMLStreamReader(new StreamSource(inputStream));

            return unmarshaller.unmarshal(xmlRead);
        }

        return null;
    }

}
