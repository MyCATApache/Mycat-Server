package io.mycat.config.loader.zkprocess.entity;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import io.mycat.config.loader.zkprocess.entity.schema.datahost.DataHost;
import io.mycat.config.loader.zkprocess.entity.schema.datanode.DataNode;
import io.mycat.config.loader.zkprocess.entity.schema.schema.Schema;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://io.mycat/", name = "schema")
public class Schemas {
    /**
     * 配制的逻辑表信息
    * @字段说明 schema
    */
    private List<Schema> schema;

    /**
     * 配制的表对应的数据库信息
    * @字段说明 dataNode
    */
    private List<DataNode> dataNode;

    /**
     * 用于指定数据信息
    * @字段说明 dataHost
    */
    private List<DataHost> dataHost;

    public List<Schema> getSchema() {
        if (this.schema == null) {
            schema = new ArrayList<>();
        }
        return schema;
    }

    public void setSchema(List<Schema> schema) {
        this.schema = schema;
    }

    public List<DataNode> getDataNode() {
        if (this.dataNode == null) {
            dataNode = new ArrayList<>();
        }
        return dataNode;
    }

    public void setDataNode(List<DataNode> dataNode) {
        this.dataNode = dataNode;
    }

    public List<DataHost> getDataHost() {
        if (this.dataHost == null) {
            dataHost = new ArrayList<>();
        }
        return dataHost;
    }

    public void setDataHost(List<DataHost> dataHost) {
        this.dataHost = dataHost;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Schemas [schema=");
        builder.append(schema);
        builder.append(", dataNode=");
        builder.append(dataNode);
        builder.append(", dataHost=");
        builder.append(dataHost);
        builder.append("]");
        return builder.toString();
    }

}
