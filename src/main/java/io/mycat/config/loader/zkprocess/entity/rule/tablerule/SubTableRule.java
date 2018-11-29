package io.mycat.config.loader.zkprocess.entity.rule.tablerule;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

/**
 * @author liunan  by 2018/8/29
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "subTableRule", propOrder = { "columns", "algorithm" })
public class SubTableRule {
    protected String columns;
    protected String algorithm;

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }
}
