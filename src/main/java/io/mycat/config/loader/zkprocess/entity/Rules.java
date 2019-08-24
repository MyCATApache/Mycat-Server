package io.mycat.config.loader.zkprocess.entity;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import io.mycat.config.loader.zkprocess.entity.rule.function.Function;
import io.mycat.config.loader.zkprocess.entity.rule.tablerule.TableRule;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://io.mycat/", name = "rule")
public class Rules {

    /**
     * 表的路由配制信息
    * @字段说明 tableRule
    */
    protected List<TableRule> tableRule;

    /**
     * 指定的方法信息
    * @字段说明 function
    */
    protected List<Function> function;

    public List<TableRule> getTableRule() {
        if (this.tableRule == null) {
            tableRule = new ArrayList<>();
        }
        return tableRule;
    }

    public void setTableRule(List<TableRule> tableRule) {
        this.tableRule = tableRule;
    }

    public List<Function> getFunction() {
        if (this.function == null) {
            function = new ArrayList<>();
        }
        return function;
    }

    public void setFunction(List<Function> function) {
        this.function = function;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Rules [tableRule=");
        builder.append(tableRule);
        builder.append(", function=");
        builder.append(function);
        builder.append("]");
        return builder.toString();
    }


}
