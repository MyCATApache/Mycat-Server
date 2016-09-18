package io.mycat.config.loader.zkprocess.entry.server.user;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entry.Named;
import io.mycat.config.loader.zkprocess.entry.Propertied;
import io.mycat.config.loader.zkprocess.entry.Property;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "user")
public class User implements Propertied, Named {

    @XmlAttribute(required = true)
    protected String name;

    protected List<Property> property;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Property> getProperty() {
        if (this.property == null) {
            property = new ArrayList<>();
        }
        return property;
    }

    public void setProperty(List<Property> property) {
        this.property = property;
    }

    @Override
    public void addProperty(Property property) {
        this.getProperty().add(property);
    }

    @Override
    public String toString() {
        return "User{" + "name='" + name + '\'' + ", property=" + property + '}';
    }

}
