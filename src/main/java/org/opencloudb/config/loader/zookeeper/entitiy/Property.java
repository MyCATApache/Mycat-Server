package org.opencloudb.config.loader.zookeeper.entitiy;

import javax.xml.bind.annotation.*;
import java.util.Objects;

@XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "Property") public class Property
    implements Named {

    @XmlValue protected String value;
    @XmlAttribute(name = "name") protected String name;

    public String getValue() {
        return value;
    }

    public Property setValue(String value) {
        this.value = value;
        return this;
    }

    public String getName() {
        return name;
    }

    public Property setName(String value) {
        this.name = value;
        return this;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Property property = (Property) o;
        return value.equals(property.value) && name.equals(property.name);
    }

    @Override public int hashCode() {
        return Objects.hash(value, name);
    }

    @Override public String toString() {
        return "Property{" +
            "value='" + value + '\'' +
            ", name='" + name + '\'' +
            '}';
    }
}
