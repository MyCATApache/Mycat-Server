package org.opencloudb.config.loader.zookeeper.entitiy;


import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://org.opencloudb/", name = "server") public class Server {

    @XmlElement(required = true) protected System system;

    @XmlElement(required = true) protected List<User> user;

    public System getSystem() {
        return system;
    }

    public void setSystem(System value) {
        this.system = value;
    }

    public List<User> getUser() {
        return user;
    }

    public void setUser(List<User> user) {
        this.user = user;
    }

    @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "system") public static class System
        implements Propertied {
        protected List<Property> property;

        public List<Property> getProperty() {
            if (this.property == null) {
                property = new ArrayList<>();
            }
            return property;
        }

        public void setProperty(List<Property> property) {
            this.property = property;
        }

        @Override public void addProperty(Property property) {
            this.getProperty().add(property);
        }
    }


    @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "user") public static class User
        implements Propertied, Named {

        @XmlAttribute(required = true) protected String name;

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

        @Override public void addProperty(Property property) {
            this.getProperty().add(property);
        }

        @Override public String toString() {
            return "User{" +
                "name='" + name + '\'' +
                ", property=" + property +
                '}';
        }
    }
}



