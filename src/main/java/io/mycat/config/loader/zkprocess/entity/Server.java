package io.mycat.config.loader.zkprocess.entity;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import io.mycat.config.loader.zkprocess.entity.server.System;
import io.mycat.config.loader.zkprocess.entity.server.user.User;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://io.mycat/", name = "server")
public class Server {

    @XmlElement(required = true)
    protected System system;

    @XmlElement(required = true)
    protected List<User> user;

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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Server [system=");
        builder.append(system);
        builder.append(", user=");
        builder.append(user);
        builder.append("]");
        return builder.toString();
    }

}
