package org.opencloudb.config.loader.zookeeper;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opencloudb.config.loader.zookeeper.entitiy.Propertied;
import org.opencloudb.config.loader.zookeeper.entitiy.Property;
import org.opencloudb.config.loader.zookeeper.entitiy.Server;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

/**
 * save data loaded from  zookeeper to xml file.
 */
public class ZookeeperSaver {
    private final static Joiner commaJonier = Joiner.on(",");
    private JAXBContext jaxbContext;

    public ZookeeperSaver() throws JAXBException {
        super();
        this.jaxbContext =
            JAXBContext.newInstance(org.opencloudb.config.loader.zookeeper.entitiy.Server.class);
    }

    public void saveConfig(JSONObject jsonObject) throws JAXBException, IOException {
        saveServer(jsonObject);

    }

    Server saveServer(JSONObject jsonObject) throws JAXBException, IOException {
        JSONObject myNode = jsonObject.getJSONObject(ZookeeperLoader.NODE_KEY);
        JSONObject users = jsonObject.getJSONObject(ZookeeperLoader.CLUSTER_KEY);
        Preconditions.checkNotNull(myNode);
        Preconditions.checkNotNull(users);

        JSONObject systemParams = myNode.getJSONObject("systemParams");
        JSONObject user = users.getJSONObject("user");

        Server server = new Server();

        //system
        Server.System serverSystem = new Server.System();
        putProperty(systemParams, serverSystem);
        server.setSystem(serverSystem);

        //user
        ArrayList<Server.User> userList = new ArrayList<>();
        if (user != null && user.length() > 0) {
            for (String key : user.keySet()) {
                Server.User serverUser = new Server.User();
                putProperty(user.getJSONObject(key), serverUser);
                userList.add(serverUser);
            }
        }
        server.setUser(userList);

        //save to file
        marshaller(server, "server.xml");
        return server;
    }

    private void marshaller(Object object, String fileName) throws JAXBException, IOException {
        Marshaller marshaller = jaxbContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        Path path = Paths.get(getClass().getResource("/").getFile(), fileName);

        try (OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE,StandardOpenOption.WRITE)) {
            marshaller.marshal(object, out);
        }
    }

    private void putProperty(JSONObject json, Propertied propertied) {
        if (json != null && json.length() > 0) {
            for (String key : json.keySet()) {
                Object obj = json.get(key);
                if (obj instanceof JSONArray) {
                    //join value using ',' .
                    String value = commaJonier.join(json.getJSONArray(key).iterator()).trim();
                    propertied.addProperty(createProperty(key, value));
                    continue;
                }

                propertied.addProperty(createProperty(key, obj.toString()));
            }
        }
    }

    private Property createProperty(String key, String value) {
        return new Property().setName(key).setValue(value);
    }
}
