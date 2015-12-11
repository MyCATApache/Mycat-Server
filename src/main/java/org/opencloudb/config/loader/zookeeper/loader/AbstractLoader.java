package org.opencloudb.config.loader.zookeeper.loader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


public abstract class AbstractLoader {
    protected final CuratorFramework curator;

    public AbstractLoader(CuratorFramework curator) {
        this.curator = curator;
    }

    public abstract JSONObject takeConfig(String path) throws Exception;

    /**
     * get path and their child data then convert it to {@link JSONObject}.
     */
    protected JSONObject takeData(String path) throws Exception {
        List<String> childNames = getChildNames(path);

        //terminal condition,have not child node.
        if (childNames.isEmpty()) {
            return new JSONObject(getDataToString(path));
        } else {
            JSONObject jsonObject = new JSONObject(getDataToString(path));
            for (String childName : childNames) {
                final String childPath = ZKPaths.makePath(path, childName);
                jsonObject.put(childName, takeData(childPath));
            }
            return jsonObject;
        }
    }

    /**
     * get data from zookeeper and convert to string with check not null.
     */
    protected String getDataToString(String path) throws Exception {
        byte[] raw = curator.getData().forPath(path);
        checkNotNull(raw, "data of " + path + " must be not null!");

        return byteToString(raw);
    }

    /**
     * get child node name list based on path from zookeeper.
     */
    protected List<String> getChildNames(String path) throws Exception {
        return curator.getChildren().forPath(path);
    }

    /**
     * raw byte data to string
     */
    protected String byteToString(byte[] raw) {
        //return empty json {}.
        if (raw.length == 0) {
            return "{}";
        }
        return new String(raw, StandardCharsets.UTF_8);
    }

}
