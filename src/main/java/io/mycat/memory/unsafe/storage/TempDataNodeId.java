package io.mycat.memory.unsafe.storage;

/**
 * Created by zagnix on 2016/6/3.
 */
public class TempDataNodeId extends ConnectionId {

    private String uuid;

    public TempDataNodeId(String uuid) {
        super();
        this.name = uuid;
        this.uuid = uuid;
    }

    @Override
    public String getBlockName() {
        return "temp_local_" + uuid;
    }
}
