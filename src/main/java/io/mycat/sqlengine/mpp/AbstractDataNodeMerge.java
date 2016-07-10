package io.mycat.sqlengine.mpp;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.nio.handler.MultiNodeQueryHandler;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultset;
import io.mycat.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zagnix on 2016/7/6.
 */
public abstract class AbstractDataNodeMerge implements Runnable{


    private static Logger LOGGER = Logger.getLogger(AbstractDataNodeMerge.class);
    /**
     *row 有多少col
     */
    protected int fieldCount;

    /**
     * 本次select的路由缓存集
     */
    protected final RouteResultset rrs;
    /**
     * 夸分片处理handler
     */
    protected MultiNodeQueryHandler multiQueryHandler = null;
    /**
     * 分片结束包
     */
    public PackWraper END_FLAG_PACK = new PackWraper();


    /**
     * 是否执行流式结果集输出
     */

    protected boolean isStreamOutputResult = false;

    /**
     * rowData缓存队列
     */
    protected BlockingQueue<PackWraper> packs = new LinkedBlockingQueue<PackWraper>();

    /**
     * 标志业务线程是否启动了？
     */
    protected final AtomicBoolean running = new AtomicBoolean(false);

    public AbstractDataNodeMerge(MultiNodeQueryHandler handler,RouteResultset rrs){
        this.rrs = rrs;
        this.multiQueryHandler = handler;
    }

    public boolean isStreamOutputResult() {
        return isStreamOutputResult;
    }

    public void setStreamOutputResult(boolean streamOutputResult) {
        isStreamOutputResult = streamOutputResult;
    }

    /**
     * Add a row pack, and may be wake up a business thread to work if not running.
     * @param pack row pack
     * @return true wake up a business thread, otherwise false
     *
     * @author Uncle-pan
     * @since 2016-03-23
     */
    protected final boolean addPack(final PackWraper pack){
        packs.add(pack);
        if(running.get()){
            return false;
        }
        final MycatServer server = MycatServer.getInstance();
        server.getBusinessExecutor().execute(this);
        return true;
    }

    /**
     * 处理新进来每个row数据，通过PackWraper进行封装，
     * 投递到队列中进行后续处理即可。
     * process new record (mysql binary data),if data can output to client
     * ,return true
     *
     * @param dataNode
     *            DN's name (data from this dataNode)
     * @param rowData
     *            raw data
     */
    public boolean onNewRecord(String dataNode, byte[] rowData) {
        final PackWraper data = new PackWraper();
        data.dataNode = dataNode;
        data.rowData = rowData;
        addPack(data);

        return false;
    }


    /**
     * 将Map对应的col字段集，返回row中对应的index数组
     * @param columns
     * @param toIndexMap
     * @return
     */
    protected static int[] toColumnIndex(String[] columns, Map<String, ColMeta> toIndexMap) {
        int[] result = new int[columns.length];
        ColMeta curColMeta;
        for (int i = 0; i < columns.length; i++) {
            curColMeta = toIndexMap.get(columns[i].toUpperCase());
            if (curColMeta == null) {
                throw new IllegalArgumentException(
                        "all columns in group by clause should be in the selected column list.!"
                                + columns[i]);
            }
            result[i] = curColMeta.colIndex;
        }
        return result;
    }

    @Override
    public abstract void run();

    public abstract void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldCount) throws IOException;

    public void outputMergeResult(NonBlockingSession session, byte[] eof) {
        addPack(END_FLAG_PACK);
    }

    public RouteResultset getRrs() {
        return this.rrs;
    }

    /**
     * 做最后的结果集输出
     * @return (最多i*(offset+size)行数据)
     */
    public abstract List<RowDataPacket> getResults(byte[] eof);
    public abstract void clear();

}
