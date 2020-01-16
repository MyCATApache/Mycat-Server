package io.mycat.backend.mysql.nio.handler;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.jdbc.JDBCDatasource;
import io.mycat.cache.CachePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

public class JDBCFetchStoreNodeOfChildTableHandler {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(JDBCFetchStoreNodeOfChildTableHandler.class);

    public String execute(String schema, String sql, ArrayList<String> dataNodes) {

        String key = schema + ":" + sql;
        CachePool cache = MycatServer.getInstance().getCacheService()
                .getCachePool("ER_SQL2PARENTID");
        String result = (String) cache.get(key);
        if (result != null) {
            return result;
        }
        Map<String, PhysicalDBNode> dbNodeMap = MycatServer.getInstance().getConfig().getDataNodes();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("find child node with sql:" + sql);
        }
        for (String dn : dataNodes) {

            PhysicalDBNode physicalDBNode = dbNodeMap.get(dn);
            PhysicalDatasource physicalDatasource = physicalDBNode.getDbPool().getSource();
            if(physicalDatasource instanceof JDBCDatasource) {
                JDBCDatasource jdbcDatasource = (JDBCDatasource) physicalDatasource;
                Connection con = null;
                PreparedStatement pstmt = null;
                ResultSet rs = null;
                try {
                    con = jdbcDatasource.getDruidConnection();
                    String useDB = "use " + physicalDBNode.getDatabase() + ";";
                    pstmt = con.prepareStatement(useDB);
                    pstmt.execute();
                    pstmt = con.prepareStatement(sql);
                    rs = pstmt.executeQuery();
                    if (rs.next()) {
                        return dn;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (con != null) {
                            con.close();
                        }
                        if (pstmt != null) {
                            pstmt.close();
                        }
                        if (rs != null) {
                            rs.close();
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        LOGGER.error("can't find (root) parent sharding node for sql:"+ sql);
        return null;
    }
}
