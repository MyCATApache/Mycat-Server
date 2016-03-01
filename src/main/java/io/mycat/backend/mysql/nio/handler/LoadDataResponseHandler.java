package io.mycat.backend.mysql.nio.handler;

import io.mycat.backend.BackendConnection;

/**
 * Created by nange on 2015/3/31.
 */
public interface LoadDataResponseHandler
{
    /**
     * 收到请求发送文件数据包的响应处理
     */
    void requestDataResponse(byte[] row, BackendConnection conn);
}
