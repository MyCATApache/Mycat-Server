package io.mycat.config.loader.zkprocess.comm;

/**
 * 通过接口
 * @author liujun
 *
 * @date 2015年2月4日
 * @vsersion 0.0.1
 */
public interface NotiflyService {

    /**
     * 进行通知接口
     * @throws Exception 异常操作
     * @return true 通知更新成功，false ，更新失败
     */
    public boolean notiflyProcess() throws Exception;

}
