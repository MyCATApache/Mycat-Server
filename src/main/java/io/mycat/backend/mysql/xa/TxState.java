package io.mycat.backend.mysql.xa;

/**
 * Created by zhangchao on 2016/10/13.
 */
public class TxState {
    /** XA INIT STATUS **/
    public static final int TX_INITIALIZE_STATE = 0;
    /** XA STARTED STATUS **/
    public static final int TX_STARTED_STATE = 1;
    /** XA is prepared **/
    public static final int TX_PREPARED_STATE = 2;
    /** XA is commited **/
    public static final int TX_COMMITED_STATE = 3;
    /** XA is rollbacked **/
    public static final int TX_ROLLBACKED_STATE = 4;
}
