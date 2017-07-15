package io.mycat.backend.mysql.xa.recovery.impl;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.Deserializer;
import io.mycat.backend.mysql.xa.Serializer;
import io.mycat.backend.mysql.xa.VersionedFile;
import io.mycat.backend.mysql.xa.recovery.DeserialisationException;
import io.mycat.backend.mysql.xa.recovery.Repository;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * XA 协调者日志 文件存储
 *
 * Created by zhangchao on 2016/10/13.
 */
public class FileSystemRepository implements Repository {

    public static final Logger logger = LoggerFactory.getLogger(FileSystemRepository.class);

    /**
     * 文件
     */
    private VersionedFile file;
    private FileChannel rwChannel = null;
    /**
     * 序列化，目前是基于JSON存储
     */
    private Serializer serializer = new Serializer();
    /**
     * 反序列化
     */
    private static Deserializer deserializer = new Deserializer();

    public FileSystemRepository() {
        init();
    }

    @Override
    public void init() {
//        ConfigProperties configProperties = Configuration.getConfigProperties();
//        String baseDir = configProperties.getLogBaseDir();
//        String baseName = configProperties.getLogBaseName();
        MycatConfig mycatconfig = MycatServer.getInstance().getConfig();
        SystemConfig systemConfig = mycatconfig.getSystem();

        String baseDir = systemConfig.getXARecoveryLogBaseDir();
        String baseName = systemConfig.getXARecoveryLogBaseName();

        logger.debug("baseDir " + baseDir);
        logger.debug("baseName " + baseName);

        //Judge whether exist the basedir
        createBaseDir(baseDir);

        file = new VersionedFile(baseDir, baseName, ".log");
    }

    /**
     * 写入 XA 协调者日志
     * tips：该方法写完后，立即刷盘
     * @see #write(CoordinatorLogEntry, boolean)
     *
     * @param id XA 事务id
     * @param coordinatorLogEntry XA 协调者日志
     */
    @Override
    public void put(String id, CoordinatorLogEntry coordinatorLogEntry) {
        try {
            initChannelIfNecessary();
            write(coordinatorLogEntry, true);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private synchronized void initChannelIfNecessary()
            throws FileNotFoundException {
        if (rwChannel == null) {
            rwChannel = file.openNewVersionForNioWriting();
        }
    }

    /**
     * 写入 XA 协调者日志
     *
     * @param coordinatorLogEntry XA 协调者日志
     * @param flushImmediately 是否立即刷盘
     * @throws IOException 当 IO 发生异常时
     */
    private void write(CoordinatorLogEntry coordinatorLogEntry, boolean flushImmediately)
            throws IOException {
        String str = serializer.toJSON(coordinatorLogEntry);
        byte[] buffer = str.getBytes();
        ByteBuffer buff = ByteBuffer.wrap(buffer);
        writeToFile(buff, flushImmediately);
    }

    /**
     * 写入文件
     *
     * @param buff 内容
     * @param force 是否强制刷盘
     * @throws IOException 当 IO 发生异常时
     */
    private synchronized void writeToFile(ByteBuffer buff, boolean force)
            throws IOException {
        rwChannel.write(buff);
        rwChannel.force(force);
    }

    @Override
    public CoordinatorLogEntry get(String coordinatorId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<CoordinatorLogEntry> findAllCommittingCoordinatorLogEntries() {
        throw new UnsupportedOperationException();
    }

    /**
     * 获得全部 XA 协调者日志
     *
     * @return XA 协调者日志
     */
    @Override
    public Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries() {
        FileInputStream fis = null;
        try {
            fis = file.openLastValidVersionForReading();
        } catch (FileNotFoundException firstStart) {
            // the file could not be opened for reading;
            // merely return the default empty vector
        }
        if (fis != null) {
            return readFromInputStream(fis);
        }
        //else
        return Collections.emptyList();
    }

    /**
     * 从 输入流 获得全部 XA 协调者日志
     *
     * @param in 输入流
     * @return XA 协调者日志
     */
    private static Collection<CoordinatorLogEntry> readFromInputStream(InputStream in) {
        Map<String, CoordinatorLogEntry> coordinatorLogEntries = new HashMap<>();
        BufferedReader br = null;
        try {
            InputStreamReader isr = new InputStreamReader(in);
            br = new BufferedReader(isr);
            coordinatorLogEntries = readContent(br);
        } catch (Exception e) {
            logger.error("Error in recover", e);
        } finally {
            closeSilently(br);
        }
        return coordinatorLogEntries.values();
    }

    /**
     * 逐行读取 XA 协调者日志，并进行反序列化，返回XA 协调者日志
     *
     * @param br buffer reader
     * @return XA 协调者日志
     * @throws IOException 当 IO 发生异常时
     */
    private static Map<String, CoordinatorLogEntry> readContent(BufferedReader br)
            throws IOException {
        Map<String, CoordinatorLogEntry> coordinatorLogEntries = new HashMap<>();
        try {
            String line;
            while ((line = br.readLine()) != null) {
                CoordinatorLogEntry coordinatorLogEntry = deserialize(line);
                coordinatorLogEntries.put(coordinatorLogEntry.id, coordinatorLogEntry);
            }
        } catch (EOFException unexpectedEOF) {
            logger.info("Unexpected EOF - logfile not closed properly last time?", unexpectedEOF);
            // merely return what was read so far...
        } catch (StreamCorruptedException unexpectedEOF) {
            logger.info("Unexpected EOF - logfile not closed properly last time?", unexpectedEOF);
            // merely return what was read so far...
        } catch (ObjectStreamException unexpectedEOF) {
            logger.info("Unexpected EOF - logfile not closed properly last time?", unexpectedEOF);
            // merely return what was read so far...
        } catch (DeserialisationException unexpectedEOF) {
            logger.info("Unexpected EOF - logfile not closed properly last time? " + unexpectedEOF);
        }
        return coordinatorLogEntries;
    }

    private static void closeSilently(BufferedReader fis) {
        try {
            if (fis != null)
                fis.close();
        } catch (IOException io) {
            logger.warn("Fail to close logfile after reading - ignoring");
        }
    }

    private static CoordinatorLogEntry deserialize(String line)
            throws DeserialisationException {
        return deserializer.fromJSON(line);
    }

    @Override
    public void close() {
        try {
            closeOutput();
        } catch (Exception e) {
            logger.warn("Error closing file - ignoring", e);
        }
    }

    private void closeOutput() throws IllegalStateException {
        try {
            if (file != null) {
                file.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error closing previous output", e);
        }
    }

    /**
     * 写入 XA 协调者日志集合
     *
     * @param checkpointContent XA 协调者日志集合
     */
    @Override
    public synchronized void writeCheckpoint(Collection<CoordinatorLogEntry> checkpointContent) {
        try {
            closeOutput();
            // 写入
            rwChannel = file.openNewVersionForNioWriting();
            for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
                write(coordinatorLogEntry, false);
            }
            rwChannel.force(false);
            // 删除备份
            file.discardBackupVersion();
        } catch (FileNotFoundException firstStart) {
            // the file could not be opened for reading;
            // merely return the default empty vector
        } catch (Exception e) {
            logger.error("Failed to write checkpoint", e);
        }
    }

    /**
     * create the log base dir
     *
     * @param baseDir
     */
    private void createBaseDir(String baseDir) {
        File baseDirFolder = new File(baseDir);
        if (!baseDirFolder.exists()) {
            baseDirFolder.mkdirs();
        }
    }

}
