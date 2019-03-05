package io.mycat.backend.mysql.xa.recovery.impl;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectStreamException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.Deserializer;
import io.mycat.backend.mysql.xa.Serializer;
import io.mycat.backend.mysql.xa.VersionedFile;
import io.mycat.backend.mysql.xa.recovery.DeserialisationException;
import io.mycat.backend.mysql.xa.recovery.Repository;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SystemConfig;


/**
 * Created by zhangchao on 2016/10/13.
 */
public class FileSystemRepository implements Repository{
    public static final Logger logger = LoggerFactory
            .getLogger(FileSystemRepository.class);
    private VersionedFile file;
    private FileChannel rwChannel = null;
    private  Map<String, String > writeStorage = new HashMap<String, String>();

    public FileSystemRepository()  {
           init();
    }

    @Override
    public void init(){
//        ConfigProperties configProperties = Configuration.getConfigProperties();
//        String baseDir = configProperties.getLogBaseDir();
//        String baseName = configProperties.getLogBaseName();
        MycatConfig mycatconfig = MycatServer.getInstance().getConfig();
        SystemConfig systemConfig = mycatconfig.getSystem();

        String baseDir =systemConfig.getXARecoveryLogBaseDir();
        String baseName = systemConfig.getXARecoveryLogBaseName();

        logger.debug("baseDir " + baseDir);
        logger.debug("baseName " + baseName);

        //Judge whether exist the basedir
        createBaseDir(baseDir);

        file = new VersionedFile(baseDir, baseName, ".log");

    }

    private Serializer serializer = new Serializer();

    /*没有被调用*/
    @Override
    public void put(String id, CoordinatorLogEntry coordinatorLogEntry) {

//        try {
//            initChannelIfNecessary();
//            write(coordinatorLogEntry, true);
//        } catch (IOException e) {
//            logger.error(e.getMessage(),e);
//        }
    }

    private synchronized void initChannelIfNecessary()
            throws FileNotFoundException {
        if (rwChannel == null) {
            rwChannel = file.openNewVersionForNioWriting();
        }
    }

    private int write(CoordinatorLogEntry coordinatorLogEntry,
                       boolean flushImmediately) throws IOException {
        String str = serializer.toJSON(coordinatorLogEntry);
        //缓存一下
        writeStorage.put(coordinatorLogEntry.id, str);
//        logger.info(str);
        byte[] buffer = str.getBytes();
        ByteBuffer buff = ByteBuffer.wrap(buffer);
        writeToFile(buff, flushImmediately);
        return buffer.length;
    }

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

    public static Collection<CoordinatorLogEntry> readFromInputStream(
            InputStream in) {
        Map<String, CoordinatorLogEntry> coordinatorLogEntries = new HashMap<String, CoordinatorLogEntry>();
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

    static Map<String, CoordinatorLogEntry> readContent(BufferedReader br)
            throws IOException {

        Map<String, CoordinatorLogEntry> coordinatorLogEntries = new HashMap<String, CoordinatorLogEntry>();
        try {
            String line;
            while ((line = br.readLine()) != null) {
                CoordinatorLogEntry coordinatorLogEntry = deserialize(line);
                coordinatorLogEntries.put(coordinatorLogEntry.id,
                        coordinatorLogEntry);
            }

        } catch (EOFException unexpectedEOF) {
            logger.info(
                    "Unexpected EOF - logfile not closed properly last time?",
                    unexpectedEOF);
            // merely return what was read so far...
        } catch (StreamCorruptedException unexpectedEOF) {
            logger.info(
                    "Unexpected EOF - logfile not closed properly last time?",
                    unexpectedEOF);
            // merely return what was read so far...
        } catch (ObjectStreamException unexpectedEOF) {
            logger.info(
                    "Unexpected EOF - logfile not closed properly last time?",
                    unexpectedEOF);
            // merely return what was read so far...
        } catch (DeserialisationException unexpectedEOF) {
            logger.info("Unexpected EOF - logfile not closed properly last time? "
                    + unexpectedEOF);
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

    private static Deserializer deserializer = new Deserializer();

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

    protected void closeOutput() throws IllegalStateException {
        try {
            if (file != null) {
                file.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error closing previous output", e);
        }
    }

    @Override
    public synchronized void writeCheckpoint(String id,
                                             Collection<CoordinatorLogEntry> checkpointContent)
             {

        try {
            if(rwChannel == null) {
                initChannelIfNecessary();
            }
//            closeOutput();
//            rwChannel = file.openNewVersionForNioWriting();

            //判断xaId这条记录是否被修改
            boolean isUpdate = true;
            for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
                if(coordinatorLogEntry.id.equals(id)) {
                    isUpdate = checkForUpdate(id, coordinatorLogEntry);
                    break;
                }
            }
            if(isUpdate == false ){
                return ;
            }
            //清空所有的缓存
            writeStorage.clear();
            rwChannel.position(0);
            long writeSize = 0 ;
            for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
                writeSize += write(coordinatorLogEntry, false);
            }
//            logger.info("xaId {} writeCheckpoint {}",id, writeStorage.get(id));
            rwChannel.truncate(writeSize);
            rwChannel.force(true);
            file.discardBackupVersion();
        } catch (FileNotFoundException firstStart) {
            // the file could not be opened for reading;
            // merely return the default empty vector
        } catch (Exception e) {
            logger.error("Failed to write checkpoint", e);
        }

    }

    private boolean checkForUpdate(String id, CoordinatorLogEntry coordinatorLogEntry) {
        String backCoordinatorLogEntryStr = writeStorage.get(id);
        String str = serializer.toJSON(coordinatorLogEntry);
        if(null == backCoordinatorLogEntryStr || !str.equals(backCoordinatorLogEntryStr)) {
            writeStorage.put(id, str);
            return true;
        }
        return false;
    }



    /**
     * create the log base dir
     * @param baseDir
     */
    public void createBaseDir(String baseDir){
        File baseDirFolder = new File (baseDir);
        if (!baseDirFolder.exists()){
                baseDirFolder.mkdirs();
        }
    }

}
