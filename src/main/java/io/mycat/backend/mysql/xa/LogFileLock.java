package io.mycat.backend.mysql.xa;

import io.mycat.backend.mysql.xa.recovery.LogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class LogFileLock {
    public static final Logger logger = LoggerFactory
            .getLogger(LogFileLock.class);
    private static final String FILE_SEPARATOR = String.valueOf(File.separatorChar);
    private File lockfileToPreventDoubleStartup_;
    private FileOutputStream lockfilestream_ = null;
    private FileLock lock_ = null;

    private String dir;

    private String fileName;

    public LogFileLock(String dir, String fileName) {
        if(!dir.endsWith(FILE_SEPARATOR)) {
            dir += FILE_SEPARATOR;
        }
        this.dir = dir;
        this.fileName = fileName;
    }

    public void acquireLock() throws LogException {
        try {
            File parent = new File(dir);
            if(!parent.exists()) {
                parent.mkdir();
            }
            lockfileToPreventDoubleStartup_ = new File(dir, fileName + ".lck");
            lockfilestream_ = new FileOutputStream(lockfileToPreventDoubleStartup_);
            lock_ = lockfilestream_.getChannel().tryLock();
            lockfileToPreventDoubleStartup_.deleteOnExit();
        } catch (OverlappingFileLockException failedToGetLock) {
            // happens on windows
            lock_ = null;
        } catch (IOException failedToGetLock) {
            // happens on windows
            lock_ = null;
        }
        if (lock_ == null) {
            logger.error("ERROR: the specified log seems to be in use already: " + fileName + " in " + dir + ". Make sure that no other instance is running, or kill any pending process if needed.");
            throw new LogException("Log already in use? " + fileName + " in "+ dir);
        }
    }

    public void releaseLock() {
        try {
            if (lock_ != null) {
                lock_.release();
            }
            if (lockfilestream_ != null)
                lockfilestream_.close();
        } catch (IOException e) {
            logger.warn("Error releasing file lock: " + e.getMessage());
        } finally {
            lock_ = null;
        }

        if (lockfileToPreventDoubleStartup_ != null) {
            lockfileToPreventDoubleStartup_.delete();
            lockfileToPreventDoubleStartup_ = null;
        }
    }
}
