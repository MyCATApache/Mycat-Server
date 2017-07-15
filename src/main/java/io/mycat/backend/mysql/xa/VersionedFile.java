package io.mycat.backend.mysql.xa;

import java.io.*;
import java.nio.channels.FileChannel;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class VersionedFile {

    private static final String FILE_SEPARATOR = String.valueOf(File.separatorChar);
    /**
     * 所在文件夹
     */
    private String baseDir;
    /**
     * 基础文件后缀
     */
    private String suffix;
    /**
     * 基础文件名
     */
    private String baseName;

    //state attributes below
    /**
     * 最新版本
     */
    private long version;
    private FileInputStream inputStream;
    private RandomAccessFile randomAccessFile;

    /**
     * Creates a new instance based on the given name parameters.
     * The actual complete name(s) of the physical file(s) will be based on a version number
     * inserted in between, to identify versions.
     *
     * @param baseDir  The base folder.
     * @param baseName The base name for of the file path/name.
     * @param suffix   The suffix to append to the complete file name.
     */
    public VersionedFile(String baseDir, String baseName, String suffix) {
        if (!baseDir.endsWith(FILE_SEPARATOR)) {
            baseDir += FILE_SEPARATOR;
        }
        this.baseDir = baseDir;
        this.suffix = suffix;
        this.baseName = baseName;
        resetVersion();
    }

    private void resetVersion() {
        this.version = extractLastValidVersionNumberFromFileNames();
    }

    /**
     * 获得文件夹下有效文件的最大版本。
     * 目前有效文件命名为 ${baseName}${version}${suffix}
     * 默认情况下，tmlog-1.log；
     *
     * @return 版本号
     * @see #extractVersion(String)
     */
    private long extractLastValidVersionNumberFromFileNames() {
        long version = -1;
        File cd = new File(getBaseDir());
        String[] names = cd.list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(getBaseName())
                        && name.endsWith(getSuffix());
            }
        });
        if (names != null) {
            for (String name : names) {
                long sfx = extractVersion(name);
                if (version < 0 || sfx < version)
                    version = sfx;
            }
        }
        return version;
    }

    /**
     * 解析文件名版本👌
     *
     * @param name 文件名
     * @return 版本号
     */
    private long extractVersion(String name) {
        long ret = 0;
        int lastpos = name.lastIndexOf('.');
        int startpos = getBaseName().length();
        String suffix = name.substring(startpos, lastpos);
        try {
            ret = Long.valueOf(suffix);
        } catch (NumberFormatException e) {
            IllegalArgumentException err = new IllegalArgumentException("Error extracting version from file: " + name + " in " + getBaseDir());
            err.initCause(e);
            throw err;
        }
        return ret;
    }

    private String getBackupVersionFileName() {
        return getBaseUrl() + (version - 1) + getSuffix();
    }

    public String getCurrentVersionFileName() {
        return getBaseUrl() + version + getSuffix();
    }

    public String getBaseUrl() {
        return baseDir + baseName;
    }

    public String getBaseDir() {
        return this.baseDir;
    }

    public String getBaseName() {
        return this.baseName;
    }

    public String getSuffix() {
        return this.suffix;
    }

    /**
     * Opens the last valid version for reading.
     *
     * @return A stream to read the last valid contents
     * of the file: either the backup version (if present)
     * or the current (and only) version if no backup is found.
     * @throws IllegalStateException If a newer version was opened for writing.
     * @throws FileNotFoundException If no last version was found.
     */
    public FileInputStream openLastValidVersionForReading()
            throws IllegalStateException, FileNotFoundException {
        if (randomAccessFile != null) {
            throw new IllegalStateException("Already started writing.");
        }
        inputStream = new FileInputStream(getCurrentVersionFileName());
        return inputStream;
    }

    /**
     * Opens a new version for writing to. Note that
     * this new version is tentative and cannot be read
     * by {@link #openLastValidVersionForReading()} until
     * {@link #discardBackupVersion()} is called.
     *
     * @return A stream for writing to.
     * @throws IllegalStateException If called more than once
     *                               without a close in between.
     * @throws IOException           If the file cannot be opened for writing.
     */
    public FileOutputStream openNewVersionForWriting() throws IOException {
        openNewVersionForNioWriting();
        return new FileOutputStream(randomAccessFile.getFD());
    }

    /**
     * Opens a new version for writing to. Note that
     * this new version is tentative and cannot be read
     * by {@link #openLastValidVersionForReading()} until
     * {@link #discardBackupVersion()} is called.
     * <p>
     * tips：目前 version++ 被注释掉。目前功能就是打开当前version对应的文件。
     *
     * @return A file for writing to.
     * @throws IllegalStateException If called more than once
     *                               without a close in between.
     * @throws FileNotFoundException If the file cannot be opened for writing.
     */
    public FileChannel openNewVersionForNioWriting() throws FileNotFoundException {
        if (randomAccessFile != null) {
            throw new IllegalStateException("Already writing a new version.");
        }
        //version++;
        randomAccessFile = new RandomAccessFile(getCurrentVersionFileName(), "rw");
        return randomAccessFile.getChannel();
    }

    /**
     * Discards the backup version (if any).
     * After calling this method, the newer version
     * produced after calling {@link #openNewVersionForWriting()}
     * becomes valid for reading next time when
     * {@link #openLastValidVersionForReading()} is called.
     * <p>
     * Note: it is the caller's responsibility to make sure that
     * all new data has been flushed to disk before calling this method!
     * <p>
     * tips：由于{@link #openNewVersionForNioWriting()}的version++被注释了，该方法基本没用。
     *
     * @throws IllegalStateException If {@link #openNewVersionForWriting()} has not been called yet.
     * @throws IOException           If the previous version exists but could no be deleted.
     */
    public void discardBackupVersion() throws IllegalStateException, IOException {
        if (randomAccessFile == null) {
            throw new IllegalStateException("No new version yet!");
        }
        String fileName = getBackupVersionFileName();
        File temp = new File(fileName);
        if (temp.exists() && !temp.delete()) {
            throw new IOException("Failed to delete backup version: " + fileName);
        }
    }

    /**
     * Closes any open resources and resets the file for reading again.
     *
     * @throws IOException If the output stream could not be closed.
     */

    public void close() throws IOException {
        resetVersion();
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                //don't care and won't happen: closing an input stream
                //does nothing says the JDK javadoc!
            } finally {
                inputStream = null;
            }
        }
        if (randomAccessFile != null) {
            try {
                if (randomAccessFile.getFD().valid()) randomAccessFile.close();
            } finally {
                randomAccessFile = null;
            }
        }
    }

    public long getSize() {
        long res;
        File f = new File(getCurrentVersionFileName());
        res = f.length();
        return res;
    }

}
