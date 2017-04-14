/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mycat.memory.unsafe.storage;


import io.mycat.memory.unsafe.utils.JavaUtils;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
 * Block files are hashed among the directories listed in mycat.local.dir
 */
public class DataNodeFileManager {
  private static final Logger LOG = LoggerFactory.getLogger(DataNodeFileManager.class);
  private MycatPropertyConf conf;
  private  boolean deleteFilesOnStop;
  /**
   * TODO 操作完成之后，需要删除临时文件
   */
  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  // private val shutdownHook ;
  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */

  private List<File> localDirs ;
  private int subDirsPerLocalDir;

  private ConcurrentHashMap<Integer,ArrayList<File>> subDirs;


  public DataNodeFileManager(MycatPropertyConf conf , boolean deleteFilesOnStop) throws IOException {

    this.conf = conf;
    this.deleteFilesOnStop = deleteFilesOnStop;


    subDirsPerLocalDir = conf.getInt("mycat.diskStore.subDirectories", 64);
    localDirs  = createLocalDirs(conf);
    if (localDirs.isEmpty()) {
      System.exit(-1);
    }
    subDirs =  new ConcurrentHashMap<Integer,ArrayList<File>>(localDirs.size());



    for (int i = 0; i < localDirs.size() ; i++) {
      ArrayList<File> list = new ArrayList<File>(subDirsPerLocalDir);

      for (int j = 0; j < subDirsPerLocalDir; j++) {
        list.add(i,null);
      }

      subDirs.put(i,list);
    }

  }

  /** Produces a unique block id and File suitable for storing local intermediate results. */
  public TempDataNodeId createTempLocalBlock() throws IOException {

    TempDataNodeId blockId = new TempDataNodeId(UUID.randomUUID().toString());

    while (getFile(blockId).exists()) {
      blockId = new TempDataNodeId(UUID.randomUUID().toString());
    };

    return  blockId;
  }


  /** Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  public File getFile(String filename) throws IOException {
    // Figure out which local directory it hashes to, and which subdirectory in that
    int hash = JavaUtils.nonNegativeHash(filename);
    int dirId = hash % localDirs.size();
    int subDirId = (hash / localDirs.size()) % subDirsPerLocalDir;

    synchronized (this) {
      File file = subDirs.get(dirId).get(subDirId);
      if (file != null) {
      
      } else {
        file = new File(localDirs.get(dirId), "%02x".format(String.valueOf(subDirId)));
        if (!file.exists() && !file.mkdir()) {
          throw new IOException("Failed to create local dir in $newDir.");
        }
        subDirs.get(dirId).add(subDirId,file);
      }
    }

    /**
     *类似二维数组
     */
    return  new File(subDirs.get(dirId).get(subDirId),filename);
  }

  public File getFile(ConnectionId connid) throws IOException {
    return getFile(connid.name);
  }

  /**TODO config root
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  private  List<File> createLocalDirs(MycatPropertyConf conf) {

    String rootDirs = conf.getString("mycat.local.dirs","datanode");

    String rdir[] = rootDirs.split(",");
    List<File> dirs = new ArrayList<File>();
    for (int i = 0; i <rdir.length ; i++) {
      try {
        File localDir = JavaUtils.createDirectory(rdir[i],"datenode");
        dirs.add(localDir);
      } catch(Exception e) {
        LOG.error("Failed to create local dir in "+ rdir[i] + ". Ignoring this directory.");
      }
    }

    return  dirs;
  }

  /** Cleanup local dirs and stop shuffle sender. */
  public void stop() {
    doStop();
  }

  private void doStop() {
    if (deleteFilesOnStop) {
      File localDir;
      int i = 0;
      System.out.println(localDirs.size());
      while (i<localDirs.size()&&localDirs.size()>0){
        localDir = localDirs.get(i);
        //System.out.println(localDir);
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            JavaUtils.deleteRecursively(localDir);
          } catch(Exception e) {
            LOG.error(e.getMessage());
          }
        }
        i++;
      }
    }
  }
}
