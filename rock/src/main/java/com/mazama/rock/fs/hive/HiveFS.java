package com.mazama.rock.fs.hive;

import com.mazama.rock.core.RockFS;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

public class HiveFS extends RockFS {

  private static final String fileSystem = "hive";

  HiveMetaStoreClient hiveMetaStoreClient;

  public HiveFS() {
  }

  public HiveFS(HiveMetaStoreClient hiveMetaStoreClient) {
    this.hiveMetaStoreClient = hiveMetaStoreClient;
  }

  /**
   * Get the file system
   *
   * @return "hive"
   */
  @Override
  protected String getFileSystem() {
    return HiveFS.fileSystem;
  }
}
