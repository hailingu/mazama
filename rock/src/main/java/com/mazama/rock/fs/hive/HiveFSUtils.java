package com.mazama.rock.fs.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;


public final class HiveFSUtils {
  private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(HiveFSUtils.class);

  /**
   * Mount a Hive file system
   * @param mountPoint mount point
   * @param args file system arguments
   * @return a {@link HiveFS} object
   */
  public static HiveFS mount(String mountPoint, String... args) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource("hive-site.xml");
    HiveMetaStoreClient hiveMetaStoreClient = null;
    try {
      hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
    } catch (MetaException e) {
      LOGGER.warn("Failed to create HiveMetaStoreClient", e);
    }
    return new HiveFS(hiveMetaStoreClient);
  }
}
