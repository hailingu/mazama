package com.mazama.rock.fs.hive;

import com.mazama.rock.core.RockFile;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseResult;

public class HiveFile extends RockFile {

  private Table table;
  private final HiveMetaStoreClient hiveMetaStoreClient;

  public HiveFile(HiveMetaStoreClient hiveMetaStoreClient) {
    this.hiveMetaStoreClient = hiveMetaStoreClient;
  }

  /**
   * Open a hive table
   *
   * @param tableArgs table name
   * @return {@link RockFile}
   * @throws Exception table not found
   */
  @Override
  protected RockFile open(String... tableArgs) throws Exception {
    if (this.hiveMetaStoreClient == null) {
      throw new Exception("HiveMetaStoreClient is null");
    }

    if (tableArgs.length < 2) {
      throw new Exception("Missing arguments: databaseName, tableName");
    }

    String databaseName = tableArgs[0];
    String tableName = tableArgs[1];

    this.table = hiveMetaStoreClient.getTable(new GetTableRequest(databaseName, tableName));
    return this;
  }

  @Override
  protected int close(RockFile file) throws Exception {
    this.table = null;
    return 0;
  }

  @Override
  protected RockFile create(String command) throws Exception {
    ParseDriver pd = new ParseDriver();
    try {
      ParseResult result = pd.parse(command);
      ASTNode root = result.getTree();





    } catch (Exception e) {
      throw new Exception("Failed to parse command: " + command);
    }

    return null;
  }

  @Override
  protected int delete(String path) throws Exception {
    return 0;
  }

  @Override
  protected boolean exists(String path) throws Exception {
    return false;
  }

  @Override
  protected int copy(RockFile src, RockFile dest) throws Exception {
    return 0;
  }
}
