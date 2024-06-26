package com.mazama.rock.fs.hive;

import com.google.common.collect.Lists;
import com.mazama.rock.core.RockFile;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseResult;

/**
 * Mapping rock file operations to hive table.
 */
public class HiveFile extends RockFile {

  /** target table */
  private Table table;

  /** hive meta store client, get or set table metadata*/
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

  /**
   * Parse hive sql ddl command to get table metadata, and create a new table.
   * @param command hive ddl sql command
   * @return {@link RockFile}
   * @throws Exception
   */
  @Override
  protected RockFile create(String command) throws Exception {
    ParseDriver pd = new ParseDriver();
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    table.setSd(sd);
    List<FieldSchema> cols = Lists.newArrayList();
    sd.setCols(cols);
    Partition partition = new Partition();
    partition.setSd(sd);
    try {
      ParseResult result = pd.parse(command);
      ASTNode root = result.getTree();
      for (Node node : root.getChildren()) {
        ASTNode childNode = (ASTNode) node;
        if (childNode.getToken().getType() == HiveParser.TOK_CREATETABLE) {
          table.setTableName(childNode.getChild(0).getChild(0).getText());

          // Get columns name, type and comment from ast.
          for (Node childChild : childNode.getChildren()) {
            ASTNode childChildNode = (ASTNode) childChild;
            if (childChildNode.getToken().getType() == HiveParser.TOK_TABCOLLIST) {
              for (Node columnNode : childChildNode.getChildren()) {
                ASTNode column = (ASTNode) columnNode;
                String name = column.getChild(0).getText();
                String type = column.getChild(1).getText();
                String comment = column.getChild(2).getText();
                FieldSchema fieldSchema = new FieldSchema(name, type, comment);
                cols.add(fieldSchema);
              }
            }

            // Get partition columns name and type from ast.
            if (childChildNode.getToken().getType() == HiveParser.TOK_TABLEPARTCOLS) {
              for (Node partitionNode : childChildNode.getChildren()) {
                ASTNode partitionParams = (ASTNode) partitionNode;
                String name = partitionParams.getChild(0).getText();
                String type = partitionParams.getChild(1).getText();
              }
            }

            //
            if (childChildNode.getToken().getType() == HiveParser.TOK_TABLEPROPERTIES) {
              for (Node propertyNode : childChildNode.getChildren()) {
                ASTNode property = (ASTNode) propertyNode;
                if (property.getToken().getType() == HiveParser.TOK_TABLEPROPLIST) {
                  for(Node prop : property.getChildren()) {
                    ASTNode propNode = (ASTNode) prop;
                    String name = propNode.getChild(0).getText();
                    String value = propNode.getChild(1).getText();
//                    sd.putToParameters();
                  }
                }
              }
            }

          }
        }
      }

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
