package com.mazama.rock.fs.hive;

import com.google.common.collect.Lists;
import com.mazama.rock.core.RockFile;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapping rock file operations to hive table.
 *
 * @version 0.0.0.1
 */
public class HiveFile extends RockFile {

  static final Logger LOGGER = LoggerFactory.getLogger(HiveFile.class);

  /**
   * target table
   */
  private Table table;

  /**
   * hive meta store client, get or set table metadata
   */
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
      LOGGER.warn("HiveMetaStoreClient is null");
      return null;
    }

    if (tableArgs.length < 2) {
      LOGGER.warn("Missing arguments: databaseName, tableName");
      return null;
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
   *
   * @param command hive ddl sql command
   * @return {@link RockFile}
   * @throws Exception
   */
  @Override
  protected RockFile create(String command) throws Exception {
    ParseDriver pd = new ParseDriver();
    Table table = null;

    try {
      ParseResult result = pd.parse(command);
      ASTNode root = result.getTree();
      for (Node node : root.getChildren()) {
        ASTNode childNode = (ASTNode) node;
        if (childNode.getToken().getType() == HiveParser.TOK_CREATETABLE) {
          table = new Table();
          StorageDescriptor sd = new StorageDescriptor();
          table.setSd(sd);

          table.setTableName(childNode.getChild(0).getChild(0).getText());

          // Get columns name, type and comment from ast.
          for (Node childChild : childNode.getChildren()) {
            ASTNode childChildNode = (ASTNode) childChild;
            if (childChildNode.getToken().getType() == HiveParser.TOK_TABCOLLIST) {
              List<FieldSchema> cols = Lists.newArrayList();
              HiveFile.setFieldSchemaFromASTNode(childChildNode, cols);
              sd.setCols(cols);
            }

            // Set partition columns name, type and comment from ast.
            if (childChildNode.getToken().getType() == HiveParser.TOK_TABLEPARTCOLS) {
              List<FieldSchema> partitions = Lists.newArrayList();
              HiveFile.setFieldSchemaFromASTNode(childChildNode, partitions);
              table.setPartitionKeys(partitions);
            }

            // Set storage property from ast.
            if (childChildNode.getToken().getType() == HiveParser.TOK_TABLEPROPERTIES) {
              for (Node propertyNode : childChildNode.getChildren()) {
                ASTNode property = (ASTNode) propertyNode;
                if (property.getToken().getType() == HiveParser.TOK_TABLEPROPLIST) {
                  for (Node prop : property.getChildren()) {
                    ASTNode propNode = (ASTNode) prop;
                    String key = propNode.getChild(0).getText();
                    String value = propNode.getChild(1).getText();
                    sd.putToParameters(key, value);
                  }
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to parse command: {}", command);
      throw new Exception("Failed to parse command: " + command);
    }

    if (table != null) {
      hiveMetaStoreClient.createTable(table);
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

  /**
   * Get field schema from ast node
   *
   * @param astNode      ast node describe column name, type and comment
   * @param fieldSchemas list of parsed field schema from ast node
   */
  static void setFieldSchemaFromASTNode(
      ASTNode astNode,
      List<FieldSchema> fieldSchemas) {
    for (Node partitionNode : astNode.getChildren()) {
      ASTNode partitionParams = (ASTNode) partitionNode;
      String name = partitionParams.getChild(0).getText();
      String type = partitionParams.getChild(1).getText();
      String comment = partitionParams.getChild(2).getText();
      FieldSchema fieldSchema = new FieldSchema(name, type, comment);
      fieldSchemas.add(fieldSchema);
    }
  }
}
