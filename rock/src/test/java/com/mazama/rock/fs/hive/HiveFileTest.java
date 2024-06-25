package com.mazama.rock.fs.hive;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseResult;
import org.junit.jupiter.api.Test;

public class HiveFileTest {

  @Test
  public void testHiveDDLParser() {
    ParseDriver pd = new ParseDriver();
    try {
      ParseResult result = pd.parse("CREATE TABLE IF NOT EXISTS test (id INT, name STRING) PARTITIONED BY (`dt` string) \n"
          + "STORED AS ORC \n"
          + "TBLPROPERTIES ('orc.compression'='snappy')");
      ASTNode root = result.getTree();
      for (Node child : root.getChildren()) {
        ASTNode childNode = (ASTNode) child;
        if (childNode.getToken().getType() == HiveParser.TOK_CREATETABLE) {
          System.out.println("Create table statement");
          ASTNode tableNode = (ASTNode) childNode.getChild(0);
          String tableName = tableNode.getChild(0).getText();
          System.out.println("Table name: " + tableName);

          for (Node childChild : childNode.getChildren()) {
            ASTNode childChildNode = (ASTNode) childChild;
            System.out.println(childChildNode.getToken());
            if (childChildNode.getToken().getType() == HiveParser.TOK_TABCOLLIST) {
              for (Node columnNode : childChildNode.getChildren()) {
                ASTNode column = (ASTNode) columnNode;
                String columnName = column.getChild(0).getText();
                String columnType = column.getChild(1).getText();
                System.out.println("Column name: " + columnName + ", Column type: " + columnType);
              }
            }

            if (childChildNode.getToken().getType() == HiveParser.TOK_TABLEPARTCOLS) {
              for (Node partitionNode : childChildNode.getChildren()) {
                ASTNode partition = (ASTNode) partitionNode;
                String partitionName = partition.getChild(0).getText();
                String partitionType = partition.getChild(1).getText();
                System.out.println("Partition name: " + partitionName + ", Partition type: " + partitionType);
              }
            }

            if (childChildNode.getToken().getType() == HiveParser.TOK_TABLEPROPERTIES) {
              for (Node propertyNode : childChildNode.getChildren()) {
                ASTNode property = (ASTNode) propertyNode;
                if (property.getToken().getType() == HiveParser.TOK_TABLEPROPLIST) {
                  for(Node prop : property.getChildren()) {
                    ASTNode propNode = (ASTNode) prop;
                    String propertyName = propNode.getChild(0).getText();
                    String propertyValue = propNode.getChild(1).getText();
                    System.out.println("Property name: " + propertyName + ", Property value: " + propertyValue);
                  }
                }
              }
            }

            if (childChildNode.getToken().getType() == HiveParser.TOK_FILEFORMAT_GENERIC) {
              String fileFormat = childChildNode.getChild(0).getText();
              System.out.println("File format: " + fileFormat);
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
