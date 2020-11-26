package com.bigdata.security;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * HBase Development Instruction Sample Code The sample code uses user
 * information as source data,it introduces how to implement businesss process
 * development using HBase API
 */
public class HBaseSample {
  private final static Log LOG = LogFactory.getLog(HBaseSample.class.getName());

//  private TableName tableName = null;
  private Configuration conf = null;
  private Connection conn = null;

  public HBaseSample(Configuration conf) throws IOException {
    this.conf = conf;
//    this.tableName = TableName.valueOf("hbase_sample_table");
    this.conn = ConnectionFactory.createConnection(conf);
  }

  public void test() throws Exception {
    try {
    	System.out.println("============HBaseSample.test");
    } catch (Exception e) {
      throw e;
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (Exception e1) {
          LOG.error("Failed to close the connection ", e1);
        }
      }
    }
  }
}
