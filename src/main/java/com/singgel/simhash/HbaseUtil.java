package com.singgel.simhash;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author chenyuan
 * @description
 * @created_at: 2020-06-02 11:42
 **/
@Slf4j
public class HbaseUtil {

    private static Connection connection;

    private static ExecutorService pool = Executors.newFixedThreadPool(6);

    /**
     * 生成hbase connection
     *
     * @return
     */
    private static Connection generateConnection() {
        if (connection == null || connection.isClosed()) {
            synchronized (HbaseUtil.class) {
                org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", "10.10.31.9:2181,10.10.36.7:2181,10.10.37.7:2181");
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
                configuration.set("zookeeper.znode.parent", "/hbase");
                for (int retrys = 0; (connection == null || connection.isClosed()) && retrys < 20; retrys++) {
                    try {
                        connection = ConnectionFactory.createConnection(configuration, pool);
                    } catch (IOException e) {
                        log.warn("create hbase connection failed. retrys: {}", retrys, e);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
        }
        return connection;
    }

    /**
     * 写入hbase
     *
     * @param tableName 表名
     * @param cf        列簇名
     * @param qualifier 列名
     * @param key       rowkey
     * @param value     写入的值
     */
    public static void writeString(String tableName, String cf, String qualifier, String key, String value) {

        try (Table table = generateConnection().getTable(TableName.valueOf(tableName))) {
            Put put = new Put(key.getBytes());
            put.addColumn(cf.getBytes(), qualifier.getBytes(), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            log.error(String.format("write to hbase failed! key=%s,qualifier=%s", key, qualifier), e);
        }
    }

    /**
     * 读取hbase
     *
     * @param tableName 表名
     * @param cf        列簇名
     * @param qualifier 列名
     * @param key       rowkey
     * @return 如果rowkey不存在返回值为null
     */
    public static String readString(String tableName, String cf, String qualifier, String key) {
        String value = null;
        try (Table table = generateConnection().getTable(TableName.valueOf(tableName))) {
            Get get = new Get(key.getBytes());
            get.addColumn(cf.getBytes(), qualifier.getBytes());
            Result result = table.get(get);
            if (result.getRow() == null) {
                return null;
            }
            byte[] byteValue = result.getValue(cf.getBytes(), qualifier.getBytes());
            value = new String(byteValue);
        } catch (IOException e) {
            log.error(String.format("read hbase failed! key=%s,qualifier=%s", key, qualifier), e);
        }
        return value;
    }

    /**
     * 写入hbase
     *
     * @param tableName 表名
     * @param puts      put列表
     */
    public static void batchPut(String tableName, List<Put> puts) {

        try (Table table = generateConnection().getTable(TableName.valueOf(tableName))) {
            table.put(puts);
        } catch (IOException e) {
            log.error(String.format("batch put simHash statusId failed!"), e);
        }
    }

    /**
     * 批量读取
     *
     * @param tableName 表名
     * @param gets      get列表
     */
    public static Result[] batchGet(String tableName, List<Get> gets) {
        Result[] results = null;
        try (Table table = generateConnection().getTable(TableName.valueOf(tableName))) {
            results = table.get(gets);
        } catch (IOException e) {
            log.error(String.format("batch get simHash statusId failed!"), e);
        }
        return results;
    }


}
