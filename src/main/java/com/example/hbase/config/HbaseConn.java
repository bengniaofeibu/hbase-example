package com.example.hbase.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * Created by t_hz on 2019/2/18.
 */
@Slf4j
public class HbaseConn {

    private static final HbaseConn INSTANCE = new HbaseConn();
    private static Configuration configuration;
    private static Connection connection;

    private HbaseConn(){
        try {
            System.setProperty("hadoop.home.dir", "/Users/t_hz/Downloads/hadoop-2.8.5");
            if (configuration == null){
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.master", "192.168.0.101:16020");
                configuration.set("hbase.zookpeer.quorum","localhost");
                configuration.set("hbase.zookeeper.property.clientPort","2181");
            }
        }catch (Exception e){
            log.error("初始化configuration失败 {}",e.getMessage());
        }

    }

    private Connection getConnection(){
        try {
            if (connection == null || connection.isClosed()){
                connection = ConnectionFactory.createConnection(configuration);
            }
        }catch (Exception e){
            log.error("创建连接失败 {}",e.getMessage());
        }

        return connection;
    }

    public static Connection getHbaseConnection(){
        return INSTANCE.getConnection();
    }

    public static Table getTable(String table) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(table));
    }

    public static void closeConnection(){
        if (connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                log.error("关闭连接失败 {}",e.getMessage());
            }
        }
    }
}
