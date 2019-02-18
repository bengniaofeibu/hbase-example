package com.example.hbase;

import com.example.hbase.config.HbaseConn;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by t_hz on 2019/2/18.
 */
public class HbaseConnTest extends HbaseExampleApplicationTests {

    @Test
    public void getHbaseConnTest(){
        Connection connection = HbaseConn.getHbaseConnection();
        System.out.println(connection.isClosed());
        HbaseConn.closeConnection();
        System.out.println(connection.isClosed());
    }

    @Test
    public void getTable(){
        try {
            Table fileTabel = HbaseConn.getTable("OrderTable");
            System.out.println(fileTabel.getName().getNameAsString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
