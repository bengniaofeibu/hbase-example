package com.example.hbase;

import com.example.hbase.util.HbaseUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Created by t_hz on 2019/2/18.
 */
public class HbaseUtilTest extends HbaseExampleApplication {


    @Test
    public void createTable(){

        boolean fileTabel = HbaseUtil.createTable("OrderTable", new String[]{"goodsInfo", "amountInfo"});
        System.out.println(fileTabel);

    }

    @Test
    public void putRow(){

        HbaseUtil.putRow("OrderTable","rowKey1","goodsInfo","name","毛巾");
        HbaseUtil.putRow("OrderTable","rowKey1","goodsInfo","type","日用");
        HbaseUtil.putRow("OrderTable","rowKey1","goodsInfo","amout","10");
        HbaseUtil.putRow("OrderTable","rowKey2","goodsInfo","name","骑车");
        HbaseUtil.putRow("OrderTable","rowKey2","goodsInfo","type","玩具");
        HbaseUtil.putRow("OrderTable","rowKey2","goodsInfo","amout","20");

        HbaseUtil.putRow("OrderTable","rowKey2","amountInfo","name","支付");
        HbaseUtil.putRow("OrderTable","rowKey2","amountInfo","type","充值");
        HbaseUtil.putRow("OrderTable","rowKey3","amountInfo","amout","30");
    }

    @Test
    public void getResult(){
        Result result = HbaseUtil.getRow("OrderTable", "rowKey1");
        System.out.println("rowkey="+ Bytes.toString(result.getRow()));
        System.out.println("info="+Bytes.toString(result.getValue(Bytes.toBytes("goodsInfo"),Bytes.toBytes("name"))));
    }

    @Test
    public void getScanResut(){
        ResultScanner result = HbaseUtil.getResult("OrderTable", "rowKey1", "rowKey2");
        result.forEach(result1 -> {
            System.out.println(Bytes.toString(result1.getValue(Bytes.toBytes("goodsInfo"),Bytes.toBytes("name"))));
        });
        result.close();
    }

    @Test
    public void deleteRow(){
        HbaseUtil.deleteRow("OrderTable","rowKey3");
    }

    @Test
    public void  deleteColumnFamily(){
        HbaseUtil.deleteColumnFamily("OrderTable","amountInfo");
    }

    @Test
    public void deleteQualifier(){
        HbaseUtil.deleteQualifier("OrderTable","rowKey1","goodsInfo","name");
    }

    @Test
    public void deleteTable(){
        HbaseUtil.deleteTable("OrderTable");
    }

}
