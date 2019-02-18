package com.example.hbase.util;

import com.example.hbase.config.HbaseConn;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.List;

/**
 * Created by t_hz on 2019/2/18.
 */
@Slf4j
public class HbaseUtil {

    /**
     *
     * @param table 表名
     * @param cfs 列族的数组
     * @return
     */
    public static boolean createTable(String table,String[] cfs){

        try (HBaseAdmin baseAdmin = (HBaseAdmin) HbaseConn.getHbaseConnection().getAdmin()){

            if (baseAdmin.tableExists(table)){
                return false;
            }

            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(table));
            Arrays.stream(cfs).forEach(cf->{
                HColumnDescriptor colum = new HColumnDescriptor(cf);
                colum.setMaxVersions(1);
                hTableDescriptor.addFamily(colum);
            });
            baseAdmin.createTable(hTableDescriptor);
        }catch (Exception e){
            log.error("创建table失败 {}",e.getMessage());
        }
        return true;
    }

    /**
     *  删除表
     * @param tableName
     * @return
     */
    public static boolean deleteTable(String tableName){
        try(HBaseAdmin baseAdmin = (HBaseAdmin)HbaseConn.getHbaseConnection().getAdmin()) {
            //先禁用表
            baseAdmin.disableTable(tableName);
            baseAdmin.deleteTable(tableName);

        }catch (Exception e){
            log.error("删除表失败{}",e.getMessage());
        }
      return true;
    }

    /**
     *
     * @param tableName 表明
     * @param rowKey 唯一标识
     * @param cfName 列族名
     * @param qualifier 列标识
     * @param data 数据
     * @return
     */
    public static boolean putRow(String tableName,String rowKey,String cfName,String qualifier,String data){
        try(Table table = HbaseConn.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifier),Bytes.toBytes(data));
            table.put(put);
        }catch (Exception e){
            log.error("添加数据失败{}",e.getMessage());
        }
        return true;
    }

    /**
     *  批量插入数据
     * @param tableName
     * @param putList
     * @return
     */
    public static boolean putRows(String tableName,List<Put> putList){
        try(Table table = HbaseConn.getTable(tableName)) {
            table.put(putList);
        }catch (Exception e){
            log.error("添加数据失败{}",e.getMessage());
        }
        return true;
    }


    /**
     *  获取单条数据
     * @param tableName
     * @param rowKey
     * @return
     */
    public static Result getRow(String tableName,String rowKey){

        try(Table table = HbaseConn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        }catch (Exception e){
            log.error("获取数据失败{}",e.getMessage());
        }
        return null;
    }

    /**
     *  获取单条数据
     * @param tableName
     * @param rowKey
     * @return
     */
    public static Result getRow(String tableName,String rowKey,FilterList filterList){

        try(Table table = HbaseConn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filterList);
            return table.get(get);
        }catch (Exception e){
            log.error("获取数据失败{}",e.getMessage());
        }
        return null;
    }

    public static ResultScanner getResult(String tableName){
        try(Table table = HbaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setCaching(1000);
            return table.getScanner(scan);
        }catch (Exception e){
            log.error("获取数据失败{}",e.getMessage());
        }
        return null;
    }

    /**
     *  批量检索数据
     * @param tableName 表名
     * @param startRowKey 起始RowKey
     * @param endRowKey 终止RowKey
     * @return
     */
    public static ResultScanner getResult(String tableName,String startRowKey,String endRowKey){
        try(Table table = HbaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setCaching(1000);
            return table.getScanner(scan);
        }catch (Exception e){
            log.error("获取数据失败{}",e.getMessage());
        }
        return null;
    }
    /**
     *  批量检索数据
     * @param tableName 表名
     * @param startRowKey 起始RowKey
     * @param endRowKey 终止RowKey
     * @return
     */
    public static ResultScanner getResult(String tableName, String startRowKey, String endRowKey, FilterList filterList){
        try(Table table = HbaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            scan.setCaching(1000);
            return table.getScanner(scan);
        }catch (Exception e){
            log.error("获取数据失败{}",e.getMessage());
        }
        return null;
    }


    /**
     * 删除一行数据
     * @param tableName
     * @param rowKey
     * @return
     */
    public static boolean deleteRow(String tableName,String rowKey){
        try(Table table = HbaseConn.getTable(tableName)) {
           Delete delete = new Delete(Bytes.toBytes(rowKey));
           table.delete(delete);
        }catch (Exception e){
            log.error("删除数据失败{}",e.getMessage());
        }
        return true;
    }

    /**
     *  删除表
     * @param tableName
     * @return
     */
    public static boolean deleteColumnFamily(String tableName,String cfName){
        try(HBaseAdmin baseAdmin = (HBaseAdmin)HbaseConn.getHbaseConnection().getAdmin()) {
            baseAdmin.deleteColumn(tableName,cfName);
        }catch (Exception e){
            log.error("删除column失败{}",e.getMessage());
        }
        return true;
    }

    /**
     *  删除Qualifier
     * @param tableName
     * @return
     */
    public static boolean deleteQualifier(String tableName,String rowKey,String cfName,String qualifier){
        try(Table table = HbaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifier));
            table.delete(delete);
        }catch (Exception e){
            log.error("删除qualifier失败{}",e.getMessage());
        }
        return true;
    }

}

