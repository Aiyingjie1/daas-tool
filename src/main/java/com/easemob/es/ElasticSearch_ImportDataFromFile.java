package com.easemob.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.File;
import java.net.InetSocketAddress;

/**
 * Created by seeker on 16/2/25 10:48.
 */
public class ElasticSearch_ImportDataFromFile {

    public static TransportClient importTransportClient;
    public static TransportClient exportTransportClient;

    static {

//        exportTransportClient = TransportClient
//                .builder()
//                .settings(
//                        //                        Settings.settingsBuilder().put("cluster.name", "localES").build()
//                        Settings.settingsBuilder().put("cluster.name", "easemob-es").build()
//                )
//                .build()
//                .addTransportAddress(
//                        //                        new InetSocketTransportAddress(new InetSocketAddress("127.0.0.1", 9300))
//                        new InetSocketTransportAddress(new InetSocketAddress("120.26.197.70", 9300))
//                );

        importTransportClient = TransportClient
                .builder()
                .settings(
                        Settings.settingsBuilder().put("cluster.name", "localES").build()
                )
                .build()
                .addTransportAddress(
                        //                        new InetSocketTransportAddress(new InetSocketAddress("172.17.2.204", 9300))
                        new InetSocketTransportAddress(new InetSocketAddress("127.0.0.1", 9300))
                );

    }

    public static void main(String[] args) throws Exception {
        importFromEsResult("agentserve", "agentserve", "/Users/guoqingwu/sandboxlog/aa.log");
    }

    public static String importFromFile(String location) {

        LineIterator li = null;
        try {
            return FileUtils.lineIterator(new File(location)).nextLine();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            LineIterator.closeQuietly(li);
        }

        return null;
    }

    public static void importFromJson(String index, String type, String id, String data) {

        importTransportClient.prepareIndex(index, type).setId(id)
                .setSource(data)
                .get();
    }

    public static void importFromEsResult(String index, String key, String location) {

        JSONObject obj = JSONObject.parseObject(importFromFile(location));

        JSONArray data = obj.getJSONObject("hits").getJSONArray("hits");
        for (int i = 0; i < data.size(); i++) {
            JSONObject record = data.getJSONObject(i).getJSONObject("_source");

            System.out.println(record.toJSONString());

            importFromJson(index, index, record.getString(key), record.toJSONString());
        }
    }
}