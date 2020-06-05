package com.singgel.simhash;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author singgel
 * @description
 * @created_at: 2020-06-02 15:40
 **/
@Slf4j
public class Processor {

    private static ObjectMapper mapper = new ObjectMapper();

    private static final String SIMHASH_TABLE = ConfigFactory.load().getConfig("hbase").getString("simHash_table");

    private static final String SIMHASH_TABLE_QUALIFIER = "status_ids";

    private static final String OUTPUT_TOPIC = ConfigFactory.load().getConfig("kafka").getString("output_topic");

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMM d, yyyy h:m:s a", Locale.ENGLISH);

    private KafkaProducer kafkaProducer;

    public Processor(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    void process(ConsumerRecord<String, String> record) {
        try {
            JsonNode jsonNode = mapper.readTree(record.value());


            int contentType = jsonNode.get("contentType").asInt();
            //只处理帖子
            if (contentType == 0) {
                JsonNode data = mapper.readTree(jsonNode.get("data").asText());
                Long statusId = data.get("id").asLong();
                log.info("process status_id:" + statusId);
                String text = data.get("text").asText();
                String createdAt = data.get("created_at").asText();
                LocalDateTime localDateTime = LocalDateTime.parse(createdAt, formatter);
                Long userId = data.get("user_id").asLong();
                int textLength = Utils.getTextLengthWithoutHtml(text);

                //生成statusInfo对象
                StatusInfo statusInfo = new StatusInfo(statusId, localDateTime, userId, text, textLength);

                //处理
                doProcess(statusInfo);

            }

        } catch (Exception e) {
            e.printStackTrace();
            log.error(String.format("process record: %s failed!", record.value()), e);
        }
    }

    private void doProcess(StatusInfo statusInfo) {

        List<String> simHashList = statusInfo.listSimHashes();
        statusInfo.setSentenceCount(simHashList.size());

        if (simHashList.isEmpty()) {
            return;
        }

        //先从hbase读出新帖的simHash值对应的帖子信息列表
        Map<String, List<String>> simHashStatus = getSimHashStatus(simHashList);

        //写入simHash_table
        writeSimHashTable(simHashStatus, simHashList, statusInfo);

        if (simHashStatus.size() != 0) {
            //返回相似度最高的帖子
            StatusInfo similarStatusInfo = getTopSimilarStatus(statusInfo, simHashStatus);

            if (similarStatusInfo == null) {
                return;
            }
            //发送kafka
            JSONObject object = new JSONObject();
            object.fluentPut("statusInfo", statusInfo).
                    fluentPut("similarStatus", similarStatusInfo);

            ProducerRecord<String, String> record = new ProducerRecord<>(OUTPUT_TOPIC, statusInfo.getStatusId().toString(), object.toJSONString());
            kafkaProducer.send(record);

        }
    }


    /**
     * 将simhash写入simhash table
     * rowkey为simhash, value 为多个 statusId_createdAt_userId_sentenceCount_textLength 字符串的拼接
     *
     * @param simHashList
     * @param statusInfo
     */
    private void writeSimHashTable(Map<String, List<String>> oldSimHashStatus, List<String> simHashList, StatusInfo statusInfo) {
        List<Put> puts = new ArrayList<>();

        for (String simHash : simHashList) {

            Put put = new Put(simHash.getBytes());
            List<String> value = new ArrayList<>();
            List<String> statusIdsStr = oldSimHashStatus.get(simHash);

            long currentTime = System.currentTimeMillis();
            //如果simHash值有对应的帖子列表，则需要将7天前插入的帖子去除掉，以免此simHash对应的帖子个数无限增加
            if (statusIdsStr != null) {
                String[] arr;
                for (String status : statusIdsStr) {
                    arr = status.split(Constants.INNER_SEP);
                    if (currentTime - Long.valueOf(arr[1]) <= 7 * 24 * 3600 * 1000 && !statusInfo.getStatusId().equals(arr[0])) {
                        value.add(arr[0] + Constants.INNER_SEP + arr[1]);
                    }
                }
                value.add(statusInfo.jointString());
                put.addColumn(Constants.CF.getBytes(), SIMHASH_TABLE_QUALIFIER.getBytes(), StringUtils.join(value, Constants.OUTER_SEP).getBytes());
            } else {
                //如果simHash之前没有插入帖子数据，则直接插入
                put.addColumn(Constants.CF.getBytes(), SIMHASH_TABLE_QUALIFIER.getBytes(), statusInfo.jointString().getBytes());
            }
            puts.add(put);
        }
        HbaseUtil.batchPut(SIMHASH_TABLE, puts);
    }

    /**
     * 获取相似度最高的一篇帖子
     *
     * @param statusInfo
     * @param simHashStatus
     * @return
     */
    private StatusInfo getTopSimilarStatus(StatusInfo statusInfo, Map<String, List<String>> simHashStatus) {
        StatusInfo result = null;
        List<String> allSimStatus = new ArrayList<>(4);
        for (List<String> status : simHashStatus.values()) {
            allSimStatus.addAll(status);
        }
        Map<String, Long> collect = allSimStatus.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
        LinkedHashMap<String, Long> sortedMap = Utils.mapSortByValue(collect);
        Long simCount;

        for (Map.Entry<String, Long> entry : sortedMap.entrySet()) {
            String status = entry.getKey();
            String userId;
            try {
                userId = status.split(Constants.INNER_SEP)[2];
            } catch (Exception e) {
                log.error("get user_id from statusInfo:{} failed,", status);
                continue;
            }
            if (!statusInfo.getUserId().equals(userId)) {
                simCount = entry.getValue();
                result = StatusInfo.of(status);
                result.setSimCount(simCount);
                break;
            }
        }
        return result;
    }

    /**
     * 获取每个simHash列表对应的帖子信息列表
     *
     * @param simHashList
     * @return
     */
    private Map<String, List<String>> getSimHashStatus(List<String> simHashList) {
        Map<String, List<String>> simHashStatus = new HashMap<>(4);
        List<Get> gets = new ArrayList<>();
        for (String simHash : simHashList) {
            Get get = new Get(simHash.getBytes());
            get.addColumn(Constants.CF.getBytes(), SIMHASH_TABLE_QUALIFIER.getBytes());
            gets.add(get);
        }
        Result[] results = HbaseUtil.batchGet(SIMHASH_TABLE, gets);
        String rowKey, statuses;
        for (Result result : results) {
            if (result.getRow() != null) {
                rowKey = new String(result.getRow());
                statuses = new String(result.getValue(Constants.CF.getBytes(), SIMHASH_TABLE_QUALIFIER.getBytes()));
                simHashStatus.put(rowKey, Arrays.asList(statuses.split(Constants.OUTER_SEP)));

            }
        }
        return simHashStatus;
    }

}
