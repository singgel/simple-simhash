package com.singgel.simhash;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author singgel
 * @description
 * @created_at: 2020-06-03 16:41
 **/
@Data
public class StatusInfo {

    private Long statusId;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    private Long userId;

    @JSONField(serialize = false)
    private String text;

    private Long simCount;

    /**
     * 分句后的句子数
     */
    private int sentenceCount;

    /**
     * 去掉html链接后的字数
     */
    private int textLength;

    public StatusInfo() {
    }

    public StatusInfo(Long statusId, LocalDateTime createdAt, Long userId, String text, int textLength) {
        this.statusId = statusId;
        this.createdAt = createdAt;
        this.userId = userId;
        this.text = text;
        this.textLength = textLength;
    }


    public static StatusInfo of(String str) {
        String[] arr = str.split(Constants.INNER_SEP);

        StatusInfo statusInfo = new StatusInfo();
        statusInfo.setStatusId(Long.valueOf(arr[0]));
        statusInfo.setCreatedAt(LocalDateTime.ofEpochSecond(Long.valueOf(arr[1]) / 1000, 0, ZoneOffset.of("+8")));
        statusInfo.setUserId(Long.valueOf(arr[2]));
        statusInfo.setSentenceCount(Integer.valueOf(arr[3]));
        statusInfo.setTextLength(Integer.valueOf(arr[4]));

        return statusInfo;
    }

    /**
     * 返回一篇文章的simHash列表
     *
     * @return
     */
    public List<String> listSimHashes() {

        List<String> simHashList = new ArrayList<>();
        List<String> sentences = SimHash.cutTextToSentenceList(text);
        //如果text句数少于6句或者字数少于400则直接返回空list
        if (sentences.size() <= 2 || textLength <= 100) {
            return simHashList;
        }
        for (String sentence : sentences) {
            SimHash hash = new SimHash(sentence, 128);
            simHashList.add(hash.getStrSimHash().toString());
        }
        return simHashList;
    }

    private Long epochSecondOfCreatedAt() {
        return createdAt.toEpochSecond(ZoneOffset.of("+8")) * 1000;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatusInfo)) return false;
        StatusInfo that = (StatusInfo) o;
        return statusId.equals(that.statusId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statusId);
    }


    public String jointString() {
        return statusId + Constants.INNER_SEP +
                epochSecondOfCreatedAt() + Constants.INNER_SEP +
                userId + Constants.INNER_SEP +
                sentenceCount + Constants.INNER_SEP +
                textLength;
    }


}
