package com.singgel.simhash;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author singgel
 * @description
 * @created_at: 2020-06-02 15:12
 **/
public class Constants {

    public static final StringDeserializer STR_DE = new StringDeserializer();

    public static final StringSerializer STR_SER = new StringSerializer();

    /**
     * hbase表中默认列簇名
     */
    public static final String CF = "basic";

    /**
     * simHash表value中每个帖子信息内部字段的拼接符
     */
    public static final String INNER_SEP = "_";

    /**
     * simHash表value中多个帖子信息之间的拼接符
     */
    public static final String OUTER_SEP = ",";

}
