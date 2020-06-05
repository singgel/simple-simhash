package com.singgel.simhash;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author singgel
 * @description
 * @created_at: 2020-06-03 14:40
 **/
public class Utils {

    private static final String REGXP_FOR_HTML = "<([^>]*)>";

    public static int getTextLengthWithoutHtml(String text) {
        Pattern pattern = Pattern.compile(REGXP_FOR_HTML);
        Matcher matcher = pattern.matcher(text);
        StringBuffer sb = new StringBuffer();
        boolean result1 = matcher.find();
        while (result1) {
            matcher.appendReplacement(sb, "");
            result1 = matcher.find();
        }
        matcher.appendTail(sb);
        return sb.toString().length();

    }

    public static LinkedHashMap<String, Long> mapSortByValue(Map<String, Long> unsortMap) {
        List<Map.Entry<String, Long>> list = new LinkedList<>(unsortMap.entrySet());

        list.sort(((o1, o2) -> o2.getValue().compareTo(o1.getValue())));

        LinkedHashMap<String, Long> sortedMap = new LinkedHashMap<>();

        for (Map.Entry<String, Long> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;

    }
}
