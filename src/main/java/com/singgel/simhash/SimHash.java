package com.singgel.simhash;

import com.huaban.analysis.jieba.JiebaSegmenter;
import lombok.Data;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author chenyuan
 * @description
 * @created_at: 2020-06-01 14:23
 **/
@Data
public class SimHash {

    private String sentence;

    private BigInteger strSimHash;

    private int hashBits = 128;

    private static final String[] cutLineFlag = {"？", "！", "。", "…","】"};

    public SimHash(String tokens) {
        this.sentence = tokens;
        this.strSimHash = this.simHash();
    }

    public SimHash(String sentence, int hashBits) {
        this.sentence = sentence;
        this.hashBits = hashBits;
        this.strSimHash = this.simHash();
    }

    public BigInteger simHash() {
        int[] v = new int[this.hashBits];
        List<String> words = cutSentenceToWords(sentence);
        for (String word : words) {
            BigInteger t = this.hash(word);
            for (int i = 0; i < this.hashBits; i++) {
                BigInteger bitmask = new BigInteger("1").shiftLeft(i);
                if (t.and(bitmask).signum() != 0) {
                    v[i] += 1;
                } else {
                    v[i] -= 1;
                }
            }
        }
        BigInteger fingerprint = new BigInteger("0");
        for (int i = 0; i < this.hashBits; i++) {
            if (v[i] >= 0) {
                fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i));
            }
        }
        return fingerprint;
    }

    private BigInteger hash(String source) {
        if (source == null || source.length() == 0) {
            return new BigInteger("0");
        } else {
            char[] sourceArray = source.toCharArray();
            BigInteger x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
            BigInteger m = new BigInteger("1000003");
            BigInteger mask = new BigInteger("2").pow(this.hashBits).subtract(
                    new BigInteger("1"));
            for (char item : sourceArray) {
                BigInteger temp = BigInteger.valueOf((long) item);
                x = x.multiply(m).xor(temp).and(mask);
            }
            x = x.xor(new BigInteger(String.valueOf(source.length())));
            if (x.equals(new BigInteger("-1"))) {
                x = new BigInteger("-2");
            }
            return x;
        }
    }

    public int hammingDistance(SimHash other) {
        BigInteger m = new BigInteger("1").shiftLeft(this.hashBits).subtract(
                new BigInteger("1"));
        BigInteger x = this.strSimHash.xor(other.strSimHash).and(m);
        int tot = 0;
        while (x.signum() != 0) {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    /**
     * 将每句话分词
     *
     * @param sentence
     * @return
     */
    public static List<String> cutSentenceToWords(String sentence) {

        JiebaSegmenter segmenter = new JiebaSegmenter();

        return segmenter.process(sentence, JiebaSegmenter.SegMode.SEARCH).stream().map(e -> e.word).collect(Collectors.toList());

    }

    /**
     * 将每篇文章分句
     *
     * @param text
     * @return
     */
    public static List<String> cutTextToSentenceList(String text) {
        List<String> sentenceList = new ArrayList<>();

        text = text.trim().replaceAll("</p>", "。").replaceAll("\n", "。").replaceAll("\r", "。");
        StringBuilder singleSentence = new StringBuilder();
        Set<String> cutLineFlagSet = new HashSet<>(Arrays.asList(cutLineFlag));
        char[] words = text.toCharArray();
        for (char word : words) {
            singleSentence.append(word);
            if (cutLineFlagSet.contains(word + "")) {
                if (singleSentence.toString().length() > 4) {
                    sentenceList.add(singleSentence.toString());
                }
                singleSentence = new StringBuilder();
            }
        }
        return sentenceList;
    }
}
