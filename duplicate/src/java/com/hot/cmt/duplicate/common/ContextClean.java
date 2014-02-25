package com.hot.cmt.duplicate.common;

import java.util.regex.Matcher; 
import java.util.regex.Pattern; 

/**
 * 
 * @author yongleixiao
 * 取出文本中的html特殊标签
 */
public class ContextClean {

    /* 定义script的正则表达式  */
    private static String scriptRegex = "<script[^>]*?>[\\s\\S]*?<\\/script>";
    /* 定义style的正则表达式  */
    private static String styleRegex = "<style[^>]*?>[\\s\\S]*?<\\/style>";
    /*定义HTML标签的正则表达式 */
    private static String tagRegex = "<[^>]+>";
    /* 预先编译正则表达式  */
    private static Pattern scriptPattern = Pattern.compile(scriptRegex, Pattern.CASE_INSENSITIVE);
    private static Pattern stylePattern = Pattern.compile(styleRegex, Pattern.CASE_INSENSITIVE);
    private static Pattern tagPattern = Pattern.compile(tagRegex, Pattern.CASE_INSENSITIVE);

    /* 标点符号正则 */
    private static String punctuationRegex = "[(.|,|\"|\\?|!|:;')]";
    private static Pattern puncPattern = Pattern.compile(punctuationRegex, Pattern.UNICODE_CASE);

    public static String filterHtmlTag(String content) {
        String result = "";
        Matcher match = scriptPattern.matcher(content);
        result = match.replaceAll("");
        match = stylePattern.matcher(result);
        result = match.replaceAll("");
        match = tagPattern.matcher(result);
        result = match.replaceAll("");
        return result;
    }

    public static String filterPunct(String content) {
        String result = "";
        Matcher match = puncPattern.matcher(content);
        result = match.replaceAll("");
        return result;
    }

    public static String filterAll(String content) {
        String result = filterHtmlTag(content);
        return filterPunct(result);
    }

}
