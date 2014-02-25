package com.hot.cmt.test;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hot.cmt.duplicate.Shingle;

public class TestDuplicate {

    private static final Logger LOG = LoggerFactory.getLogger(TestDuplicate.class);
    private static Shingle shinger = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        shinger = new Shingle("11.11.72.121",9911,0);
    }

    @Test
    public void testShinleService() throws Exception {
        String content = "长沙市民范先生拨打本报新闻热线96258：真是逆天了！我家楼下超市老板娘家里的老猫生了4只小猫，上周五，有只胆大包天的老鼠，竟然冲进猫窝，把小猫咬死了两只，咬伤了一只。幸好老板娘威武霸气，发现之后两棍子打死了老鼠，才保住了最后一只小猫。";
        List<String> shingles = shinger.getShingleString(content);
        for (int i = 0; i < 6; i++){
            LOG.info("{}@{}",shingles.get(i), i);
        }
    }
    @AfterClass
    public static void tearDown() {
        shinger.close();
    }
}
