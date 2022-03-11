package com.jinyaqia.flink.example.utlls;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author jinyaqia
 * @date 2022/3/11 10:16 上午
 */
public class SqlHelperTest {


    @Test
    public void readSql() {
        System.out.println(SqlHelper.readSql("sql/1.sql"));
    }
}