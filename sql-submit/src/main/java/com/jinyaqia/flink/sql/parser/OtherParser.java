package com.jinyaqia.flink.sql.parser;

import com.jinyaqia.flink.sql.job.JobEnv;

/**
 * @Author jinyaqia
 * @Date 2019-12-11 15:43
 */

public class OtherParser implements IParser {

    public static OtherParser newInstance() {
        return new OtherParser();
    }

    @Override
    public boolean verify(String sql) {
        return true;
    }

    @Override
    public void parseAndRegisterSql(String sql, JobEnv jobEnv) {
        jobEnv.executeSql(sql).print();
    }

}
