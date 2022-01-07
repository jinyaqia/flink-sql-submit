package com.jinyaqia.flink.sql.parser;


import com.jinyaqia.flink.sql.job.JobEnv;

public interface IParser {

    boolean verify(String sql);

    void parseAndRegisterSql(String sql, JobEnv jobEnv);
}
