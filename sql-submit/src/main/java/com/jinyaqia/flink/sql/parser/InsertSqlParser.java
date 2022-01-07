package com.jinyaqia.flink.sql.parser;

import com.jinyaqia.flink.sql.job.JobEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @Author jinyaqia
 * @Date 2019-12-11 15:43
 */
@Slf4j
public class InsertSqlParser implements IParser {

    public static InsertSqlParser newInstance() {
        return new InsertSqlParser();
    }

    @Override
    public boolean verify(String sql) {
        return StringUtils.isNotBlank(sql) && sql.trim().toLowerCase().startsWith("insert");
    }

    @Override
    public void parseAndRegisterSql(String sql, JobEnv jobEnv) {
        jobEnv.addInsert(sql);
    }

}
