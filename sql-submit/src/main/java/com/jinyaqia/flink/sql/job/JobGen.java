package com.jinyaqia.flink.sql.job;

import com.jinyaqia.flink.sql.parser.EnvSettingParser;
import com.jinyaqia.flink.sql.parser.IParser;
import com.jinyaqia.flink.sql.parser.InsertSqlParser;
import com.jinyaqia.flink.sql.parser.OtherParser;
import com.jinyaqia.flink.sql.utils.SqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

import static com.jinyaqia.flink.sql.utils.SqlUtils.SQL_DELIMITER;


/**
 * @Author jinyaqia
 * @Date 2019-12-09 15:45
 */
@Slf4j
public class JobGen {

    private static final List<IParser> SQL_PARSER_LIST = Arrays.asList(
            EnvSettingParser.newInstance(),
            InsertSqlParser.newInstance(),
            OtherParser.newInstance());

    private static void parseAndRegisterSql(String sql, JobEnv jobEnv) {
        if (StringUtils.isBlank(sql)) {
            throw new RuntimeException("sql is not null");
        }
        sql = sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();
        List<String> sqlArr = SqlUtils.splitIgnoreQuota(sql, SQL_DELIMITER);
        for (String childSql : sqlArr) {
            if (StringUtils.isBlank(childSql)) {
                continue;
            }
            boolean result = false;
            for (IParser sqlParser : SQL_PARSER_LIST) {
                if (!sqlParser.verify(childSql)) {
                    continue;
                }
                sqlParser.parseAndRegisterSql(childSql, jobEnv);
                result = true;
                break;
            }
            if (!result) {
                throw new RuntimeException(String.format("%s :Syntax does not support.", childSql));
            }
        }

    }

    public static JobEnv parseAndGenJob(String sqlStr) throws Exception {
        JobEnv jobEnv = new JobEnv();
        parseAndRegisterSql(sqlStr, jobEnv);
        return jobEnv;
    }



}
