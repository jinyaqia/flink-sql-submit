package com.jinyaqia.flink.sql.parser;

import com.jinyaqia.flink.sql.job.JobEnv;
import lombok.extern.slf4j.Slf4j;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author jinyaqia
 * @Date 2019-12-11 10:41
 */
@Slf4j
public class EnvSettingParser implements IParser {
    private static final String PATTERN_STR = "(?i)SET\\s+(\\S+)\\s*=\\s*(.*)?";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    public static EnvSettingParser newInstance() {
        return new EnvSettingParser();
    }

    @Override
    public boolean verify(String sql) {
        return PATTERN.matcher(sql).find();
    }

    @Override
    public void parseAndRegisterSql(String sql, JobEnv jobEnv) {
        Matcher matcher = PATTERN.matcher(sql);
        if (matcher.find()) {
            String key = matcher.group(1).trim();
            String value = matcher.group(2).trim();
            log.info("key=>{} value=>{}\n", key, value);
            jobEnv.getTEnv().getConfig().getConfiguration().setString(key, value);
        }
    }

}
