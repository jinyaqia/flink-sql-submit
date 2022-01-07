package com.jinyaqia.flink.sql.boot;

import com.jinyaqia.flink.sql.cli.CliOptions;
import com.jinyaqia.flink.sql.cli.CliOptionsParser;
import com.jinyaqia.flink.sql.job.JobEnv;
import com.jinyaqia.flink.sql.job.JobGen;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * @author jinyaqia
 * @date 2022/1/7 2:28 下午
 */
@Slf4j
public class SqlSubmit {
    private String sqlStr;
    private String jobName;
    private JobEnv jobEnv;

    public SqlSubmit(CliOptions options) throws UnsupportedEncodingException {
        if (StringUtils.isNotBlank(options.getSqlFile())) {
            this.sqlStr = CliOptionsParser.readToString(options.getSqlFile());
        } else {
            this.sqlStr = URLDecoder.decode(options.getSqlEncode(), StandardCharsets.UTF_8.name());
        }
        assert StringUtils.isNotBlank(this.sqlStr);
        this.jobName = options.getJobName();
        log.info("jobName={}, sqlStr={}", jobName, sqlStr);
    }

    public void run() throws Exception {
        this.jobEnv = JobGen.parseAndGenJob(this.sqlStr);
        this.jobEnv.run(this.jobName);
    }

    public void explain() throws Exception {
        this.jobEnv = JobGen.parseAndGenJob(this.sqlStr);
        System.out.println(this.jobEnv.explain());
    }
}
