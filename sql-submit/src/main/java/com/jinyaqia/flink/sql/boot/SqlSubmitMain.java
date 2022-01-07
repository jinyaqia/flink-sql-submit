package com.jinyaqia.flink.sql.boot;

import com.jinyaqia.flink.sql.cli.CliOptionsParser;

/**
 * @author jinyaqia
 * @date 2022/1/7 11:39 上午
 */
public class SqlSubmitMain {
    public static void main(String[] args) throws Exception {
        final CliOptionsParser parser = CliOptionsParser.parseClient(args);
        if (parser.getCliOptions().isHelp()) {
            parser.getJCommander().usage();
            return;
        }
        SqlSubmit submit = new SqlSubmit(parser.getCliOptions());
        if (parser.getCliOptions().isExplain()) {
            submit.explain();
        } else {
            submit.run();
        }
    }
}
