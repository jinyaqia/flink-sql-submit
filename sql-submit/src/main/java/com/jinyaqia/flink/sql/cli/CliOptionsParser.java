package com.jinyaqia.flink.sql.cli;

import com.beust.jcommander.JCommander;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

@Slf4j
@Data
public class CliOptionsParser {

    private JCommander jCommander;
    private CliOptions cliOptions = new CliOptions();

    private CliOptionsParser(){
        this.jCommander = JCommander.newBuilder()
                .acceptUnknownOptions(true)
                .allowAbbreviatedOptions(true)
                .allowParameterOverwriting(true)
                .addObject(cliOptions).build();
    }

    public static CliOptionsParser parseClient(String[] args)  {
        log.info("args={}", Arrays.toString(args));
        CliOptionsParser cliOptionsParser = new CliOptionsParser();
        cliOptionsParser.jCommander.parse(args);
        return cliOptionsParser;
    }

    public static String readToString(String fileName) {
        String encoding = "UTF-8";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            System.err.println("The OS does not support " + encoding);
            e.printStackTrace();
            return null;
        }
    }
}
