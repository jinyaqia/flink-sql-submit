package com.jinyaqia.flink.example.utlls;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author jinyaqia
 * @date 2022/3/11 10:05 上午
 */
public class SqlHelper {

    public static String readSql(String sqlPath) {
        URL path = SqlHelper.class.getClassLoader().getResource(sqlPath);
        byte[] ori = new byte[0];
        try {
            ori = Files.readAllBytes(Paths.get(path.toURI()));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return new String(ori);
    }
}
