package com.jinyaqia.flink.sql.utils;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author jinyaqia
 * @date 2022/1/7 11:33 上午
 */
public class SqlUtils {
    public static final char SQL_DELIMITER = ';';
    private static final TableConfig TABLE_CONFIG = new TableConfig();

    private static final String TABLE_SCHEMA_NAME = "name";
    private static final String TABLE_SCHEMA_TYPE = "type";

    private static final String INSERT_SELECT_REGX = "(?i)(insert\\s+into\\s+([\\w\\.]+)\\s?(.*?))(select\\s+.+)";
    private static final Pattern INSERT_SELECT_PATTERN = Pattern.compile(INSERT_SELECT_REGX);


    /**
     * 添加反引号 `
     *
     * @return
     */
    public static String wrapBackticks(ObjectPath objectPath) {
        return "`" + objectPath.getDatabaseName() + "`" + ".`" + objectPath.getObjectName() + "`";
    }

    /**
     * 将 originSql 根据 tableNameMap 的key换成value
     *
     * @param originSql
     * @param tableNameMap
     * @return
     */
    public static String replaceTableName(String originSql, Map<String, String> tableNameMap) {
        String resultSql = originSql;
        for (Map.Entry<String, String> entry : tableNameMap.entrySet()) {
            resultSql = resultSql.replaceAll(entry.getKey(), entry.getValue());
        }
        return resultSql;
    }



    /**
     * sql去除引号等
     *
     * @param sql
     * @return
     */
    public static String trimSql(String sql) {
        return sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();
    }

    /**
     * 根据属性获取ddl中schema的定义
     * <p>
     * 如： schema.0.name='f0', schema.1.data-type=varchar
     * schema.1.name='f1', schema.1.data-type=int
     * 得到  ["f0 varchar", "f1 int"]
     * 用于生成ddl, todo: 需要加上字段表达式的部分,需要升级到1.10以上
     *
     * @param dp
     * @return String[] schema ddl
     */
    public static String[] getFieldDefinedFromProperties(DescriptorProperties dp) {
        int fieldCount = dp.getPropertiesWithPrefix(SCHEMA).keySet().stream()
                .filter((k) -> k.endsWith('.' + TABLE_SCHEMA_NAME))
                .mapToInt((k) -> 1)
                .sum();

        if (fieldCount == 0) {
            return new String[]{};
        }
        final String[] fieldNameType = new String[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            final String nameKey = SCHEMA + '.' + i + '.' + TABLE_SCHEMA_NAME;
            final String typeKey = SCHEMA + '.' + i + '.' + TABLE_SCHEMA_TYPE;
            fieldNameType[i] = '`' + dp.getString(nameKey) + "` " + dp.getString(typeKey);
        }
        return fieldNameType;
    }


    /**
     * 过滤掉schema开头的，即字段的配置
     *
     * @param totalProps
     * @return
     */
    public static Map<String, String> filterSchemaProperties(Map<String, String> totalProps) {
        final Map<String, String> otherProps = new HashMap<>();
        for (Map.Entry<String, String> entry : totalProps.entrySet()) {
            String key = entry.getKey().toLowerCase();
            if (!key.startsWith(SCHEMA)) {
                otherProps.put(key, entry.getValue());
            }
        }
        return otherProps;
    }

    /**
     * 过滤掉partition.keys开头的，即分区字段的配置
     *
     * @param totalProps
     * @return
     */
    public static Map<String, String> filterPartitionProperties(Map<String, String> totalProps) {
        final Map<String, String> otherProps = new HashMap<>();
        for (Map.Entry<String, String> entry : totalProps.entrySet()) {
            String key = entry.getKey().toLowerCase();
            if (!key.startsWith(PARTITION_KEYS)) {
                otherProps.put(key, entry.getValue());
            }
        }
        return otherProps;
    }


    private static final String CONNECTOR = "connector";
    private static final String FORMAT = "format";
    private static final String VALUE_FORMAT = "value.format";
    private static final String SCHEMA = "schema";
    private static final String CONNECTOR_PROPERTIES = "properties.";
    public static final String PARTITION_KEYS = "partition.keys";


    /**
     * 添加with参数
     *
     * @param oriProps
     * @param appendProps
     * @return
     */
    public static Map<String, String> appendConnectorProperties(final Map<String, String> oriProps, Map<String, String> appendProps) {
        Map<String, String> resultMap = new HashMap<>();
        resultMap.putAll(oriProps);
        resultMap.putAll(appendProps);
        return resultMap;
    }


    /**
     * rowkey varchar
     * f1@countryid varchar
     * <p>
     * CREATE TABLE `dim_outlive_anchor` (
     * `rowkey`  VARCHAR,
     * `f1`  ROW< `countryid` VARCHAR >
     * )
     *
     * @param finalColumnNameTypes
     * @return
     */
    public static List<String> getFinalColumnsForHbase(List<String> finalColumnNameTypes) {
        Map<String, String> result = new HashMap<>();
        if (finalColumnNameTypes.size() == 0) {
            return finalColumnNameTypes;
        }
        Map<String, List<String>> row = new HashMap<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < finalColumnNameTypes.size(); i++) {
            if (finalColumnNameTypes.get(i).contains("@")) {
                String tmp = finalColumnNameTypes.get(i).replaceAll("`", "");
                String[] keyArr = tmp.split("@");
                List<String> values = row.get(keyArr[0]);
                if (values == null) {
                    values = new ArrayList<>();
                }
                values.add(tmp.replace(keyArr[0] + "@", ""));
                row.put(keyArr[0], values);
                if (!keys.contains(keyArr[0])) {
                    keys.add(keyArr[0]);
                }
            } else {
                keys.add(finalColumnNameTypes.get(i));
                result.put(finalColumnNameTypes.get(i), finalColumnNameTypes.get(i));
            }
        }

        for (String key : row.keySet()) {
            List<String> values = row.get(key);
            String cf = "";
            for (String cl : values) {
                cf += cl + ",";
            }
            result.put(key, key + " " + String.format("ROW<%s>", cf.substring(0, cf.length() - 1)));
        }

        return new ArrayList<>(result.values());
    }

    /**
     * 获取with参数key的大小，根据此大小对参数进行排序
     * connector.  开头
     * properties. 开头
     * 其他
     * format. 开头
     * schema. 开头
     *
     * @param propsKey
     * @return
     */
    private static int getOrderByString(String propsKey) {
        int priority = 0;
        if (propsKey.startsWith(CONNECTOR_PROPERTIES)) {
            priority = 100;
        } else if (propsKey.startsWith(CONNECTOR)) {
            priority = 0;
        } else if (propsKey.startsWith(FORMAT)) {
            priority = 200;
        } else if (propsKey.startsWith(SCHEMA)) {
            priority = 300;
        } else {
            priority = 400;
        }
        return priority;
    }

    /**
     * with参数排序
     *
     * @param propsMap 未排序的map
     * @return map 排好序的treeMap
     */
    public static Map<String, String> getSortedMap(Map<String, String> propsMap) {
        return propsMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey((o1, o2) -> {
                    int rank1 = getOrderByString(o1);
                    int rank2 = getOrderByString(o2);
                    return rank1 - rank2;
                }))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldVal, newVal) -> oldVal,
                        LinkedHashMap::new
                ));
    }

    /**
     * 获取with参数排序后的字符串
     *
     * @param propsMap
     * @return
     */
    public static String getSortedWithPropsString(Map<String, String> propsMap) {
        Map<String, String> sortedMap = getSortedMap(propsMap);
        return sortedMap.entrySet().stream()
                .map(entry -> "'" + entry.getKey() + "' = '" + entry.getValue() + "'")
                .collect(Collectors.joining(","));
    }


    /**
     * 从ddl的with参数里获取需要哪些flink connector lib
     *
     * @param withProps
     * @return
     */
    public static Set<String> getFlinkConnector(Map<String, String> withProps) {
        Set<String> connector = new HashSet<>();
        if (withProps.containsKey(CONNECTOR)) {
            connector.add(withProps.get(CONNECTOR));
        }
        if (withProps.containsKey(FORMAT)) {
            connector.add(withProps.get(FORMAT));
        }
        if (withProps.containsKey(VALUE_FORMAT)) {
            connector.add(withProps.get(VALUE_FORMAT));
        }
        return connector;
    }


    public static List<String> splitIgnoreQuota(String str, char delimiter) {
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        int bracketLeftNum = 0;
        StringBuilder b = new StringBuilder();
        char[] chars = str.toCharArray();
        int idx = 0;
        for (char c : chars) {
            char flag = 0;
            if (idx > 0) {
                flag = chars[idx - 1];
            }
            if (c == delimiter) {
                if (inQuotes) {
                    b.append(c);
                } else if (inSingleQuotes) {
                    b.append(c);
                } else if (bracketLeftNum > 0) {
                    b.append(c);
                } else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            } else if (c == '\"' && '\\' != flag && !inSingleQuotes) {
                inQuotes = !inQuotes;
                b.append(c);
            } else if (c == '\'' && '\\' != flag && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            } else if (c == '(' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum++;
                b.append(c);
            } else if (c == ')' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum--;
                b.append(c);
            } else {
                b.append(c);
            }
            idx++;
        }
        String el = b.toString().trim();
        if (el.endsWith("" + delimiter)) {
            el = el.substring(0, el.length() - 1);
        }
        tokensList.add(el);

        return tokensList;
    }
}
