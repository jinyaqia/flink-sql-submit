package com.jinyaqia.flink.sql.boot;

import org.junit.Test;

/**
 * @author jinyaqia
 * @date 2022/1/10 7:48 下午
 */

public class SqlSubmitTest {

    public void test(String num) {
        String[] on = {"零", "一", "二", "三", "四", "五", "六", "七", "八", "九"};
        String[] units = {"","十","百","千","万","十","百","千","亿","十","百","千","万" };
        int len = num.length();
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<len;i++){
            int n = Integer.parseInt(String.valueOf(num.charAt(i)));
            boolean isZero = n==0;
            if (isZero) {
                if('0'==num.charAt(i-1)) continue;
                sb.append(on[n]);
            }else{
                sb.append(on[n]).append(units[len-i-1]);
            }
        }
        System.out.println(sb.toString());
    }

    @Test
    public void t1(){
//        test("123456789");

        test("100110010102");
    }


}