package com.jinyaqia.flink.sql.boot;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jinyaqia
 * @date 2022/1/17 8:21 下午
 */
public class DemoTest {

//  输入:
//[
// [ 1, 2, 3 ],
// [ 4, 5, 6 ],
// [ 7, 8, 9 ]
//]
//输出: [1,2,3,6,9,8,7,4,5]
//
//示例 2:
//输入:
//[
//  [1, 2, 3, 4],
//  [5, 6, 7, 8],
//  [9,10,11,12]
//]
//输出: [1,2,3,4,8,12,11,10,9,5,6,7]

    public static void main(String[] args) {
        int[][] matrix = new int[3][3];
        int a=1;
        for(int i=0;i<3;i++){
            for(int j=0;j<3;j++){
                matrix[i][j] = a++;
            }
        }
        System.out.println(test(matrix));

        int[][] m2 = new int[3][4];
        int b=1;
        for(int i=0;i<3;i++){
            for(int j=0;j<4;j++){
                m2[i][j] = b++;
            }
        }
        System.out.println(te1(m2));
    }
    public static List<Integer> test(int[][] matrix){
        List<Integer> res = new ArrayList<>();
        int h= matrix.length;
        int w = h==0?0:matrix[0].length;
        int size = h * w;
        int x = 0,y=-1,d=0;
        //d 0 1 2 3  右下左上
        int[] dx = {0,1,0,-1};
        int[] dy = {1,0,-1,0};
        for(int step, totalRun = 0;totalRun<size;totalRun+=step){
            if(d==0||d==2){
                step=w;
                h--;
            }else{
                step=h;
                w--;
            }
            for(int i=step;i>0;i--){
                x+=dx[d];
                y+=dy[d];
                res.add(matrix[x][y]);
            }
            d = (++d)%4;
        }
        return res;
    }

    public static int[] te1(int[][] matrix){
        int m = matrix.length;
        int n = m==0?0:matrix[0].length;
        int l=0,r=m-1,t=0,b=n-1;
        int total = m*n;
        int[] arr = new int[total];
        int c = 0;

        while(true){
            for(int i=l;i<=r;i++){
                arr[c++] = matrix[t][i];
            }
            if(++t >b) break;
            for(int i=t;i<=b;i++){
                arr[c++] = matrix[i][r];
            }
            if(--r <l) break;
            for(int i=r;i>=l;i--){
                arr[c++]  = matrix[b][i];
            }
            if(--b <t) break;
            for(int i =b;i>=t;i--){
                arr[c++] = matrix[i][l];
            }
            if(++l >r) break;
        }
        return arr;
    }
}
