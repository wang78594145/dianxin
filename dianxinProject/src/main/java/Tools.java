package dianxinProject;

import org.apache.hadoop.util.Tool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Tools {
    // 求标准差
    public static double stand_dev(ArrayList<Integer> list) {
        if (list.size()==1){
            return list.get(0);
        }else if (list.size()>1){
            float len = list.size();
            float sum = 0;
            for(int a : list){
                sum += a;
            }
            float avg = sum / len;
            float square = 0;
            for(int a : list){
                square += (a - avg)*(a - avg);
            }
            return Math.sqrt((double)(square/len));
        }
        return -1;
    }
}
