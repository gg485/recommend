package recommend.demo.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;

public class Temp {
    public static void main(String[] args) throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec("cmd /c hdfs dfs -ls /");
        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        while((line=br.readLine())!=null){
            System.out.println(line);
        }
        p.waitFor();
        p.destroy();
    }
}
