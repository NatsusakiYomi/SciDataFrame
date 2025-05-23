package com.example;
import java.io.*;
public class SaveFileReader {
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("/usr/local/airflow/3000files.txt"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("/usr/local/airflow/java_output.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            bw.write(line);
            bw.newLine();
        }
        br.close();
        bw.close();
    }
}