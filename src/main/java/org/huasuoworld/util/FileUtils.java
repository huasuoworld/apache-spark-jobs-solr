package org.huasuoworld.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

public class FileUtils {
	
	public static void main(String[] args) {
		String inputfile = "C:/Users/hua/Desktop/hbase/all/all.csv";
		String outputfile = "C:/Users/hua/Desktop/hbase/all/all_.csv";
		test(inputfile,outputfile);
	}

	public static void test(String inputfile, String outputfile) {
		FileReader fr = null;
		FileWriter fw = null;
		try {
			File infile = new File(inputfile);
			fr = new FileReader(infile);
			
			File oufile = new File(outputfile);
			fw = new FileWriter(oufile);
			PrintWriter pw = new PrintWriter(fw);
			
			String line = "";
			BufferedReader br = new BufferedReader(fr);
			while((line = br.readLine()) != null) {
				pw.println(line.replace("/", "-"));
			}
			pw.close();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(fr != null) {
					fr.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
			try {
				if(fw != null) {
					fw.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}
