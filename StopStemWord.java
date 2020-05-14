package io.stockgeeks.repository;
import io.stockgeeks.repository.IRUtilities.*;
import java.io.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;
public class StopStemWord {
    private Porter porter;
    private Vector<String> stopWords;

    public StopStemWord() {
        super();
        porter = new Porter();
        stopWords=new Vector<>();
        File f = new File("/home/isabella/Documents/4321/ProjFinal/rocksdbBootApp-master/src/main/java/io/stockgeeks/repository/stopwords.txt");
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	public boolean isStopWord(String str)
	{
		return stopWords.contains(str);
	}

    public String stem(String str) {
        return porter.stripAffixes(str);
    }

    public Vector<String> deleteSpecialSymbol(Vector<String> words) {
        Vector<String> output = new Vector<String>();
        // System.out.println("Keywords in "+crawler_child.url+":");
        for (int j = 0; j < words.size(); j++) {
            // String strippedInput = (words.get(i)).replaceAll("\\W", " ");
            String strippedInput = words.get(j);
            // System.out.print(strippedInput + " ");
            // count=count+1;
            boolean endisDelimiter = strippedInput.endsWith(".") || strippedInput.endsWith(",")
                    || strippedInput.endsWith(":") || strippedInput.endsWith(";") || strippedInput.endsWith("?")
                    || strippedInput.endsWith("!") || strippedInput.endsWith("…");
            boolean startisDelimiter = strippedInput.startsWith("{") || strippedInput.startsWith("(")
                    || strippedInput.startsWith("[");

            boolean endWithStartDelimiter = strippedInput.endsWith("]") || strippedInput.endsWith(")")
                    || strippedInput.endsWith("}");
            if (endisDelimiter)
                strippedInput = strippedInput.substring(0, strippedInput.length() - 1);
            /*if (startisDelimiter)
                strippedInput = strippedInput.substring(1,strippedInput.length());
            if (endWithStartDelimiter)
                strippedInput = strippedInput.substring(0,strippedInput.length()-1);*/
            // System.out.println(strippedInput);
            // If want to filter all words that contain only digit
            if (strippedInput.matches("[0-9]+")) {
                continue;
            }
            if(strippedInput!=null){
                output.add(strippedInput);
            }
        }
        return output;

    }

    public Vector<String> process(Vector<String> words) {
        Vector<String> temp = new Vector<>();
        for (String s : words) {
            if (!isStopWord(s)) {
                temp.add(stem(s));
            }
        }
        return temp;
    }
    /*
    public Vector<String> toBigram(Vector<String> content) {
        Vector<String> temp = new Vector<>();
        System.out.println("---------------------------toBigram --------------------------");
        for (int i = 0; i < content.size() - 1; i++) {
            String nstr = content.get(i) + " " + content.get(i + 1);
            // System.out.println(nstr);
            temp.add(nstr);
        }
        temp.addAll(content);
        return temp;
    }
    public Vector<String> toTrigram(Vector<String> content) {
        Vector<String> temp = new Vector<>();
        System.out.println("---------------------------toTrigram --------------------------");
        for (int i = 0; i < content.size() - 2; i++) {
            String nstr = content.get(i) + " " + content.get(i + 1)+content.get(i+2);
            // System.out.println(nstr);
            temp.add(nstr);
        }
        temp.addAll(content);
        return temp;
    }
    public Vector<String> to4gram(Vector<String> content) {
        Vector<String> temp = new Vector<>();
        System.out.println("---------------------------to4gram --------------------------");
        for (int i = 0; i < content.size() - 3; i++) {
            String nstr = content.get(i) + " " + content.get(i + 1)+content.get(i+2)+content.get(i+3);
            // System.out.println(nstr);
            temp.add(nstr);
        }
        temp.addAll(content);
        return temp;
    } */
}
