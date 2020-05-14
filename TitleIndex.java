package io.stockgeeks.repository;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class TitleIndex {
    RocksDB db;
	 private Options options;
    static{
        RocksDB.loadLibrary();
    }	

    //constructor 
    public TitleIndex(String dbPath) throws RocksDBException {
		this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.db = RocksDB.open(options, dbPath);
    }

    private Map<String, Map<String,String>> invertedFile=new HashMap<String,Map<String,String>>();

    //change the vector to map
    public Map<String,String> changetoMap(Vector<String>allWords){
        Map<String,String>WordList=new HashMap<String,String>();
        int i=0;
        for(String word:allWords) {
            String currentID=Integer.toString(i);//position
			if(WordList.containsKey(word)){//the word
            //document id
                    WordList.put(word,WordList.get(word)+";;"+currentID);       
            }else{
                WordList.put(word,currentID);
            }
            i++;   
        }
        return WordList;

    }

    //put it into the db
    public void put(String pid, Vector<String> allWords) {
        Map<String,String> out=this.changetoMap(allWords);
        Vector<String> wordList =new Vector<String>();
        for (Map.Entry<String, String> word : out.entrySet()) {
            String temp=Integer.toString(word.getValue().split(";;").length);
			//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
			if(invertedFile.containsKey(word.getKey())){//the word
                if(invertedFile.get(word.getKey()).containsKey(pid)){//document id
                    
                    invertedFile.get(word.getKey()).put(pid,invertedFile.get(word.getKey()).get(pid)+";;"+temp);
                }
                else{
                    invertedFile.get(word.getKey()).put(pid,word.getValue()+";;"+temp);
                    wordList.add(word.getKey());
                }
            }
            else{
                invertedFile.put(word.getKey(),new HashMap<String, String>(){{
					put(pid,word.getValue()+";;"+temp);
			}});
                wordList.add(word.getKey());
            }
        }
        wordList.forEach((key) -> {
            try {
                addEntry(key,pid,invertedFile.get(key).get(pid));
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        });
    }
    
    //put it into the db
    public void put2(String key, String newValue) throws RocksDBException{
            this.db.put(key.getBytes(), newValue.getBytes());
    }

    //get the data by key
    public String get(String key) throws RocksDBException {
        byte[] value = db.get(key.getBytes());
        if (value != null) {
            return new String(value);
        } else
            return null;
    }

    //add the entry
    private void addEntry(String word, String x, String y) throws RocksDBException {
        // Add a "docX Y" entry for the key "word" into hashtable
        // ADD YOUR CODES HERE
        String content = this.get(word);
        if (content != null)
            content = (new String(content) +"---"+ x + ";;" + y);
        else
            content = (x + ";;" + y);
        this.put2(word, content);
    }

    //print all the data
    public void printAll() {
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            System.out.println(new String(iter.key()) + ":" + new String(iter.value()));
        }
        iter.close();
    }

    //get frequency of the index
	public HashMap<String,Integer> getFreq(String key) throws RocksDBException{
		byte[] value = db.get(key.getBytes());
		if (value != null) {
			HashMap<String,Integer> output=new HashMap<>();
			 String out= new String(value);
			 String []res=out.split("---");
			
			 for(int i=0;i<res.length;i++){
				 String []resOut=res[i].split(";;"); 
				 output.put(resOut[0],Integer.parseInt(resOut[resOut.length-1]));
				 
			 }
			 return output;
		} else
			return null;
    }

    //get position of the word
	public HashMap<String,String> getPos(String key)throws RocksDBException{	
		byte[] value = db.get(key.getBytes());
		if (value != null) {
			HashMap<String,String> output=new HashMap<>();
			 String out= new String(value);
			 String []res=out.split("---");
			 for(int i=0;i<res.length;i++){
				 String []resOut=res[i].split(";;"); 
				 String pos="";
				 for(int j=1;j<resOut.length-1;j++){
					 if(j<resOut.length-2){
					 pos+=resOut[j]+" ";
					 }
					 else{
						pos+=resOut[j];
					 }
				 }
				 output.put(resOut[0],pos);
			 }
			 return output;
		} else
			return null;
    } 
    public void removeAll() throws RocksDBException{
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            db.remove(iter.key());   
        }
    }
    
    /*public static void main (String[] args) {
        try {
            InvertedIndex i = new InvertedIndex("/temp");
			Vector<String> test=new Vector<String>();
			Vector<String> test2=new Vector<String>();
			test.add("hello");
			test.add("hello");
			test.add("hello");
			test.add("hello");
			test.add("world");
			test.add("world");
			test2.add("hello");
			test2.add("world");
			i.put("doc1", test);
            i.put("doc2", test2);
			System.out.println(i.get("hello")+"\n\n");
			System.out.println(i.getFreq("hello")+"\n\n");
			System.out.println(i.getPos("hello")+"\n\n");
			i.printAll();
			i.delete("hello");
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }*/
}