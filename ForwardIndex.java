
package javafile;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import java.util.*;

public class ForwardIndex{
    RocksDB db;
	 private Options options;
    final public static String DELIM = ";;";
    static{
        RocksDB.loadLibrary();
    }	
    
    //constructor 
    public ForwardIndex(String dbPath) throws RocksDBException {
		this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.db = RocksDB.open(options, dbPath);
    }

    //put data into the db
    public void put(String pid, Vector<String> words) throws RocksDBException{
        Set<String> set = differentWords(words);
        String value = "";
        Iterator<String> itr = set.iterator();
        while(itr.hasNext()){
            String word = itr.next();
            int freq = Collections.frequency(words, word);
            value += word+"="+String.valueOf(freq)+"--";
        }
        this.db.put(pid.getBytes(), value.getBytes());
    }

    //make the words into different and put in the DB
    public Set<String> differentWords(Vector<String> words){
        Set set = new HashSet();
        set.addAll(words);
        return set;
    }

    //get words by key
    public String get(String key) throws RocksDBException {
        byte[] value = db.get(key.getBytes());
        if (value != null) {
            return new String(value);
        } else
            return null;
    }

    //getFreq of the word;
    public HashMap<String,Integer> getFreq(String key) throws RocksDBException {
        byte[] value = db.get(key.getBytes());
        if (value != null) {
			HashMap<String,Integer> output =new HashMap<String,Integer>();
            String out=new String(value);
			String []res=out.split("--");
			for(int i=0;i<res.length;i++){
				String []resOut=res[i].split("="); 
				 output.put(resOut[0],Integer.parseInt(resOut[1]));
			}
			return output;
        } else
            return null;
    }	

    //print all the word
    public void printAll() {
        RocksIterator iter = db.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + ":" + new String(iter.value()));
        }
        iter.close();
    }
    //get all the word
    public HashMap<String, String> getAll () {
        HashMap<String, String> content = new HashMap<>();
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            content.put(new String(iter.key()), new String(iter.value()));
        }
        iter.close();
        return content;
    }   

    public static HashMap<String, Integer> sortByValue(HashMap<String, Integer> hm) 
    { 
        // Create a list from elements of HashMap 
        List<Map.Entry<String, Integer> > list = 
               new LinkedList<Map.Entry<String, Integer> >(hm.entrySet()); 
  
        // Sort the list 
        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() { 
            public int compare(Map.Entry<String, Integer> o1,  
                               Map.Entry<String, Integer> o2) 
            { 
                return (o1.getValue()).compareTo(o2.getValue()); 
            } 
        }); 
          
        // put data from sorted list to hashmap  
        HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>(); 
        for (Map.Entry<String, Integer> aa : list) { 
            temp.put(aa.getKey(), aa.getValue()); 
        } 
        return temp; 
    }
    public void removeAll() throws RocksDBException{
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            db.remove(iter.key());   
        }
    }
    //testing
    /*public static void main (String[] args) {
        try {
            ForwardIndex i = new ForwardIndex("/testt");
            Vector<String> a = new Vector<>();
            a.add("hello");
            a.add("hello");
			a.add("hello");
            a.add("world");
            Vector<String> b = new Vector<>();
            b.add("hello");
            b.add("world");
            b.add("hello");

            i.put("1", a);
            i.put("2", b);
			System.out.println(i.getFreq("1")+"\n\n");
			System.out.println(i.getFreq("2")+"\n\n");
			i.printAll();
			i.delete("1");
			i.delete("2");
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }*/
}