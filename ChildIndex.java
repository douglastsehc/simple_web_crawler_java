package io.stockgeeks.repository;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import java.util.*;
//find the parent
public class ChildIndex {
    RocksDB db;
	 private Options options;
    static{
        RocksDB.loadLibrary();
    }	
    
    //constructor
    public ChildIndex(String dbPath) throws RocksDBException {
		this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.db = RocksDB.open(options, dbPath); 
    }
    
    //put word into the db
    public void put(String key, String newValue) throws RocksDBException{
            this.db.put(key.getBytes(), newValue.getBytes());
    }

    //get the data by giving the string key
    public String get(String key) throws RocksDBException {
        byte[] value = db.get(key.getBytes());
        if (value != null) {
            return new String(value);
        } else
            return null;
    }

    // print all the data
    public void printAll() {
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            System.out.println( new String(iter.key()) + "::" + new String(iter.value()) );
        }
        iter.close();
    }

    //get all data and put it in a hash map.
    public HashMap<String, String> getAll () {
        HashMap<String, String> content = new HashMap<>();
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            content.put(new String(iter.key()), new String(iter.value()));
        }
        iter.close();
        return content;
    }

    public Vector<String> getChild(String URL) throws RocksDBException {
        String ChildidList = this.get(URL);
        if(ChildidList!= null){
        String []rawParentsList=ChildidList.split("::");
        
            Vector<String> result= new Vector<String>();
            for(String out:rawParentsList){
                result.add(out);
            }
            return result;
        }
        return new Vector<String>();
    }
    public void removeAll() throws RocksDBException{
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            db.remove(iter.key());   
        }
    }
    /*public static void main (String[] args) {
        try {
            ChildIndex db = new ChildIndex("/tempchild");
            db.put("hello", "world");
            System.out.println(db.get("hello"));
            System.out.println(db.get("non"));

            db.printAll();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    } */   
}