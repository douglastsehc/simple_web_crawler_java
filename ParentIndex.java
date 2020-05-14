package io.stockgeeks.repository;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import java.util.*;

public class ParentIndex {
    RocksDB db;
	 private Options options;
    static{
        RocksDB.loadLibrary();
    }	

    //constructor 
    public ParentIndex(String dbPath) throws RocksDBException {
		this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.db = RocksDB.open(options, dbPath);
    }

    //put the data into db
    public void put(String key, String newValue) throws RocksDBException{
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

    //print all the data
    public void printAll() {
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            System.out.println(new String(iter.key()) + ";;" + new String(iter.value()));
        }
        iter.close();
    }

    //get all the data
    public HashMap<String, String> getAll () {
        HashMap<String, String> content = new HashMap<>();
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            content.put(new String(iter.key()), new String(iter.value()));
        }
        iter.close();
        return content;
    }

    //get parents of the db
    public Vector<String> getParents(String pid) throws RocksDBException {
        String ChildidList = this.get(pid);
		ChildidList=ChildidList.substring(1,ChildidList.length()-1);
		//System.out.println(ChildidList);
		String []rawParentsList=ChildidList.split(", ");
		//System.out.println(rawParentsList);
        if(ChildidList!= null){
        //String []rawParentsList=ChildidList.split(", ");
        
            Vector<String> result= new Vector<String>();
            for(String out:rawParentsList){
                result.add(out);
            }
            return result;
        }
        return new Vector<String>();
    }

    //add the parents for the child url
    public void addParent(String pid, String parentPid) throws RocksDBException {
        String old = this.get(pid);
        if (old == null) {
            this.put(pid, parentPid);
        } else {
            this.put(pid, old + ";;" + parentPid);
        }
    }
    public void removeAll() throws RocksDBException{
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            db.remove(iter.key());   
        }
    }
}