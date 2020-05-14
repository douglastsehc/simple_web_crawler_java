package io.stockgeeks.repository;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.*;

public class MetaIndex {
    RocksDB db;
	 private Options options;
    final public static String DELIMITER = ";;";
    static{
        RocksDB.loadLibrary();
    }	

    //constructor
    public MetaIndex(String dbPath) throws RocksDBException {
		this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.db = RocksDB.open(options, dbPath);
    }

    //put the data into db
    public void put(String pid, String title, String url, long date, int size)
            throws RocksDBException {
        String values = String.join(DELIMITER, title, url, Long.toString(date), Integer.toString(size));
        this.db.put(pid.getBytes(), values.getBytes());
    }

    //get the title by the page id
    public String getTitle(String pid) throws RocksDBException {
        return this.get(pid).split(DELIMITER)[0];
    }

    //get the url by the page id
    public String getURL(String pid) throws RocksDBException {
        if (this.get(pid) != null) {
            return this.get(pid).split(DELIMITER)[1];
        } else {
            return null;
        }
    }

    //get the date of the page id
    public Date getDate(String pid) throws RocksDBException {
        if(this.get(pid)!=null){
            return new Date(Long.parseLong(this.get(pid).split(DELIMITER)[2]));
        }
        return null;
    }

    //get the size by the page id
    public int getSize(String pid) throws RocksDBException {
        if(this.get(pid)!=null){
        return Integer.parseInt(this.get(pid).split(DELIMITER)[3]);
        }
        return 0;
    }

    //get the key
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
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + ";;" + new String(iter.value()) );
        }
        iter.close();
    }    
    public void removeAll() throws RocksDBException{
        RocksIterator iter = db.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            db.remove(iter.key());   
        }
    }
    /*public static void main(String [] args){
        //(String pid, String title, String url, long date, int size, String parent, String child)
	try {	
        MetaIndex newIndex=new MetaIndex("/meta");
        newIndex.put("1","testing123","www.testing.com",new java.util.Date().getTime(), 100);
        newIndex.put("2","testing1234","www.testing.comm",new java.util.Date().getTime(), 101);
        newIndex.put("3","testing12345","www.testing.commm,",new java.util.Date().getTime(), 102);
        newIndex.put("4","testing123456","www.testing.commmm",new java.util.Date().getTime(), 103);
        newIndex.printAll();
        System.out.println(newIndex.getDate("1"));
        System.out.println(newIndex.getSize("1"));
        System.out.println(newIndex.getURL("1"));
        System.out.println(newIndex.getTitle("1"));   
		} catch (RocksDBException e) {
            e.printStackTrace();
        }		          
    }*/    
}