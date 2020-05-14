package javafile;

//import url.*;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URLConnection;
import java.util.*;

import org.htmlparser.beans.StringBean;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.tags.TitleTag;
import org.htmlparser.util.NodeIterator;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
import org.htmlparser.beans.LinkBean;
import java.net.URL;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Crawler {
    //final static private String ROOT_DIR = "/root/comp4321/phase11/";
    public String url;
    public String host;
	private final int N_PAGES;    

    private MetaIndex metaDB;
    private ForwardIndex forwardIndex;
    private InvertedIndex invertedIndex;
    private TitleIndex titleIndex;
    private ChildIndex childIndex;
    private ParentIndex parentIndex;
    private LinkedList<String> queue = new LinkedList<>();
    private HashMap<String, String> urlstatus = new HashMap<>();  
    private final String FINISHED = "FINISHED";
	private final String DISCOVERED = "DISCOVERED";

  
    //constructor initialize different indexer
    public Crawler(String _url, int count, MetaIndex metaDB, ForwardIndex forwardIndex, InvertedIndex invertedIndex,TitleIndex titleIndex,
    ParentIndex parentIndex, ChildIndex childIndex) throws MalformedURLException {
		url = _url;
		host = new URL(_url).getHost();
        this.N_PAGES = count;
        this.metaDB = metaDB;
        this.forwardIndex = forwardIndex;
        this.invertedIndex = invertedIndex;
        this.titleIndex=titleIndex;
        this.parentIndex=parentIndex;
        this.childIndex=childIndex;
    }

    // get the url title
    public String getTitle(String URL) throws ParserException {
        String Title = new String();
        Parser parser = new Parser(URL);
        NodeClassFilter TitleTag = new NodeClassFilter(TitleTag.class);
        NodeList TagsList = parser.parse(TitleTag);

        for (int i = 0; i < TagsList.size(); ++i) {
            if (TagsList.elementAt(i) instanceof TitleTag)
                Title = ((TitleTag) TagsList.elementAt(i)).getTitle();
        }
        return Title;
    }

    //get the last modification date, if no date then will be the date of today
    public long extractLastModificationDate(String URL) throws IOException {
        URL target_page = new URL(URL);
        URLConnection HTTPConnection = target_page.openConnection();
        Date LastModificationDate = new Date(HTTPConnection.getLastModified());
        Date gettdate = new Date(HTTPConnection.getDate());
        if (HTTPConnection.getLastModified() != 0) {
            // System.out.println("no date!!!!!!");
            return (HTTPConnection.getLastModified());
        }
        return (HTTPConnection.getDate());
    }
    // get the page size of the url (bytes)
    public int getPageSize(String URL) {
        String content = "";
        URLConnection HTTPConnection = null;
        try {
            String temp_string = "";
            String cur_line = "";
            URL target_page = new URL(URL);
            HTTPConnection = target_page.openConnection();
            InputStreamReader URL_reader = new InputStreamReader(HTTPConnection.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(URL_reader);
            for (int i = 0;; ++i) {
                if ((cur_line = bufferedReader.readLine()) == null)
                    break;
                temp_string = temp_string + cur_line;
            }
            bufferedReader.close();
            content = temp_string;
        } catch (IOException e) {
            return -1;
        } finally {
        }
        return content.length();
    }

    // extract the keywords 
    public Vector<String> extractWords(String urll) throws ParserException {
        // extract words in url and return them
        // use StringTokenizer to tokenize the result from StringBean
        Vector<String> result = new Vector<String>();
        StringBean bean = new StringBean();
        bean.setURL(urll);
        bean.setLinks(false);
        String contents = bean.getStrings();
        StringTokenizer st = new StringTokenizer(contents);
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }
        return result;

    }
    //get the word size 
    public int extractWordsize(Vector<String> temp){
        return temp.capacity();
    }

    //get the url child links
    public Vector<String> extractLinks(String urll) throws ParserException {
        // extract links in url and return them
        Vector<String> result = new Vector<String>();
        LinkBean bean = new LinkBean();
        bean.setURL(urll);
        URL[] urls = bean.getLinks();
        for (URL s : urls) {
            result.add(s.toString());
        }
        return result;
    }
    public void reRetrieveURL(String url, String key) throws RocksDBException, ParserException,IOException{
        metaDB.removeAll();
        invertedIndex.removeAll();
        forwardIndex.removeAll();
        titleIndex.removeAll();
        parentIndex.removeAll();
        childIndex.removeAll();
		queue.clear();
		urlstatus.clear();
        this.crawler2();
		System.exit(0);
    }
    //check the url existed before or not
    public Vector<String> checkURL(Vector<String> URL, int nINdexed) throws RocksDBException,IOException,ParserException{
        Vector<String> output= new Vector<String>();
        for(String urls:URL){
            if(this.urlstatus.containsKey(urls)){
                if(this.urlstatus.get(urls).equals(FINISHED)){
                    for(int i=0;i<nINdexed;i++){
                        String temp =Integer.toString(i);
                        if(metaDB.get(temp)!=null){
                            Date date=metaDB.getDate(temp);
                            long comp=date.getTime();
                            if(this.extractLastModificationDate(urls)>comp+7*24*60*60*1000){
                                //re crawl the urls
                                reRetrieveURL(urls,temp);
                            }
                        }
                    }
                }
            }
            else{
                
            }
			output.add(urls);
        }
        return output;
    }
    public Vector<String> checkURL2(Vector<String> URL, int nINdexed) throws RocksDBException,IOException,ParserException{
        Vector<String> output= new Vector<String>();
        for(String urls:URL){
			output.add(urls);
        }
        return output;
    }	
    public Vector<String> tolowerClass(Vector<String> wordlist){
        for(String word : wordlist){
            word=word.toLowerCase();
        }
        return wordlist;
    }
    //remove repeated url in  same child links
    public Vector<String>  removeDuplicates(Vector<String> v){
        for(int i=0;i<v.size();i++){
            for(int j=0;j<v.size();j++){
                if(i!=j){
                    if(v.elementAt(i).equalsIgnoreCase(v.elementAt(j))){
                    v.removeElementAt(j);
                    }
                }
            }   
        }
        return v;
    }
	public void crawler2() throws MalformedURLException,RocksDBException,ParserException {
        queue.add(url);
        int nIndexed = 0;
        while (!queue.isEmpty() && nIndexed < N_PAGES) {
			System.out.println(nIndexed);
            String urll = queue.poll();
            try {
                //count the pages processed
                String nindex=Integer.toString(nIndexed);
                urlstatus.put(urll,DISCOVERED);
				
                //get Title
				StopStemWord ss = new StopStemWord();
                String title =this.getTitle(urll);
                title=title.toLowerCase();			
                //get links
                Vector<String> childurlList=this.extractLinks(urll);

                //remove duplicates links
                childurlList = this.removeDuplicates(childurlList);

                //checkurl
                Vector<String> checkedurl=this.checkURL2(childurlList,nIndexed);
                for(String finURL:checkedurl){
                    queue.add(finURL);
                    urlstatus.put(finURL,DISCOVERED);
                }

                // get words
                Vector<String> words=this.extractWords(urll);
                words=this.tolowerClass(words);
                //StopStem
                
                //int wordSize=this.extractWordsize(words);
                //Vector<String> processedWords= words;
                
                //stop stem the words
                Vector<String> processedWords = ss.process(words);
                processedWords.removeIf(word->word.equals(""));
                processedWords=ss.deleteSpecialSymbol(processedWords);

                //put url in the childIndex
                //child to parents link -> the parents of the child
                for(String urlll:childurlList){
                    if(this.childIndex.get(urlll)!=null){
                        childIndex.put(urlll,childIndex.get(urlll)+"::"+nindex);
                    }
                    else {
                        childIndex.put(urlll,nindex);
                    }
                }

                // get date
                long urldate=this.extractLastModificationDate(urll);

                //get page size
                int urlsize=this.getPageSize(urll);

                //put url into the parent index.
                //the parents contain how many child url
                this.parentIndex.put(nindex,childurlList.toString());

                //put url into metadb,
                //meta data of the page id
                this.metaDB.put(nindex,title,urll,urldate,urlsize);
                
                //split the title
                String[] titlesplit=title.split(" ");
                
                //make the title to store in vector 
                Vector<String> processedTitle =new Vector<String>();
                for(String titles:titlesplit){
                    processedTitle.add(titles);
                }
                //Vector<String> processedTitleout = processedTitle;        
                Vector<String> processedTitleout = ss.process(processedTitle);

                //stop stem the title
                processedTitleout.removeIf(processedTitles->processedTitles.equals(""));

                //delete the special symbol
                processedTitleout=ss.deleteSpecialSymbol(processedTitleout);

                //titleIndex.put(nindex,processedTitleout);

                //put the title in inverted order into title index
                titleIndex.put(nindex,processedTitleout);
                //childtoPare.put(nindex,checkedurl);
                
                //put the word into forward index
                forwardIndex.put(nindex, processedWords);
                
                //put the word into inverted index
                invertedIndex.put(nindex, processedWords);


				//forwardIndex.put(nindex, words);
                //invertedIndex.put(nindex, words);  
                
                //increase the counter
                nIndexed++;
                urlstatus.put(urll,FINISHED);
				;
            }catch (ParserException e) {
                e.printStackTrace();
            }catch(IOException e){
                e.printStackTrace();
            }
            
        }
		metaDB.printAll();

    }
    //process and get the website content 
    public void crawler() throws MalformedURLException,RocksDBException,ParserException {
        queue.add(url);
        int nIndexed = 0;
        while (!queue.isEmpty() && nIndexed < N_PAGES) {
			System.out.println(nIndexed);
            String urll = queue.poll();
            try {
                //count the pages processed
                String nindex=Integer.toString(nIndexed);
                urlstatus.put(urll,DISCOVERED);
				
                //get Title
				StopStemWord ss = new StopStemWord();
                String title =this.getTitle(urll);
                title=title.toLowerCase();			
                //get links
                Vector<String> childurlList=this.extractLinks(urll);

                //remove duplicates links
                childurlList = this.removeDuplicates(childurlList);

                //checkurl
                Vector<String> checkedurl=this.checkURL(childurlList,nIndexed);
                for(String finURL:checkedurl){
                    queue.add(finURL);
                    urlstatus.put(finURL,DISCOVERED);
                }

                // get words
                Vector<String> words=this.extractWords(urll);
                words=this.tolowerClass(words);
                //StopStem
                
                //int wordSize=this.extractWordsize(words);
                //Vector<String> processedWords= words;
                
                //stop stem the words
                Vector<String> processedWords = ss.process(words);
                processedWords.removeIf(word->word.equals(""));
                processedWords=ss.deleteSpecialSymbol(processedWords);

                //put url in the childIndex
                //child to parents link -> the parents of the child
                for(String urlll:childurlList){
                    if(this.childIndex.get(urlll)!=null){
                        childIndex.put(urlll,childIndex.get(urlll)+"::"+nindex);
                    }
                    else {
                        childIndex.put(urlll,nindex);
                    }
                }

                // get date
                long urldate=this.extractLastModificationDate(urll);

                //get page size
                int urlsize=this.getPageSize(urll);

                //put url into the parent index.
                //the parents contain how many child url
                this.parentIndex.put(nindex,childurlList.toString());

                //put url into metadb,
                //meta data of the page id
                this.metaDB.put(nindex,title,urll,urldate,urlsize);
                
                //split the title
                String[] titlesplit=title.split(" ");
                
                //make the title to store in vector 
                Vector<String> processedTitle =new Vector<String>();
                for(String titles:titlesplit){
                    processedTitle.add(titles);
                }
                //Vector<String> processedTitleout = processedTitle;        
                Vector<String> processedTitleout = ss.process(processedTitle);

                //stop stem the title
                processedTitleout.removeIf(processedTitles->processedTitles.equals(""));

                //delete the special symbol
                processedTitleout=ss.deleteSpecialSymbol(processedTitleout);

                //titleIndex.put(nindex,processedTitleout);

                //put the title in inverted order into title index
                titleIndex.put(nindex,processedTitleout);
                //childtoPare.put(nindex,checkedurl);
                
                //put the word into forward index
                forwardIndex.put(nindex, processedWords);
                
                //put the word into inverted index
                invertedIndex.put(nindex, processedWords);


				//forwardIndex.put(nindex, words);
                //invertedIndex.put(nindex, words);  
                
                //increase the counter
                nIndexed++;
                urlstatus.put(urll,FINISHED);
            }catch (ParserException e) {
                e.printStackTrace();
            }catch(IOException e){
                e.printStackTrace();
            }
            
        }

    }
    public static void main(String[] args) {
        try {
            String RootDir="/root/comp4321/phase11/";
            MetaIndex metaDB= new MetaIndex(RootDir+"javafile/DBMetaIndex");
            ForwardIndex forwardIndex=new ForwardIndex(RootDir+"javafile/DBForwardIndex");
            InvertedIndex invertedIndex=new InvertedIndex(RootDir+"javafile/DBInvertedIndex");
            TitleIndex titleIndex=new TitleIndex(RootDir+"javafile/DBTitleIndex");
            ParentIndex parentIndex= new ParentIndex(RootDir+"javafile/DBParentIndex");
            ChildIndex childIndex= new ChildIndex(RootDir+"javafile/DBChildIndex");
            //Crawler(String _url, int count, MetaIndex metaDB, ForwardIndex forwardIndex, InvertedIndex invertedIndex,TitleIndex titleIndex
            Crawler crawler = new Crawler("http://www.cse.ust.hk",50,metaDB,forwardIndex,invertedIndex,titleIndex,parentIndex,childIndex);
            crawler.crawler();
            //metaDB.printAll();
            //System.out.println(childIndex.getChild("0"));
            //System.out.println(childIndex.getChild("1"));
            //System.out.println(childIndex.getChild("2"));
            //System.out.println(childIndex.getChild("3"));
            //childIndex.printAll();
            //parentIndex.printAll();
			//Vector<String> temp=parentIndex.getParents("0");
			//System.out.println(parentIndex.getParents("0"));
			//System.out.println(parentIndex.getParents("0").size());
			//System.out.println(childIndex.getChild(0).size());
			
            //forwardIndex.printAll();
            //invertedIndex.printAll();
            //titleIndex.printAll();




            //phase 1!!!

            /*int ID = 0;
            String IDs = Integer.toString(ID);
            //DouglasIndx forwardIndex = new DouglasIndx(ROOT_DIR + "/DBForwardIndex");
            // System.out.println(crawler.url);
            // System.out.println(crawler.title);
            // System.out.println(crawler.ModificationDate);
            // System.out.println(crawler.firstPageSize);
            Map<String, Integer> out = VectorToMapToString(crawler.words);
            // System.out.println(out.toString());
            forwardIndex.put2(IDs, crawler.title, crawler.url, crawler.ModificationDate, crawler.firstPageSize,
                    out.toString());
            // System.out.println(forwardIndex.getWord("0"));
            // System.out.println(crawler.extractlink);
            ID++;
            Vector<String> links = crawler.extractLinks();
            int hyperlink_count = 0;
            // System.out.println("Child Links in "+crawler.url+":");
            hyperlink_count = links.size();
            // System.out.println(links.size());
            hyperlink_count = 30;
            for (int i = 0; i <= hyperlink_count; i++) {

                Crawler crawler_child = new Crawler(links.get(i));
                crawler.setCrawler(crawler_child, links.get(i));
                IDs = Integer.toString(ID);
                out = VectorToMapToString(crawler_child.words);
                forwardIndex.put2(IDs, crawler_child.title, crawler_child.url, crawler_child.ModificationDate,
                        crawler_child.firstPageSize, out.toString());
                ID++;
            }
            /*
            File myObj = new File("spider_result.txt");
            if (myObj.createNewFile()) {
            }
            FileWriter myWriter = new FileWriter("spider_result.txt");
            for (int i = 0; i < hyperlink_count; i++) {

                String tempid = Integer.toString(i);
                if (i > 0) {
                    myWriter.write("This is the " + (i + 1) + "/" + hyperlink_count + " fetched page");
                }
                myWriter.write("\nThe title of this page is:" + forwardIndex.getTitle(tempid));
                myWriter.write("\nThe URL of this page is:" + forwardIndex.getURL(tempid));
                myWriter.write("\nThe last Modification Date of the page is:  " + forwardIndex.getDate(tempid));
                myWriter.write("\nThe size of the page is:  " + forwardIndex.getSize(tempid));

                // words=words.replaceAll("{", "");
                // words=words.replaceAll("}","");
                // words=words.replaceAll(","," ");
                myWriter.write("Keywords in " + forwardIndex.getURL(tempid) + ":\n");
                for (String retval : words.split(",")) {
                    myWriter.write(retval);
                }
                myWriter.write("\n");

                myWriter.write("================================================\n");
            }
            myWriter.close();
            // System.out.println("================================================");
            // int no_of_indexed_pages=2;
            */


        } catch (ParserException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
