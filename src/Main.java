/* generic map reducer
	Author: Ashwath

	Single JVM
	One reducer
	Thread safe
	1 test
	Task estimator, input simple


	Concurrent exec
	Reducer starts after all mappers end
	Generic map, reduce imp

    Assumptions:
        max number of mappers = 100 (100 threads) it could mean 100 nodes on cloud.
        TaskEstimator is approximated. each mapper takes x amount of time.
            Total time = number of lines * x

*/

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class Main {

    private static List<Runnable> mapperThread = new ArrayList<>();
    private static List<Runnable> pendingMapperThread = new ArrayList<>();

    private static HashMap<String, Integer> finalWordCountData = new HashMap<>();
    public static ArrayList<HashMap<String, Integer>> mapperResults = new ArrayList<HashMap<String, Integer>>();

    private static int MAX_MAPPERS = 100;
    private static Integer taskEstimator = 1;
    private static ExecutorService es = Executors.newCachedThreadPool();

    synchronized
    public static void AddMapperResult(Object o){
        mapperResults.add((HashMap) o);
    }

    public static class MyMapper implements Runnable{

        String _line_;
        HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
        public MyMapper(String line){
            _line_ = line;
        }
        @Override
        public void run() {
            //break the line into words
            for(String s :_line_.split(" ")) {
                if (wordMap.containsKey(s)) {
                    //dont make copies of same word. shuffle and sort
                    Integer count = wordMap.get(s).intValue();
                    wordMap.put(s, ++count);
                }else{
                    wordMap.put(s, 1);
                }
            }
            AddMapperResult(wordMap);
            //remove this thread from the threadpool.
        }
    }

    public static MyMapper createMapperThreadForLine(String line){
        Runnable r = new MyMapper(line);
        mapperThread.add(r);
        if(mapperThread.size() > MAX_MAPPERS){
            System.out.println(" Number of mappers exceeded, queuing the tasks. ");
            pendingMapperThread.add(r);
        }else {
            return (MyMapper) r;
        }
        return null;
    }

    public static boolean readFileAndInitializemapper(){

        try {
            File fr = new File("words.txt");
            //estimate task based on the file size.
            double mbytes = (fr.length() / 1024) / 1024;
            if(mbytes > 1 && mbytes < 5)
                taskEstimator = 2; //profile the mapper time per line. based on that task estimator must be configured.

            BufferedReader br = new BufferedReader(new FileReader("words.txt"));

            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            do{
                MyMapper tmp = createMapperThreadForLine(line);
                if(tmp != null)
                    es.execute(tmp);

                line = br.readLine();

            }while (line != null);
            return true;
        }catch(Exception e){
            System.out.println("Exception : " + e.getMessage() );

        }
        return false;
    }

    public static void main(String[] args) {
        //split lines from a file and create a mapper.

        boolean ret = readFileAndInitializemapper();
        if(!ret){
            System.out.println("MapReducer file init failed" );
            return;
        }

        //start any queued mappers due to out of slots.
        if(pendingMapperThread.size() > 0){
            for(Integer i = 0; i <  pendingMapperThread.size(); ++i){
                es.execute((MyMapper)pendingMapperThread.get(i));
                pendingMapperThread.remove(i);
            }
        }

        es.shutdown(); //no new tasks accepted
        try {
            es.awaitTermination(taskEstimator, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //1 Reducer
        //check size of the mapperresults, if too big, create threads
        for(HashMap<String, Integer> res: mapperResults){

            for(String w : res.keySet()){
                if(finalWordCountData.containsKey(w)){
                    finalWordCountData.put(w, finalWordCountData.get(w) + res.get(w));
                }else{
                    finalWordCountData.put(w, res.get(w));
                }
            }
        }
        for(String w: finalWordCountData.keySet()){
            System.out.println("Word " + w + " appeared " + finalWordCountData.get(w) + " time/s" );
        }

        //sort the words and show which word was repeated the most.

    }
}