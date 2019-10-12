import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;

/**
 * This class contains the algorithms described in the paper "Efficient Learning of Frequent Sequential Patterns".
 * The work introduced two algorithms to mine the true frequent sequential patterns (TFSP) from a dataset.
 * Such dataset is seen as a collection of sample transactions obtained from an unknown generative process.
 * TFSP are patterns frequently generated from the unknown generative process.
 * Given a dataset, we compute n upper bound on the empirical VC-dimension, called sBound, with a MapReduce
 * algorithm. The sBound is used to compute a corrected minimum frequency threshold to mine an approximation of
 * the TFSP from the dataset.
 * The first algorithm aims to extract an approximation of the TFSP that does not contain false positives
 * with high probability. The second one aims to extract all the TFSP with high probability.
 */
public class SparkTFSP {

    /**
     * Private class that implements a simple generic pair structure
     */
    private static class Pair<K, V> {
        K key;
        V value;

        Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        K getKey() {
            return key;
        }

        V getValue() {
            return value;
        }
    }

    /**
     * Computes the sBound of a dataset
     * @param   dataset     the dataset used to mine the TFSP
     * @param   n           the level of parallelism
     * @return              the sBound of the dataset
     */
    static JavaRDD<Integer> getSbound(JavaRDD<String> dataset, int n){
        return dataset.mapToPair(t-> {
            String[] splitted = t.split(":");
            int index = Integer.parseInt(splitted[0]);
            String tr = splitted[1];
            BigInteger cap = getCapacity(tr);
            return new Tuple2<>(index%n,new Tuple2<>(tr,cap));
        }).groupByKey().mapToPair(tuple->{
            Iterator<Tuple2<String,BigInteger>> set = tuple._2().iterator();
            return getPartialSBound(set);
         }).groupByKey()
        .map(t->{
            ArrayList<Tuple2<String,BigInteger>> set = new ArrayList<>();
            for(ArrayList<Tuple2<String,BigInteger>> curr : t._2()) set.addAll(curr);
            return getFinalSBound(set);
        });
    }

    /**
     * Executed the first round of the MapReduce algorithm to compute the sBound of a dataset
     * @param   set     the set of transactions and their capacities in one of the n portions of the dataset
     * @return          the set of transactions and their capacities used to compute the sBound in such portion
     */
    private static Tuple2<Integer,ArrayList<Tuple2<String,BigInteger>>> getPartialSBound(Iterator<Tuple2<String,BigInteger>> set) {
        int d = 0;
        BigInteger minCap = BigInteger.ZERO;
        String minSeq = "";
        Object2ObjectOpenHashMap<String,BigInteger> outputSet = new Object2ObjectOpenHashMap<>();
        while (set.hasNext()) {
            Tuple2<String,BigInteger> curr = set.next();
            BigInteger capacity = curr._2();
            String t = curr._1();
            if (capacity.compareTo(new BigInteger("2").pow(d).subtract(BigInteger.ONE)) > 0 && !outputSet.containsKey(t)) {
                    if (capacity.compareTo(minCap) < 0) {
                        minCap = capacity;
                        minSeq = t;
                        d++;
                    } else {
                        if (minCap.compareTo(new BigInteger("2").pow(d + 1).subtract(BigInteger.ONE)) >= 0) d++;
                        else {
                            outputSet.remove(minSeq);
                            minCap = capacity;
                            minSeq = t;
                            for (String s : outputSet.keySet()) {
                                BigInteger len = outputSet.get(s);
                                if (minCap.compareTo(len) > 0) {
                                    minCap = len;
                                    minSeq = s;
                                }
                            }
                        }
                    }
                    outputSet.put(t, capacity);
            }
        }
        ArrayList<Tuple2<String,BigInteger>> out = new ArrayList<>();
        for(String s : outputSet.keySet()){
            out.add(new Tuple2<>(s,outputSet.get(s)));
        }
        return new Tuple2<>(0,out);
    }

    /**
     * Executed the second round of the MapReduce algorithm to compute the sBound of a dataset
     * @param   set     the union of the sets of transactions and their capacities computed in the first MapReduce round
     * @return          the set of transactions and their capacities used to compute the sBound in such portion
     */
    private static int getFinalSBound(ArrayList<Tuple2<String,BigInteger>> set) {
        int d = 0;
        BigInteger minCap = BigInteger.ZERO;
        String minSeq = "";
        Iterator<Tuple2<String,BigInteger>> set2 = set.iterator();
        Object2ObjectOpenHashMap<String,BigInteger> outputSet = new Object2ObjectOpenHashMap<>();
        while (set2.hasNext()) {
            Tuple2<String,BigInteger> curr = set2.next();
            BigInteger capacity = curr._2();
            String t = curr._1();
            if (capacity.compareTo(new BigInteger("2").pow(d).subtract(BigInteger.ONE)) > 0 && !outputSet.containsKey(t)) {
                if (capacity.compareTo(minCap) < 0) {
                    minCap = capacity;
                    minSeq = t;
                    d++;
                } else {
                    if (minCap.compareTo(new BigInteger("2").pow(d + 1).subtract(BigInteger.ONE)) >= 0) d++;
                    else {
                        outputSet.remove(minSeq);
                        minCap = capacity;
                        minSeq = t;
                        for (String s : outputSet.keySet()) {
                            BigInteger len = outputSet.get(s);
                            if (minCap.compareTo(len) > 0) {
                                minCap = len;
                                minSeq = s;
                            }
                        }
                    }
                }
                outputSet.put(t, capacity);
            }
        }
        return d;
    }

    /**
     * Computes the capacity of a transaction with the algorithm presented in the paper
     * @param   tr     the transaction
     * @return         the capacity of the transaction in input
     */
    private static BigInteger getCapacity(String tr){
        String[] tokens = tr.split(" ");
        int length = 0;
        ObjectArrayList<Pair<IntArrayList,Integer>> sequence = new ObjectArrayList<>();
        IntArrayList itemset = new IntArrayList();
        for (int j = 0; j < tokens.length - 2; j++) {
            int current = Integer.parseInt(tokens[j]);
            if (current > 0) {
                length++;
                itemset.add(current);
            } else {
                sequence.add(new Pair(itemset,length-itemset.size()));
                itemset = new IntArrayList();
            }
        }
        sequence.sort(((o1, o2) -> {
            if(o1.getKey().size()>o2.getKey().size()) return 1;
            if(o1.getKey().size()<o2.getKey().size()) return -1;
            return 0;
        }));
        BigInteger capacity = new BigInteger("2").pow(length).subtract(BigInteger.ONE);
        int len = length;
        for(int i=0;i<sequence.size()-1;i++){
            BigInteger maxValue = BigInteger.ZERO;
            BigInteger currValue;
            Pair<IntArrayList,Integer> min = sequence.get(i);
            for(int j=i+1;j<sequence.size();j++){
                Pair<IntArrayList,Integer> max = sequence.get(j);
                IntArrayList intersection = min.getKey().clone();
                intersection.retainAll(max.getKey());
                if(intersection.size()>0){
                    currValue = new BigInteger("2").pow(Math.min(max.getValue(),min.getValue()));
                    currValue = currValue.add((new BigInteger("2").pow(intersection.size())).subtract(BigInteger.ONE));
                    currValue = currValue.add(new BigInteger("2").pow(len-Math.max(max.getValue()+max.getKey().size(),min.getValue()+min.getKey().size())));
                    if(maxValue.compareTo(currValue)<0) maxValue = currValue;
                }
            }
            if(!maxValue.equals(BigInteger.ZERO)){
                len-=min.getKey().size();
                capacity = capacity.subtract(maxValue);
                for(int k=i+1;k<sequence.size();k++){
                    Integer curr = sequence.get(k).getValue();
                    if(curr>min.getValue()){
                        Pair<IntArrayList, Integer> currPair = sequence.remove(k);
                        sequence.add(k,new Pair<>(currPair.getKey(),curr-min.getKey().size()));
                    }
                }
            }
        }
        return capacity;
    }

    /**
     * Mines the frequent sequential patterns from a dataset using the PrefixSpan implementation provided by
     * the MLlib of Apache Spark
     * @param   dataset     the dataset to mine
     * @param   theta       the minimum frequency threshold used to mine the dataset
     * @param   outputFile  the name of the file where the FSP will be stored
     * @return              the number of FSP mined from the dataset
     */
    static int executeMining(JavaRDD<String> dataset, double theta, String outputFile) throws IOException {
        JavaRDD<ObjectArrayList<IntArrayList>> sequences = dataset.map(t->{
            String[] line = t.split(":")[1].split("-2")[0].split(" -1 ");
            ObjectArrayList<IntArrayList> seq = new ObjectArrayList<>();
            for(String currItemset : line){
                IntArrayList itemset = new IntArrayList();
                String[] items = currItemset.split(" ");
                for(String currItem: items){
                    itemset.add(Integer.parseInt(currItem));
                }
                seq.add(itemset);
            }
            return seq;
        });
        PrefixSpan prefixSpan = new PrefixSpan().setMinSupport(theta);
        PrefixSpanModel<Integer> model = prefixSpan.run(sequences);
        BufferedWriter output = new BufferedWriter(new FileWriter(outputFile));
        StringBuilder r = new StringBuilder();
        int numSP = model.freqSequences().toJavaRDD().collect().size();
        for (PrefixSpan.FreqSequence<Integer> freqSeq : model.freqSequences().toJavaRDD().collect())
        {
            for (List<Integer> it : freqSeq.javaSequence())
            {
                IntArrayList itt = new IntArrayList(it);
                itt.sort((Comparator.naturalOrder()));
                for (int j : itt)
                {
                    r.append(j + " ");
                }
                r.append("-1 ");
            }
            r.append("#SUP: "+ (freqSeq.freq()) + "\n");
        }
        output.write(r.toString());
        output.close();
        return numSP;
    }

    /**
     * Executes the algorithm to mine TFSP from a dataset
     * @param   fileIn      the name of the file containing tha dataset to mine
     * @param   fileOut     the name of the file where the TFSP will be stored
     * @param   theta       the minimum frequency threshold for the TFSP
     * @param   delta       the probability confidence threshold
     * @param   par         the level of parallelism
     * @param   allTFSP     chooses the algorithm to used:
     *                          TRUE:   mines all the TFSP with high probability
     *                          FALSE:  mines the TFSP without false positives with high probability
     * @return              the number of TFSP mined from the dataset
     */
    static int execute(String fileIn, String fileOut, double theta, double delta, int par, boolean allTFSP) throws IOException {
        SparkConf sparkConf = new SparkConf().setMaster("local[" + par + "]").setAppName("ArtificialDatasetTest").
                set("spark.executor.memory", "5g").set("spark.driver.memory", "5g").set("spark.executor.heartbeatInterval", "10000000").
                set("spark.network.timeout", "10000000");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
        int numPartitions = sc.defaultParallelism();
        long start = System.currentTimeMillis();
        JavaRDD<String> dataset = sc.textFile(fileIn, numPartitions);
        long datasetSize = dataset.count();
        int bound = getSbound(dataset, numPartitions).collect().get(0);
        long end = System.currentTimeMillis();
        System.out.println("Computed sBound: " + bound + " in " + (end - start) + " ms");
        double eps = Math.sqrt(1 / (2. * datasetSize) * (bound + Math.log(1 / delta)));
        System.out.println("Epsilon: " + eps);
        double correctedTheta;
        if (allTFSP) correctedTheta = theta - eps;
        else correctedTheta = theta + eps;
        start = System.currentTimeMillis();
        int numTFSP = executeMining(dataset, correctedTheta, fileOut);
        end = System.currentTimeMillis();
        System.out.println("Mining done in " + (end - start) + " ms");
        sc.close();
        return numTFSP;
    }
}