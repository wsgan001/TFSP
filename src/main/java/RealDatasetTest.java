import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.IOException;

/**
 * This class allows to reproduce the experimental evaluation performed with real
 * datasets described in the paper "Efficient Learning of Frequent Sequential Patterns"
 * and shown in Table 5.
 * Given an input dataset, we mine the FSP, compute the sBound and the two corrected thresholds,
 * one for each algorithms proposed in the paper. Finally, we mine the TFSP with the algorithm
 * with guarantees on number the false positives and with the algorithm with guarantees on
 * finding all the TFSP.
 */
public class RealDatasetTest{

    /**
     * Performs the experimental evaluation with a real dataset and
     * prints in the standard output the results.
     * @param   fileIn  the name of the file that contains the real dataset
     * @param   theta   the minimum frequency threshold
     * @param   delta   the confidence probability threshold for the algorithms
     * @param   par     the level of parallelism for the computation of the sBound and for the mining
     */
    private static void performTest(String fileIn, double theta, double delta, int par) throws IOException {
        SparkConf sparkConf = new SparkConf().setMaster("local[" + par + "]").setAppName("ArtificialDatasetTest").
                set("spark.executor.memory", "5g").set("spark.driver.memory", "5g").set("spark.executor.heartbeatInterval", "10000000").
                set("spark.network.timeout", "10000000");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
        int numPartitions = sc.defaultParallelism();
        long start = System.currentTimeMillis();
        // Read the dataset
        JavaRDD<String> dataset = sc.textFile(fileIn+".txt", numPartitions);
        long datasetSize = dataset.count();
        // Compute the sBound of the dataset
        int bound = SparkTFSP.getSbound(dataset, numPartitions).collect().get(0);
        long end = System.currentTimeMillis();
        System.out.println("Computed sBound: " + bound + " in " + (end - start) + " ms");
        double eps = Math.sqrt(1 / (2. * datasetSize) * (bound + Math.log(1 / delta)));
        System.out.println("Epsilon: " + eps);
        // Compute the two corrected thresholds
        double correctedThetaT = theta - eps;
        double correctedThetaF = theta + eps;
        start = System.currentTimeMillis();
        // Mine the dataset to obtain all the TFSP
        int allTFSP = SparkTFSP.executeMining(dataset, correctedThetaT, fileIn+"_T.txt");
        end = System.currentTimeMillis();
        System.out.println("Mining All TFSP done in " + (end - start) + " ms");
        System.out.println("Number of FSP returned to obtain all TFSP: " + allTFSP);
        start = System.currentTimeMillis();
        // Mine the dataset to obtain the TFSP without false positives
        int TFSP = SparkTFSP.executeMining(dataset, correctedThetaF, fileIn+"_F.txt");
        end = System.currentTimeMillis();
        System.out.println("Mining TFSP done in " + (end - start) + " ms");
        System.out.println("Number of FSP returned to obtain TFSP with guarantees on FPs: " + TFSP);
        start = System.currentTimeMillis();
        int FSP = SparkTFSP.executeMining(dataset, theta, fileIn+"_FSP.txt");
        end = System.currentTimeMillis();
        // Mine the dataset to obtain the FSP
        System.out.println("Mining FSP done in " + (end - start) + " ms");
        System.out.println("Number of FSP returned: " + FSP);
        sc.close();
    }

    /**
     * Main of the RealDatasetTest class
     * Input parameters from command line:
     * 0: name of the real dataset ({MSNBC,NETFLIX,NETFLIX_Y})
     * 1: minimum frequency threshold ({0.020,0.015,0.010},{0.25,0.20,0.15,0.10},{0.70,0.65,0.60,0.55.})
     * 2: confidence probability threshold (0.1)
     * 3: level of parallelism (32)
     */
    public static void main(String[] args) throws IOException {
        String fileIn = args[0];
        double theta = Double.parseDouble(args[1]);
        double delta = Double.parseDouble(args[2]);
        int par = Integer.parseInt(args[3]);
        performTest(fileIn,theta,delta,par);
    }
}