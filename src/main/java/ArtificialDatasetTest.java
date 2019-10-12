import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.IOException;

/**
 * This class allows to reproduce the experimental evaluation performed with pseudo-artificial
 * datasets described in the paper "Efficient Learning of Frequent Sequential Patterns"
 * and shown in Table 3 and Table 4.
 * Given an input dataset, that is the ground truth, we generates 10 pseudo-artificial datasets of
 * a fixed size sampling random transactions form the ground truth.
 * For each pseudo-artificial dataset, we compute its sBound and the two corrected thresholds,
 * one for each algorithms proposed in the paper. Finally, we mine the frequent sequential patterns
 * from the pseudo-artificial datasets with both threshold. The seeds of the random generators used to
 * sample the transactions are fixed for reproducibility.
 */
public class ArtificialDatasetTest {

    /**
     * Performs the experimental evaluation with 10 pseudo-artificial daasets created from a real dataset and
     * prints in the standard output the results.
     * @param   fileIn  the name of the file that contains the real dataset (the ground truth)
     * @param   theta   the minimum frequency threshold
     * @param   delta   the confidence probability threshold for the algorithms
     * @param   par     the level of parallelism for the computation of the sBound and for the mining
     * @param   size    the size of the pseudo-artificial datasets
     */
    private static void performTest(String fileIn, double theta, double delta, int par, int size) throws IOException {
        SparkConf sparkConf = new SparkConf().setMaster("local[" + par + "]").setAppName("ArtificialDatasetTest").
                set("spark.executor.memory", "5g").set("spark.driver.memory", "5g").set("spark.executor.heartbeatInterval", "10000000").
                set("spark.network.timeout", "10000000");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
        int numPartitions = sc.defaultParallelism();
        long start = System.currentTimeMillis();
        // Read and mine the ground truth
        JavaRDD<String> dataset = sc.textFile(fileIn+".txt", numPartitions);
        SparkTFSP.executeMining(dataset, theta, fileIn+"_GT.txt");
        long end = System.currentTimeMillis();
        System.out.println("Mining Ground Truth done in " + (end - start) + " ms");
        sc.close();
        long timeSBound = 0;
        long timeMineTFSP = 0;
        long timeMineFSP = 0;
        long timeMineAllTFSP = 0;
        for(int i=0;i<10;i++){
            System.out.println("Run " + (i+1)+":");
            // Generate a pseudo-artificial dataset using i as random seed
            Utils.replicate(fileIn+".txt",size,i,fileIn+"_"+i+".txt");
            sparkConf = new SparkConf().setMaster("local[" + par + "]").setAppName("ArtificialDatasetTest").
                    set("spark.executor.memory", "5g").set("spark.driver.memory", "5g").set("spark.executor.heartbeatInterval", "10000000").
                    set("spark.network.timeout", "10000000");
            sc = new JavaSparkContext(sparkConf);
            Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
            numPartitions = sc.defaultParallelism();
            start = System.currentTimeMillis();
            JavaRDD<String> datasetR = sc.textFile(fileIn+"_"+i+".txt", numPartitions);
            long datasetSize = datasetR.count();
            // Compute the sBound of the pseudo-artificial dataset
            int bound = SparkTFSP.getSbound(datasetR, numPartitions).collect().get(0);
            end = System.currentTimeMillis();
            System.out.println("    Computed sBound: " + bound + " in " + (end - start) + " ms");
            timeSBound+=(end - start);
            double eps = Math.sqrt(1 / (2. * datasetSize) * (bound + Math.log(1 / delta)));
            System.out.println("    Epsilon: " + eps);
            // Compute the two corrected thresholds
            double correctedThetaT = theta - eps;
            double correctedThetaF = theta + eps;
            start = System.currentTimeMillis();
            // Mine the pseudo-artificial dataset to obtain all the TFSP
            SparkTFSP.executeMining(datasetR, correctedThetaT, fileIn+"_"+i+"_T.txt");
            end = System.currentTimeMillis();
            System.out.println("    Mining All TFSP done in " + (end - start) + " ms");
            timeMineAllTFSP+=(end-start);
            start = System.currentTimeMillis();
            // Mine the pseudo-artificial dataset to obtain the TFSP without false positives
            SparkTFSP.executeMining(datasetR, correctedThetaF, fileIn+"_"+i+"_F.txt");
            end = System.currentTimeMillis();
            System.out.println("    Mining TFSP done in " + (end - start) + " ms");
            timeMineTFSP+=(end-start);
            start = System.currentTimeMillis();
            SparkTFSP.executeMining(datasetR, theta, fileIn+"_"+i+"_M.txt");
            end = System.currentTimeMillis();
            // Mine the pseudo-artificial dataset to obtain the FSP
            System.out.println("    Mining FSP done in " + (end - start) + " ms");
            timeMineFSP+=(end-start);
            sc.close();
            System.out.println("********************");
        }
        System.out.println("Results for FPs and FNs in FSP (Table 3 of the paper): ");
        // Compute the fraction of times in which FSP contains FPs and FNs
        Utils.getPercentageFPFN(fileIn);
        System.out.println("Results for TFSP (Table 4 of the paper): ");
        // Compute the accuracy for the algorithm without false positive
        Utils.getPercentageFP(fileIn);
        System.out.println("Results for ALL TFSP (Table 4 of the paper): ");
        // Compute the accuracy for the algorithm with all TFSP
        Utils.getPercentageALL(fileIn);
        System.out.println("AVG times: ");
        System.out.println("    AVG sBound: " + (timeSBound/10.));
        System.out.println("    AVG mining TFSP: " + (timeMineTFSP/10.));
        System.out.println("    AVG mining ALL TFSP: " + (timeMineAllTFSP/10.));
        System.out.println("    AVG mining FSP: " + (timeMineFSP/10.));
    }

    /**
     * Main of the ArificialDaasetTest class
     * Input parameters from command line:
     * 0: name of the real dataset ({BIBLE,BIKE,FIFA})
     * 1: minimum frequency threshold ({0.1,0.08,0.06,0.04,0.02},{0.025,0.020,0.015,0.010,0.005},{0.275,0.250,0.225,0.200,0.175})
     * 2: confidence probability threshold (0.1)
     * 3: level of parallelism (32)
     * 4: size of the pseudo-artificial datasets ({1M,10M,100M})
     */
    public static void main(String[] args) throws IOException {
        String fileIn = args[0];
        double theta = Double.parseDouble(args[1]);
        double delta = Double.parseDouble(args[2]);
        int par = Integer.parseInt(args[3]);
        int size = Integer.parseInt(args[4]);
        performTest(fileIn,theta,delta,par,size);
    }
}