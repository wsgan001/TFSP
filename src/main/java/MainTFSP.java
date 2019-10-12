import java.io.IOException;

/**
 * The class to execute the algorithms presented in the paper.
 */
public class MainTFSP {

    /**
     * Main of class
     * Input parameters from command line:
     * 0: name of the input dataset
     * 1: name of the output dataset
     * 2: minimum frequency threshold
     * 3: confidence probability threshold
     * 4: level of parallelism
     * 5: the algorithm to execute (TRUE: mine all the TFSP, FALSE: mine the TFSP without false positives)
     */
    public static void main(String[] args) throws IOException {
        String fileIn = args[0];
        String fileOut = args[1];
        double theta = Double.parseDouble(args[2]);
        double delta = Double.parseDouble(args[3]);
        int par = Integer.parseInt(args[4]);
        boolean all = Boolean.parseBoolean(args[5]);
        System.out.println("Parameters:");
        System.out.println("Input Dataset: " + fileIn);
        System.out.println("Output Dataset: " + fileOut);
        System.out.println("Theta: " + theta);
        System.out.println("Delta: " + delta);
        System.out.println("Level of Parallelism: " + par);
        System.out.println("Algorithm: " + ((all)?"ALL the TFSP":"TFSP without false positives"));
        int tfsp = SparkTFSP.execute(fileIn+".txt",fileOut+".txt",theta,delta,par,all);
        System.out.println("Number of TFSP mined: " + tfsp);
    }
}