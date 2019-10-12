import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

/**
 * This class contains useful support methods for the other classes.
 */
public class Utils {

    /**
     * Generates a pseudo-artificial dataset of a fixed size sampling transactions uniformly at random
     * from a real dataset.
     * @param   fileIn      the name of the file that contains the real dataset
     * @param   newSize     the size of the pseudo-artificial dataset to create
     * @param   seed        the seed for the random generator (fixed for reproducibility)
     * @param   fileOut     the name of file where the pseudo-artificial dataset generated is saved
     */
    static void replicate(String fileIn, int newSize, int seed, String fileOut) throws IOException {
        ArrayList<String> dataset = new ArrayList<>();
        FileReader fr = new FileReader(fileIn);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while(line!=null){
            dataset.add(line.split(":")[1]);
            line = br.readLine();
        }
        br.close();
        fr.close();
        int size = dataset.size();
        Random r = new Random(seed);
        FileWriter fw = new FileWriter(fileOut);
        BufferedWriter bw = new BufferedWriter(fw);
        for(int i=0;i<newSize-1;i++){
            bw.write(i+":"+dataset.get(r.nextInt(size)) + '\n');
        }
        bw.write((newSize-1)+":"+dataset.get(r.nextInt(size)));
        bw.close();
        fw.close();
    }

    /**
     * Computes the percentage of times the FSPs mined from the 10 pseudo-artificial datasets contain false
     * positives and false negatives
     * @param   fileIn  the name of the file that contains the real dataset
     */
    static void getPercentageFPFN(String fileIn) throws IOException {
        HashSet<String> gt = new HashSet<>();
        FileReader fr = new FileReader(fileIn+"_GT.txt");
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while(line!=null){
            gt.add(line.split(" #SUP: ")[0]);
            line = br.readLine();
        }
        br.close();
        fr.close();
        int totFp = 0;
        int totFn = 0;
        for(int i=0;i<10;i++) {
            HashSet<String> fsp = new HashSet<>();
            int fp = 0;
            int fn = 0;
            fr = new FileReader(fileIn+"_"+i+"_M.txt");
            br = new BufferedReader(fr);
            line = br.readLine();
            while (line != null) {
                fsp.add(line.split(" #SUP: ")[0]);
                line = br.readLine();
            }
            br.close();
            fr.close();
            for(String s:gt){
                if(!fsp.contains(s)) fn++;
            }
            for(String s:fsp){
                if(!gt.contains(s)) fp++;
            }
            if(fn>0) totFn++;
            if(fp>0) totFp++;
        }
        System.out.println("    TFSP: " + gt.size());
        System.out.println("    Times FPs: " + (totFp*10)+"%");
        System.out.println("    Times FNs: " + (totFn*10)+"%");
    }

    /**
     * Computes the percentage of times the TFSPs mined from the 10 pseudo-artificial datasets contain false
     * positives and the average fraction of TFSPs of the ground truth reported in output
     * @param   fileIn  the name of the file that contains the real dataset
     */
    static void getPercentageFP(String fileIn) throws IOException {
        HashSet<String> gt = new HashSet<>();
        FileReader fr = new FileReader(fileIn+"_GT.txt");
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while(line!=null){
            gt.add(line.split(" #SUP: ")[0]);
            line = br.readLine();
        }
        br.close();
        fr.close();
        int totFp = 0;
        double percTFSP = 0.;
        for(int i=0;i<10;i++) {
            HashSet<String> fsp = new HashSet<>();
            int fp = 0;
            fr = new FileReader(fileIn+"_"+i+"_F.txt");
            br = new BufferedReader(fr);
            line = br.readLine();
            while (line != null) {
                fsp.add(line.split(" #SUP: ")[0]);
                line = br.readLine();
            }
            br.close();
            fr.close();
            for(String s:fsp){
                if(!gt.contains(s)) fp++;
            }
            if(fp>0) totFp++;
            percTFSP+=(fsp.size()-fp)/(gt.size()*1.);
        }
        System.out.println("    Times FPs: " + (totFp*10)+"%");
        System.out.println("    TFSPs reported: " + percTFSP/10.);
    }

    /**
     * Computes the percentage of times the TFSPs mined from the 10 pseudo-artificial datasets contain all
     * the TFSPs of the ground truth and the average percentage of false positives in the output of the algorithm
     * @param   fileIn  the name of the file that contains the real dataset
     */
    static void getPercentageALL(String fileIn) throws IOException {
        HashSet<String> gt = new HashSet<>();
        FileReader fr = new FileReader(fileIn+"_GT.txt");
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while(line!=null){
            gt.add(line.split(" #SUP: ")[0]);
            line = br.readLine();
        }
        br.close();
        fr.close();
        int totALL = 0;
        double percFP = 0.;
        for(int i=0;i<10;i++) {
            HashSet<String> fsp = new HashSet<>();
            int fn = 0;
            fr = new FileReader(fileIn+"_"+i+"_T.txt");
            br = new BufferedReader(fr);
            line = br.readLine();
            while (line != null) {
                fsp.add(line.split(" #SUP: ")[0]);
                line = br.readLine();
            }
            br.close();
            fr.close();
            for(String s:gt){
                if(!fsp.contains(s)) fn++;
            }
            if(fn==0) totALL++;
            percFP+=((fsp.size()-fn)-gt.size())/(fsp.size()*1.);
        }
        System.out.println("    Times ALL: " + (totALL*10)+"%");
        System.out.println("    FPs reported: " + percFP/10.);
    }
}