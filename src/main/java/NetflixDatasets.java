import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import java.io.*;
import java.util.Comparator;
import java.util.Scanner;

/**
 * Utility class to generate the NETFLIX datasets starting from no sequential data available online
 */
public class NetflixDatasets
{
    private Int2ObjectOpenHashMap<ObjectArrayList<Rating>> data;

    /**
     * Constructor of the class
     */
    public NetflixDatasets()
    {
        data = new Int2ObjectOpenHashMap<>();
    }

    /**
     * Adds data to the Netflix Dataset
     * @param file name of the file containing a part of the whole dataset
     */
    public void addData(String file)
    {
        try
        {
            File f = new File(file);
            Scanner sc = new Scanner(f);
            Scanner scl;
            int movie = -1;
            short rate;
            int user;
            int temp;
            short year;
            short month;
            short day;
            int cont = 0;
            Rating vo;
            String line;
            while (sc.hasNextLine())
            {
                line = sc.nextLine();
                scl = new Scanner(line);
                scl.useDelimiter("[:,-]");
                temp = scl.nextInt();
                if (!scl.hasNextInt())
                {
                    cont = 0;
                    movie = temp;
                }
                else
                {
                    cont++;
                    user = temp;
                    rate = scl.nextShort();
                    year = scl.nextShort();
                    month = scl.nextShort();
                    day = scl.nextShort();
                    scl.close();
                    vo = new Rating(movie, rate, day, month, year);
                    data.putIfAbsent(user, new ObjectArrayList<>());
                    ObjectArrayList<Rating> current = data.get(user);
                    current.add(vo);
                }
            }
            sc.close();
            System.gc();
        }
        catch (FileNotFoundException e)
        {
            System.err.println("File not found!\n" + e);
        }
    }

    /**
     * Private method to sort the data
     */
    private void order()
    {
        ObjectCollection<ObjectArrayList<Rating>> d = data.values();
        for (ObjectArrayList<Rating> v : d)
        {
            v.sort(Comparator.naturalOrder());
        }
    }

    /**
     * Writes the Netflix dataset in the SPMF format
     * @param file the name of the output file
     * @param normal true to generate NETFLIX
     *               false to generate NETFLIX_Y
     */
    public void writeSPMFDataset(String file, boolean normal)
    {
        try
        {
            PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(file)));
            int cont = 0;
            ObjectCollection<ObjectArrayList<Rating>> d = data.values();
            for (ObjectArrayList<Rating> v : d) {
                v.sort(Comparator.naturalOrder());
                IntArrayList itemset = new IntArrayList();
                if (normal) itemset.add(v.get(0).getMovie());
                else itemset.add(v.get(0).getYear());
                for (int i = 1; i < v.size(); i++)
                {
                    if (v.get(i - 1).compareTo(v.get(i)) == 0)
                    {
                        if (normal) itemset.add(v.get(i).getMovie());
                        else itemset.add(v.get(i).getYear());
                    }
                    else if (v.get(i - 1).compareTo(v.get(i)) < 0)
                    {
                        itemset.sort(Comparator.naturalOrder());
                        for (int j = 0; j < itemset.size(); j++) output.write(cont + ":" + itemset.getInt(j) + " ");
                        output.write("-1 ");
                        itemset.clear();
                        itemset = new IntArrayList();
                        if (normal) itemset.add(v.get(i).getMovie());
                        else itemset.add(v.get(i).getYear());
                    }
                    else if (v.get(i - 1).compareTo(v.get(i)) > 0) System.out.println("Error!");
                }
                itemset.sort(Comparator.naturalOrder());
                for (int j = 0; j < itemset.size(); j++) output.write(itemset.getInt(j) + " ");
                output.write("-1 -2\n");
            }
            output.close();
        }
        catch (IOException e)
        {
            System.err.println("Error! " + e);
        }
    }

    /**
     * Inner class to represent the valuation performed by users
     */
    private class Rating implements Comparable<Rating>
    {
        private short rate;
        private int movie;
        private short day;
        private short month;
        private short year;

        /**
         * Constructor of the inner class
         * @param movie id of the movie
         * @param rate value of the rating
         * @param day day of the rating
         * @param month month of the rating
         * @param year year of the rating
         */
        public Rating(int movie, short rate, short day, short month, short year)
        {
            this.movie = movie;
            this.rate = rate;
            this.day = day;
            this.month = month;
            this.year = year;
        }

        /**
         * Returns the value of the rating
         * @return the rating of the valuation
         */
        public short getRate()
        {
            return rate;
        }

        /**
         * Returns the id of the movie
         * @return the id of the movie
         */
        public int getMovie()
        {
            return movie;
        }

        /**
         * Returns the day of the valuation
         * @return the day of the valuation
         */
        public short getDay()
        {
            return day;
        }

        /**
         * Returns the month of the valuation
         * @return the month of the valuation
         */
        public short getMonth()
        {
            return month;
        }

        /**
         * Returns the year of the valuation
         * @return the year of the valuation
         */
        public short getYear()
        {
            return year;
        }

        /**
         * Compare the date of two ratings
         * @return < 0 if this is supposed to be less than other;
         *         > 0 if this is supposed to be greater than other;
         *         0 if they are supposed to be equal
         */
        @Override
        public int compareTo(Rating other)
        {
            if (year == other.year && month == other.month && day == other.day) return 0;
            if (year < other.year || (year == other.year && month < other.month) || (year == other.year && month == other.month && day < other.day))
                return -1;
            return 1;

        }
    }

    /**
     * Main of class
     * It reads the 4 files of the Netflix dataset available online and generate the two sequential dataset.
     * The 4 files must be in the data folder.
     */
    public static void main(String[] args){
        NetflixDatasets nd = new NetflixDatasets();
        nd.addData("data/combined_data_1.txt");
        nd.addData("data/combined_data_2.txt");
        nd.addData("data/combined_data_3.txt");
        nd.addData("data/combined_data_4.txt");
        nd.writeSPMFDataset("data/NETFLIX.txt",false);
        nd.writeSPMFDataset("data/NETFLIX_Y.txt",true);
    }
}