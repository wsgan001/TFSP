# Efficient Learning of Frequent Sequential Patterns
## Efficient Learning of Frequent Sequential Patterns (Tonon and Vandin, SDM 2020)

This code was used to test the performance and correctness of the strategies proposed in the paper. The results obtained are reported in the Experimental Evaluation section of the paper. The implementation used the PrefixSpan algorithm provided by the MLlib of the Apache Spark Framework to mine the datasets. The code has been developed using IntelliJ IDEA v2019.1, JAVA and the Apache Spark Framework.
We provided also the datasets used in the evaluation and the scripts to generate the missing datasets (NETFLIX and NETFLIX_Y for their larger dimension and the pseudo-artificial datasets).

## Usage
These are the available classes:

	* 	ArtificialDatasetTest: this class executes the test with the pseudo-artificial datasets 
	* 	MainTFSP: main class to execute the algorithms proposed in the paper
	* 	NetflixDatasets: this class generates the two netflix datasets
	* 	RealDatasetTest: this class executes the test with the real datasets
	* 	SparkTFSP: this class contains the main methods developed in the paper
	* 	Utils: this class contains useful support method
