/**
 * Author: Cole Thomas
 * Last Modified: 2022.12.07
 *
 * This program uses Spark to perform various analytical tasks against The Tempest by Shakespeare.
 */

package org.ct;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.List;
import java.util.Scanner;

public class TextCalculations {
    static JavaRDD<String> inputFile;
    static JavaRDD<String> wordsFromFile;
    static JavaRDD<String> filteredRdd;
    static JavaRDD<String> consolidatedInput;
    static JavaRDD<String> lettersOnly;

    /**
     * Initalize The Tempest as a JavaRDD String
     * @param filename
     */
    private static void initalizeFile (String filename) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("TempestAnalytics"); // specify configuration
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf); // set up for java
        inputFile = sparkContext.textFile(filename); // set up for given filename in arguments
        wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split("[^a-zA-Z]+")));//tokenize the text
        filteredRdd = wordsFromFile.filter(k -> !k.isEmpty()); // source: https://www.oreilly.com/library/view/apache-spark-2x/9781787126497/f2dce8ba-2c8c-4a6d-b148-b3a7b4802798.xhtml
        consolidatedInput = inputFile.flatMap(content -> Arrays.asList(content.split("")));//tokenize the text
        lettersOnly = filteredRdd.flatMap(content -> Arrays.asList(content.replaceAll("[^a-zA-Z]", "").split(""))); // source: https://stackoverflow.com/questions/43263680/removing-all-characters-but-letters-in-a-string
    }

    /**
     * Task 0:
     * Use the count method of the JavaRDD class, display the number of lines in "The Tempest".
     * Write this output to the screen with System.out.println().
     * @return the total number of lines in a file
     */
    private static String numberOfLines() {
        return "Number of lines: " + inputFile.count();
    }

    /**
     * Task 1:
     * Display the number of words in The Tempest
     * Use the string "[^a-zA-Z]+" as the regular expression delimiter in your split method
     * @return string containing the number of words after filtering null rows
     */
    private static String numberOfWords() {
        return "Number of words: " + filteredRdd.count();
    }

    /**
     * Task 2:
     * Use JavaRDD distinct() and count() methods, display the number of distinct words
     * @return string containing the distinct number of words after filtering null rows
     */
    private static String numberOfDistinctWords() {
        // source: https://www.tutorialkart.com/apache-spark/spark-rdd-distinct/
        return "Number of distinct words: " + filteredRdd.distinct().count();
    }

    /**
     * Task 3:
     * Use the split method with a regular expression of "" and a flatmap to find the number of symbols
     * @return string with the number of symbols
     */
    private static String numberOfSymbols() {
        return "Number of symbols: " + consolidatedInput.count();
    }

    /**
     * Task 4:
     * Find the number of distinct symbols
     * @return string containing the number of distinct symbols
     */
    private static String numberOfDistinctSymbols() {
        return "Number of distinct symbols: " + consolidatedInput.distinct().count();
    }

    /**
     * Task 5:
     * number of distinct letters
     * @return string containing the number of distinct letters
     */
    private static String numberOfDistinctLetters() {
        return "Number of distinct letters: " + lettersOnly.distinct().count();
    }

    /**
     * Task 6:
     * return all the lines of that contain that word. The search will be case-sensitive
     * @return all lines containing a target word
     */
    private static String lineContainsWord() {

        Scanner sc = new Scanner(System.in); // set up scanner
        System.out.println("Please enter a word and the program will show all lines containing that word: ");
        String targetWord = sc.nextLine(); // get user input
        sc.close(); // close scanner

        String targetedLinesToReturn = "\nReturning lines containing the target word of " + targetWord +":\n";

        // source: https://www.tutorialkart.com/apache-spark/spark-print-contents-of-rdd/
        // collect RDD for printing
        for(String line: inputFile.collect()){
            if(line.contains(targetWord)) {
                targetedLinesToReturn += line + "\n";
            }
        }
        return targetedLinesToReturn;
    }

    public static void main(String[] args) {

        // catch errors in setup
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        // set up file
        initalizeFile(args[0]);

        // initialize array of messages to print
        List<String> messagesToPrint = new ArrayList<>();
        messagesToPrint.add(numberOfLines());
        messagesToPrint.add(numberOfWords());
        messagesToPrint.add(numberOfDistinctWords());
        messagesToPrint.add(numberOfSymbols());
        messagesToPrint.add(numberOfDistinctSymbols());
        messagesToPrint.add(numberOfDistinctLetters());
        messagesToPrint.add(lineContainsWord());

        // print messages
        for(String s : messagesToPrint){
            System.out.println(s);
        }

        // quit program
        System.exit(1);

    }

}