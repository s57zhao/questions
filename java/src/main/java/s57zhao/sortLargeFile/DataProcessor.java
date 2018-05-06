package s57zhao.sortLargeFile;

import java.io.IOException;

public class DataProcessor {
  /* *
   * parameters
   * input file directory
   * output file directory
   * size of each chunk (in MB, default 100, depends on the MEM and size of the file)
   * number of threads (default 4)
   * onePassMergeSize (default 5) depends on how large of each chunk, and how many RAM the machine has
   * */
  public static void main(String args[]) {
    if (args.length != 5) {
      System.err.println("Usage: java DataProcessor input, output, chunkSize, numOfThreads, onePassMergeSize");
      System.exit(-1);
    }

    String inputDir = args[0];
    String outputDir = args[1];
    int chunkSize = Integer.parseInt(args[2]);
    int numOfThreads = Integer.parseInt(args[3]);
    int onePassMergeSize = Integer.parseInt(args[4]);

    Reader reader = new Reader();
    reader.setChunkSize(chunkSize);
    reader.setNumThread(numOfThreads);
    reader.readFile(inputDir);

    int chunkCount = reader.getChunkCount();

    System.err.println("merge process start!");
    Merger merger = new Merger(chunkCount, outputDir);
    merger.setOnePassMergeSize(onePassMergeSize);
    try {
      merger.merge();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
