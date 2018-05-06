public class DataProcessor {
  /* *
   * parameters
   * input file directory
   * output file directory
   * size of each chunk (in MB)
   * number of threads
   * */
  public static void main(String args[]) {
    if (args.length != 4) {
      System.err.println("Usage: java DataProcessor input, output, chunkSize, numOfThreads");
      System.exit(-1);
    }

    String inputDir = args[0];
    String outputDir = args[1];
    int chunkSize = Integer.parseInt(args[2]);
    int numOfThreads = Integer.parseInt(args[3]);

    DataReader reader = new DataReader();
    reader.setChunkSize(chunkSize);
    reader.setNumThread(numOfThreads);
    reader.readFile(inputDir);

    int chunkCount = reader.getChunkCount();
  }
}
