import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataReader extends Tokenizer {

  // think about 24 bins
  private int SHARD_SIZE = 50;
  private int MB = 1048576;
  private int chunkCount = 0;
  private int NUM_THREAD = 4;

  private String TEMP_PATH = "temp";

  // each worker read a portion of the file
  class ShardWorker implements Callable<Void> {
    private String inputPath;
    private String outputPath;
    private long start;
    private long end;
    private int id;
    private Set<String> wordSet = new TreeSet<String>();

    ShardWorker(String inputPath, String outputPath, long start, long end, int id) {
      this.inputPath = inputPath;
      this.outputPath = outputPath;
      this.start = start;
      this.end = end;
      this.id = id;
    }

    @Override
    public Void call() {
      String chunkFilePath = this.outputPath.concat("/chunk-" + this.id + ".txt");
      try {
        FileInputStream fis = new FileInputStream(inputPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        OutputStream os = new BufferedOutputStream(new FileOutputStream(chunkFilePath));
        fis.skip(start);
        while (fis.getChannel().position() <= end) {
          String line = br.readLine();
          if (line == null) {
            break;
          }
          wordSet.addAll(tokenize(line));
        }
        for (String token : wordSet) {
          os.write((token + " ").getBytes());
        }
        os.flush();
        br.close();
        fis.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  DataReader() {
    cleanDir(this.TEMP_PATH);
  }

  private void cleanDir(String path) {
    File outFile = new File(path);

    // remove previous files
    if (outFile.exists()) {
      for (String dir : outFile.list()) {
        File curFile = new File(outFile.getPath(), dir);
        curFile.delete();
      }
    }

    outFile.mkdir();
  }

  void setChunkSize(int size) {
    this.SHARD_SIZE = size;
  }

  void setNumThread(int num) {
    this.NUM_THREAD = num;
  }

  void readFile(String inputPath) {
    // start the thread pool
    ExecutorService pool = Executors.newFixedThreadPool(NUM_THREAD);
    File file = new File(inputPath);
    int unitShardSize = SHARD_SIZE * MB;
    try {
      long fileStart = 0;
      long fileEnd = unitShardSize;

      FileInputStream fis = new FileInputStream(file);

      boolean endFile = false;

      while (!endFile) {
        // probing to the position, and extend the offset to the start of new line
        fis.skip(unitShardSize);
        int charRead;
        do {
          charRead = (char) fis.read();
          fileEnd++;
        } while (charRead != '\n' && charRead != -1 && fis.getChannel().position() < file.length());

        fileEnd = fis.getChannel().position();

        if (fileEnd >= file.length()) {
          fileEnd = file.length();
          endFile = true;
        }

        chunkCount++;
        pool.submit(new ShardWorker(inputPath, TEMP_PATH, fileStart, fileEnd, chunkCount));
        System.out.println(fileStart);
        System.out.println(fileEnd);

        fileStart = fileEnd;
        fileEnd = fileStart + unitShardSize;
      }
      System.out.println("finished partition, number: " + chunkCount);
      fis.close();
      pool.shutdown();

      try {
        pool.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println("finished sub sort.");

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  int getChunkCount() {
    return chunkCount;
  }

  // test usage
  public static void main(String args[]) {
    DataReader ds = new DataReader();
    ds.readFile("data/enwik9.txt");
  }
}
