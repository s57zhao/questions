package s57zhao.sortLargeFile;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class Reader extends FileProcessor {

  // think about 24 bins
  private int SHARD_SIZE = 1;
  private int chunkCount = 0;
  private int numOfThreads = 4;

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
      String chunkFilePath = this.outputPath.concat("/" + CHUNK_PREFIX + id + ".txt");
      try {
        FileInputStream fis = new FileInputStream(inputPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(chunkFilePath), 10 * MB);
        fis.skip(start);
        while (fis.getChannel().position() <= end) {
          String line = br.readLine();
          if (line == null) {
            break;
          }
          wordSet.addAll(tokenize(line));
        }
        for (String token : wordSet) {
          os.write((token + "\n").getBytes());
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

  Reader() {
    cleanDir(this.SHARD_PATH);
  }

  void setChunkSize(int size) {
    this.SHARD_SIZE = size;
  }

  void setNumThread(int num) {
    this.numOfThreads = num;
  }

  int getChunkCount() {
    return chunkCount;
  }

  void readFile(String inputPath) {
    // start the thread pool
    ExecutorService pool = Executors.newFixedThreadPool(numOfThreads);
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
        pool.submit(new ShardWorker(inputPath, SHARD_PATH, fileStart, fileEnd, chunkCount));

        fileStart = fileEnd;
        fileEnd = fileStart + unitShardSize;
      }
      System.err.println("finished partition, number of partitions: " + chunkCount);
      fis.close();
      pool.shutdown();

      try {
        pool.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.err.println("finished sub sort.");

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
