package s57zhao.sortLargeFile;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class Reader extends FileProcessor {

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

        BufferedWriter bw = new BufferedWriter(new FileWriter(chunkFilePath));
        // skip to the start of the target chunk
        fis.skip(start);

        // to indicate where is the end of the chunk
        long totalRead = 0;
        String line = br.readLine();

        while (line!=null && totalRead < end-start) {
          wordSet.addAll(tokenize(line));
          line = br.readLine();
          if(line!=null) {
            totalRead += line.getBytes().length;
          }
        }

        for (String token : wordSet) {
          bw.write((token + "\n"));
        }

        bw.flush();
        bw.close();
        br.close();
        fis.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

      System.err.println("shard " + id + " finished sorting!");
      return null;
    }
  }

  Reader() {
    cleanDir(SHARD_PATH);
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
      long shardStart = 0;
      long shardEnd;

      FileInputStream fis = new FileInputStream(file);

      boolean endFile = false;

      while (!endFile) {
        // probing to the position, and extend the offset to the start of new line
        fis.skip(unitShardSize);
        int charRead;
        do {
          charRead = (char) fis.read();
        } while (charRead != '\n' && charRead != -1 && fis.getChannel().position() < file.length());
        shardEnd = fis.getChannel().position();

        System.err.println("shard start: " + shardStart);
        System.err.println("shard end: " + shardEnd);

        if (shardEnd >= file.length()) {
          shardEnd = file.length();
          endFile = true;
        }

        chunkCount++;
        pool.submit(new ShardWorker(inputPath, SHARD_PATH, shardStart, shardEnd, chunkCount));

        shardStart = shardEnd;
      }
      System.err.println("finished partition, number of partitions: " + chunkCount);
      fis.close();
      pool.shutdown();

      try {
        pool.awaitTermination(10, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.err.println("finished sort on each shard.");

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
