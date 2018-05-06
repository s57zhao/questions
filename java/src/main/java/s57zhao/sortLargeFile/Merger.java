package s57zhao.sortLargeFile;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

class Merger extends FileProcessor {

  private static final String MERGE_PATH = "merge";
  private String outputPath = null;
  private int onePassMergeSize = 5;
  private int chunkCount;
  private int numPass = 0;
  private Set<String> sortedSet = new LinkedHashSet<>();

  public void setOnePassMergeSize(int size){
    this.onePassMergeSize = size;
  }

  Merger(int chunkCount, String outputPath){
    this.chunkCount = chunkCount;
    this.outputPath = outputPath;
    cleanDir(outputPath);
  }

  // this function will group files into one list
  private List<List<String>> groupFiles(String input) {
    List<List<String>> pathGroups = new ArrayList<>();

    File inputDir = new File(input);

    if(inputDir.exists()) {
      int count = 0;
      List<String> pathGroup = new ArrayList<>();
      for (String file : inputDir.list()) {
        pathGroup.add(inputDir.getPath() + "/" + file);
        count++;
        if(count == onePassMergeSize){
          pathGroups.add(pathGroup);
          pathGroup = new ArrayList<>();
          count = 0;
        }
      }
      if(pathGroup.size() > 0){
        pathGroups.add(pathGroup);
      }
    }

    return pathGroups;
  }

  void merge() throws IOException {


    List<Pair<String, BufferedReader>> externalList = new ArrayList<>();
    Queue<Pair<String, BufferedReader>> queue = new PriorityQueue<>(
      onePassMergeSize //initialCapacity
      , new Comparator<Pair<String, BufferedReader>>() {
      public int compare(Pair<String, BufferedReader> p1,
                         Pair<String, BufferedReader> p2) {
        return p1.key.compareTo(p2.key);
      }
    });

    int MERGE_PASS_COUNT = 0;
    List<List<String>> fileGroups;
    String inputPath = SHARD_PATH;
    do {
      fileGroups = groupFiles(inputPath);
      MERGE_PASS_COUNT++;
      String tempOutputDir = MERGE_PATH + MERGE_PASS_COUNT;
      if(fileGroups.size() == 1){
        tempOutputDir = outputPath;
      }
      cleanDir(tempOutputDir);
      System.err.println("merge pass " + MERGE_PASS_COUNT + " start!");
      inputPath = tempOutputDir;

      // TODO: I can thread this, but not worth the effort?
      for (int index = 0; index < fileGroups.size(); index++) {
        for (String filePath : fileGroups.get(index)) {
          FileInputStream fis = new FileInputStream(new File(filePath));
          BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
          String word = reader.readLine();
          if (word != null) {
            externalList.add(new Pair<>(word, reader));
          }
        }

        BufferedOutputStream os = new BufferedOutputStream(
            new FileOutputStream(tempOutputDir + "/temp-" + index + ".txt"), 20 * MB);
        queue.addAll(externalList);
        String prevKey = "";
        while (!queue.isEmpty()) {
          Pair<String, BufferedReader> val = queue.poll();
          String key = val.getKey();
          if (!key.equals(prevKey)) {
            os.write((key + "\n").getBytes());
            prevKey = key;
          }

          String newKey = val.getValue().readLine();
          if (newKey == null) {
            val.getValue().close();
          } else {
            val.setKey(newKey);
            queue.add(val);
          }
        }

        os.flush();
        for (Pair<String, BufferedReader> item : externalList) {
          item.getValue().close();
        }
        externalList.clear();
      }
    }while(fileGroups.size() > 1);
  }

  public static void main(String[]args){
    Merger merger = new Merger(10, "output");
    try {
      merger.merge();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
