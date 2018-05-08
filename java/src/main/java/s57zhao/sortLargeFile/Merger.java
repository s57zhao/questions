package s57zhao.sortLargeFile;

import java.io.*;
import java.util.*;

class Merger extends FileProcessor {

  private static final String MERGE_PATH = "merge";
  private String outputPath = null;
  private int onePassMergeSize = 5;

  void setOnePassMergeSize(int size){
    this.onePassMergeSize = size;
  }

  Merger(String outputPath){
    this.outputPath = outputPath;
    cleanDir(outputPath);
  }

  // this function will group onePassMergeSize files into one list
  // and then insert the list to another list to indicate how many rounds the merge will take
  // if there are more files than onePassMergeSize
  // several merge pass will be needed
  private List<List<String>> groupFiles(String input) {
    List<List<String>> pathGroups = new ArrayList<>();

    File inputDir = new File(input);

    if(inputDir.exists()) {
      int count = 0;
      List<String> pathGroup = new ArrayList<>();
      for (String file : Objects.requireNonNull(inputDir.list())) {
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
    List<Pair<String, BufferedReader>> fileReaderList = new ArrayList<>();

    // store each shard into a priority queue
    // the queue is order by the first word of each shard file
    Queue<Pair<String, BufferedReader>> queue = new PriorityQueue<>(
      onePassMergeSize
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
      MERGE_PASS_COUNT++;
      String tempOutputDir = MERGE_PATH + MERGE_PASS_COUNT;

      fileGroups = groupFiles(inputPath);
      if(fileGroups.size() == 1){
        tempOutputDir = outputPath;
      }
      cleanDir(tempOutputDir);
      System.err.println("merge pass " + MERGE_PASS_COUNT + " start!");


      for (int index = 0; index < fileGroups.size(); index++) {

        // each group of files will have their own output stream
        BufferedWriter bw = new BufferedWriter(
            new FileWriter(tempOutputDir + "/temp-" + index + ".txt"), 20 * MB);

        // reader each file, and store them in format of <String, FileInputReader>
        // the string is the first line of the file
        for (String filePath : fileGroups.get(index)) {
          FileInputStream fis = new FileInputStream(new File(filePath));
          BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
          String word = reader.readLine();
          // file is empty
          if (word != null) {
            fileReaderList.add(new Pair<>(word, reader));
          }
        }

        // add fileReaderList to the priority queue
        // it will sorted by the first string of the file
        queue.addAll(fileReaderList);
        String prevKey = "";


        // poll one element from the queue each time
        // write out only if the element is different from the previous one
        // if the reader is not reach the end, add it back to the priority queue
        while (!queue.isEmpty()) {
          Pair<String, BufferedReader> val = queue.poll();
          String key = val.getKey();
          if (!key.equals(prevKey)) {
            bw.write((key + "\n"));
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

        // close everything
        bw.flush();
        bw.close();
        for (Pair<String, BufferedReader> item : fileReaderList) {
          item.getValue().close();
        }

        fileReaderList.clear();
      }
      System.err.println("merge pass " + MERGE_PASS_COUNT + " end!");
      // swap for next round
      inputPath = tempOutputDir;
    }while(fileGroups.size() > 1);
  }
}
