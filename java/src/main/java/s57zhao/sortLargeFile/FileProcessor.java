package s57zhao.sortLargeFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

class FileProcessor {
  private static final Pattern PATTERN = Pattern.compile("(^[^a-z]+|[^a-z]+$)");

  static final int MB = 1048576;
  static final String CHUNK_PREFIX = "chunk-";
  static final String SHARD_PATH = "shard";

  static List<String> tokenize(String input) {
    List<String> tokens = new ArrayList<>();
    StringTokenizer itr = new StringTokenizer(input);
    while (itr.hasMoreTokens()) {
      String w = PATTERN.matcher(itr.nextToken().toLowerCase()).replaceAll("");
      if (w.length() != 0) {
        tokens.add(w);
      }
    }

    return tokens;
  }

  // used to clean up the directory if there are already files there
  static void cleanDir(String path) {
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
}
