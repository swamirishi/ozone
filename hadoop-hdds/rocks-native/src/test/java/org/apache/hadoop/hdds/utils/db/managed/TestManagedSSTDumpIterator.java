package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Test for ManagedSSTDumpIterator.
 */
public class TestManagedSSTDumpIterator {

  private static final String DELETE_SUFFIX = "delete";
  private static final String CREATE_SUFFIX = "put";

  @Test
  @Category(NativeTests.class)
  public void testSSTDumpIterator() throws Exception {

    File file = File.createTempFile("tmp_sst_file", ".sst");
    file = new File("/tmp/hi.sst");
    try (ManagedSstFileWriter sstFileWriter = new ManagedSstFileWriter(
        new ManagedEnvOptions(), new ManagedOptions())) {
      sstFileWriter.open(file.getAbsolutePath());
      Map<Pair<String, Integer>, String> keys = new TreeMap<>();
      IntStream.range(0, 100).forEach(i -> {
        if (i % 10 == 0) {
          keys.put(Pair.of("'key" + i + "'=>" + DELETE_SUFFIX, 0), null);
        } else {
          keys.put(Pair.of("'key" + i + "'=>" + CREATE_SUFFIX, 1),
              i + "value\n");
        }
      });
      for (Map.Entry<Pair<String, Integer>, String> entry : keys.entrySet()) {
        if (entry.getKey().getValue() == 0) {
          sstFileWriter.delete(entry.getKey().getKey()
              .getBytes(StandardCharsets.UTF_8));
        } else {
          sstFileWriter.put(entry.getKey().getKey()
                  .getBytes(StandardCharsets.UTF_8),
              entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
      }
      sstFileWriter.finish();
      sstFileWriter.close();
      ManagedSSTDumpTool tool = new ManagedSSTDumpTool(
          new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
              new ArrayBlockingQueue<>(1),
              new ThreadPoolExecutor.CallerRunsPolicy()), 8192);
      ManagedSSTDumpIterator iterator = new ManagedSSTDumpIterator(tool,
          file.getAbsolutePath(), new ManagedOptions());
      while (iterator.hasNext()) {
        ManagedSSTDumpIterator.KeyValue r = iterator.next();
        if (r.getType() == 0) {
          Assert.assertTrue(r.getKey().endsWith(DELETE_SUFFIX));
        } else {
          Assert.assertTrue(r.getKey().endsWith(CREATE_SUFFIX));
        }
      }
      iterator.close();
      System.out.println("Test native completed");
    }
  }
}
