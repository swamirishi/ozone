package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Test for ManagedSSTDumpIterator.
 */
public class TestManagedSSTDumpIterator {

  private void testSSTDumpIteratorWithKeys(
      Map<Pair<String, Integer>, String> records)
      throws Exception {
    Map<Pair<String, Integer>, String> keys = records instanceof TreeMap ?
        records : new TreeMap<>(records);
    File file = File.createTempFile("tmp_sst_file", ".sst");
    file.deleteOnExit();
    try (ManagedSstFileWriter sstFileWriter = new ManagedSstFileWriter(
        new ManagedEnvOptions(), new ManagedOptions())) {
      sstFileWriter.open(file.getAbsolutePath());
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
        Pair<String, Integer> recordKey = Pair.of(r.getKey(), r.getType());
        Assert.assertTrue(keys.containsKey(recordKey));
        Assert.assertEquals(Optional.ofNullable(keys.get(recordKey)).orElse(""),
            r.getValue());
        keys.remove(recordKey);
      }
      Assert.assertEquals(keys.size(), 0);
      iterator.close();
    }
  }
  @Test
  @Category(NativeTests.class)
  public void testSSTDumpIteratorWithKeySingleQuotes() throws Exception {
    Map<Pair<String, Integer>, String> keys = new TreeMap<>();
    IntStream.range(0, 100).forEach(i -> {
      if (i % 10 == 0) {
        keys.put(Pair.of("'key" + i, 0), null);
      } else {
        keys.put(Pair.of("'key" + i, 1),
            i + "value\n");
      }
    });
    testSSTDumpIteratorWithKeys(keys);
  }

  @Test
  @Category(NativeTests.class)
  public void testSSTDumpIteratorWithValueHavingSingleQuotes()
      throws Exception {
    Map<Pair<String, Integer>, String> keys = new TreeMap<>();
    IntStream.range(0, 100).forEach(i -> {
      if (i % 10 == 0) {
        keys.put(Pair.of("'key" + i, 0), null);
      } else {
        keys.put(Pair.of("'key" + i, 1),
            i + "value\n'");
      }
    });
    testSSTDumpIteratorWithKeys(keys);
  }

  @Test
  @Category(NativeTests.class)
  public void testSSTDumpIteratorWithKeyHavingEqualGreaterThan()
      throws Exception {
    Map<Pair<String, Integer>, String> keys = new TreeMap<>();
    IntStream.range(0, 100).forEach(i -> {
      if (i % 10 == 0) {
        keys.put(Pair.of("'key" + i + "=>", 0), null);
      } else {
        keys.put(Pair.of("'key" + i + "=>", 1),
            i + "value\n'");
      }
    });
    testSSTDumpIteratorWithKeys(keys);
  }
}
