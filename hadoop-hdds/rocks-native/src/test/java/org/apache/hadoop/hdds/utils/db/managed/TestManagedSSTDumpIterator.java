package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Test for ManagedSSTDumpIterator.
 */
public class TestManagedSSTDumpIterator {

  private void testSSTDumpIteratorWithKeys(
      TreeMap<Pair<String, Integer>, String> keys) throws Exception {
    File file = File.createTempFile("tmp_sst_file", ".sst");
    file.deleteOnExit();
    try (ManagedEnvOptions envOptions = new ManagedEnvOptions();
         ManagedOptions managedOptions = new ManagedOptions();
         ManagedSstFileWriter sstFileWriter = new ManagedSstFileWriter(
        envOptions, managedOptions)) {
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
      ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(1),
          new ThreadPoolExecutor.CallerRunsPolicy());
      ManagedSSTDumpTool tool = new ManagedSSTDumpTool(executorService, 8192);
      ManagedSSTDumpIterator<ManagedSSTDumpIterator.KeyValue> iterator =
          new ManagedSSTDumpIterator<ManagedSSTDumpIterator.KeyValue>(tool,
              file.getAbsolutePath(), new ManagedOptions()) {
            @Override
            protected KeyValue getTransformedValue(KeyValue value) {
              return value;
            }
          };
      while (iterator.hasNext()) {
        ManagedSSTDumpIterator.KeyValue r = iterator.next();
        Pair<String, Integer> recordKey = Pair.of(r.getKey(), r.getType());
        Assert.assertTrue(keys.containsKey(recordKey));
        Assert.assertEquals(Optional.ofNullable(keys.get(recordKey)).orElse(""),
            r.getValue());
        keys.remove(recordKey);
      }
      Assert.assertEquals(0, keys.size());
      iterator.close();
      executorService.shutdown();
    }
  }

  @Native("Managed Rocks Tools")
  @ParameterizedTest
  @ArgumentsSource(KeyValueFormatArgumentProvider.class)
  public void testSSTDumpIteratorWithKeyFormat(String keyFormat,
              String valueFormat) throws Exception {
    TreeMap<Pair<String, Integer>, String> keys =
        IntStream.range(0, 100).boxed().collect(
            Collectors.toMap(
                i -> Pair.of(String.format(keyFormat, i), i % 2),
                i -> String.format(valueFormat, i),
                (v1, v2) -> v2,
                TreeMap::new));
    testSSTDumpIteratorWithKeys(keys);
  }
}

class KeyValueFormatArgumentProvider implements ArgumentsProvider {
  @Override
  public Stream<? extends Arguments> provideArguments(
      ExtensionContext context) {
    return Stream.of(
        Arguments.of("'key%1$d=>", "%1$dvalue'"),
        Arguments.of("key%1$d", "%1$dvalue%1$d"),
        Arguments.of("'key%1$d", "%1$d'value%1$d'"),
        Arguments.of("'key%1$d", "%1$d'value%1$d'"),
        Arguments.of("key%1$d", "%1$dvalue\n\0%1$d")
    );
  }
}
