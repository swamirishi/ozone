/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_ONLY;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;

public class TestRdbSstFileWriter {
  public static void main(String[] args) throws IOException, NativeLibraryNotLoadedException {
//    Queue<Pair<String, String>> queue = new LinkedList<>();
//    queue.add(Pair.of("/-9223372036854477824/-9223372036854477568/-9223372036854477568/key-2-465", ""));
//    queue.add(Pair.of("/-9223372036854477824/-9223372036854477568/-9223372036854477568/key-2-465_renamed", "value1"));
//    String sstFilePath = "/tmp/001060.sst";
//    String prunedFilePath = "/tmp/test1.sst";
    ManagedRawSSTFileReader.loadLibrary();
//    try (RDBSstFileWriter sstFileWriter = new RDBSstFileWriter(new File(sstFilePath))) {
//      Buffer keyBuffer = new Buffer(
//          new CodecBuffer.Capacity( "-iterator-key", 1 << 10),
//          // it has to read key for matching prefix.
//          buffer -> {
//            byte[] val = StringUtils.string2Bytes(queue.peek().getKey());
//            buffer.put(val);
//            return val.length;
//          });
//      Buffer valueBuffer = new Buffer(
//          new CodecBuffer.Capacity( "-iterator-value", 1 << 10),
//          // it has to read key for matching prefix.
//          buffer -> {
//            byte[] val = StringUtils.string2Bytes(queue.peek().getValue());
//            buffer.put(val);
//            return val.length;
//          });
//
//      while (!queue.isEmpty()) {
//        CodecBuffer key = keyBuffer.getFromDb();
//        CodecBuffer value = valueBuffer.getFromDb();
//        if (value.readableBytes() > 0) {
//          System.out.println("Putting " + queue.peek());
//          sstFileWriter.put(key, value);
//        } else {
//          System.out.println("Deleting " + queue.peek());
//          sstFileWriter.delete(key);
//        }
//        System.out.println(queue.poll());
//      }
//    }
    try {
      Queue<String> queue = new LinkedList<>();
//      queue.add("/tmp/backup1/000186.sst");
//      queue.add("/tmp/backup1/000745.sst");
//      queue.add("/tmp/backup1/000127.sst");
//      queue.add("/tmp/backup1/000542.sst");
//      queue.add("/tmp/backup1/001080.sst");
//      queue.add("/tmp/backup1/001293.sst");
//      queue.add("/tmp/backup1/000850.sst");
//      queue.add("/tmp/backup1/001303.sst");
//      queue.add("/tmp/backup1/000842.sst");
//      queue.add("/tmp/backup1/001364.sst");
//      queue.add("/tmp/backup1/001089.sst");
//      queue.add("/tmp/backup1/000816.sst");
//      queue.add("/tmp/backup1/000232.sst");
//      queue.add("/tmp/backup1/000089.sst");
//      queue.add("/tmp/backup1/001218.sst");
      queue.add("/tmp/backup1/001060.sst");
      Map<String, Queue<Pair<String, Integer>>> map = new LinkedHashMap<>();
      while (!queue.isEmpty()) {
        String sstFilePath = queue.poll();
        String prunedFilePath = sstFilePath + "_pruned.sst";
//        try (ManagedOptions options = new ManagedOptions();) {
//          RocksDBCheckpointDiffer.removeValueFromSSTFile(options, sstFilePath, prunedFilePath);
//        }

        try (ManagedOptions options = new ManagedOptions();
             ManagedRawSSTFileReader rawSSTFileReader = new ManagedRawSSTFileReader(options, sstFilePath,
                 2 * 1024 * 1024);
             ManagedRawSSTFileIterator<Pair<String, Integer>> itr =
                 rawSSTFileReader.newIterator(kv -> Pair.of(StringCodec.get().fromCodecBuffer(kv.getKey()),
                     kv.getType()), null, null, KEY_ONLY)) {

          while (itr.hasNext()) {
            map.computeIfAbsent(sstFilePath, k -> new LinkedList<>()).add(itr.next());
          }
        }
      }
      for (Map.Entry<String, Queue<Pair<String, Integer>>> entry : map.entrySet()) {
        System.out.println(entry.getKey());
        Buffer keyBuffer = new Buffer(
            new CodecBuffer.Capacity( "-iterator-key", 1 << 10),
            // it has to read key for matching prefix.
            buffer -> {
              byte[] val = StringUtils.string2Bytes(entry.getValue().peek().getKey());
              buffer.put(val);
              return val.length;
            });
        try (RDBSstFileWriter sstFileWriter = new RDBSstFileWriter(new File(entry.getKey() + "_pruned.sst"))) {
          while (entry.getValue().size() > 0) {
            CodecBuffer key = keyBuffer.getFromDb();
            Pair<String, Integer> pair = entry.getValue().poll();
            if (pair.getValue() == 0) {
              System.out.println("Deleting " + pair);
              sstFileWriter.delete(key);
            } else {
              System.out.println("Putting " + pair);
              sstFileWriter.put(key, CodecBuffer.getEmptyBuffer());
            }
          }
        }
      }

    } finally {

    }

  }
}
