/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CreateSnapshot {
  ExecutorService executorService;
  private String volume;
  private String bucket;

  private int start;
  byte[] keyContent;
  private int numberKeysPerSnapshot;
  private int numberOfSnapshot;
  private int numberOfOpsPerThread;
  private OzoneConfiguration ozoneConfiguration;
  private OzoneAddress ozoneAddress;

  private static final Logger LOG =
      LoggerFactory.getLogger(CreateSnapshot.class);

  public CreateSnapshot(String volume, String bucket,
                        int numberKeysPerSnapshot,
                        int numberOfSnapshot, int threadCnt,
                        int numberOfOpsPerThread, int start) throws IOException {
    this.volume = volume;
    this.bucket = bucket;
    this.start = start;
    executorService = new ThreadPoolExecutor(threadCnt,
        threadCnt, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(numberKeysPerSnapshot), new ThreadFactoryBuilder()
        .setNameFormat("create key")
        .build());
    this.keyContent = "hello".getBytes(StandardCharsets.UTF_8);
    this.numberKeysPerSnapshot = numberKeysPerSnapshot;
    this.numberOfSnapshot = numberOfSnapshot;
    this.numberOfOpsPerThread = numberOfOpsPerThread;
  }


  public void run() throws IOException, OzoneClientException {
    this.ozoneAddress = new OzoneAddress("o3://ozone1/");
    this.ozoneConfiguration = new OzoneConfiguration();
    LOG.info("Creating ozone client");
    try (OzoneClient ozoneClient = ozoneAddress.createClient(ozoneConfiguration)){

      try {
        ozoneClient.getObjectStore().getVolume(volume);
      } catch (Exception e) {
        LOG.info("Creating Volume {}", volume);
        ozoneClient.getObjectStore().createVolume(volume);
      }

      OzoneVolume volume1 = ozoneClient.getObjectStore().getVolume(volume);
      try {
        volume1.getBucket(bucket);
      } catch(Exception e) {
        LOG.info("Creating Bucket {}", bucket);
        ozoneClient.getObjectStore().getVolume(volume).createBucket(bucket);
      }
      long keyCnt = 0;
      OzoneBucket ozoneBucket = volume1.getBucket(bucket);
      for (int snapCnt = 0; snapCnt < numberOfSnapshot; snapCnt ++) {
        if (keyCnt >= start) {
          createKeysInParallel(ozoneBucket, keyCnt, keyCnt + numberKeysPerSnapshot);
          LOG.info("Creating Snapshot snap{}", snapCnt);
          ozoneClient.getObjectStore().createSnapshot(volume, bucket,
              String.format("snap%d", snapCnt));
        }
        keyCnt += numberKeysPerSnapshot;
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      executorService.shutdown();
    }

  }

  private void createKeysInParallel(OzoneBucket ozoneBucket, long start,
                                    long end)
      throws InterruptedException {
    LOG.info("Creating key b/w {} - {}", start, end);
    List<Future<?>> futures = new ArrayList<>();
    long suffix = start;
    while (suffix <= end) {
      long finalSuffix = suffix;
      long endSuffix = Math.min(suffix + numberOfOpsPerThread - 1, end);
      futures.add(executorService.submit(() -> {
//        try (OzoneClient ozoneClient = ozoneAddress.createClient(ozoneConfiguration)){
        {
          LOG.info("Creating keys => key{} -> key{}", finalSuffix, endSuffix);
          for (long s = finalSuffix; s<=endSuffix; s++) {
            try (OzoneOutputStream ozoneOutputStream = ozoneBucket
                .createKey(String.format("key%d",
                    finalSuffix), keyContent.length)){
              ozoneOutputStream.write(keyContent);
            } catch (IOException e) {
              LOG.error("Creation of key{} failed", finalSuffix, e);
              return 1;
            }
          }
        }

        return 0;
      }));

      suffix = endSuffix + 1;
    }
    int cnt  =0;
    for (Future<?> future: futures) {
      while (!future.isDone()) {
        LOG.info("Waiting on {}", start + cnt);
        Thread.sleep(1000);
      }
    }
  }
  public static void main(String[] args)
      throws IOException, OzoneClientException {
    System.out.println(Arrays.toString(args));
    new CreateSnapshot(args[0], args[1], Integer.parseInt(args[2]),
        Integer.parseInt(args[3]), Integer.parseInt(args[4]),
        Integer.parseInt(args[5]),
        Integer.parseInt(args.length == 7 ? args[6] : "0")).run();
  }
}
