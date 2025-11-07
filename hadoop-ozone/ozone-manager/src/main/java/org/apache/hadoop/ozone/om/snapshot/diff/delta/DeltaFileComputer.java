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

package org.apache.hadoop.ozone.om.snapshot.diff.delta;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ozone.rocksdb.util.SstFileInfo;

/**
 * Computes the set of delta files that represent changes between two
 * snapshots. Implementations perform the comparison of {@link SnapshotInfo}
 * instances and produce a list of filesystem {@link Path}s that contain the
 * computed delta. The returned paths typically point to files that can be
 * consumed by downstream snapshot/diff consumers.
 *
 * <p>Implementations may hold resources (temporary files, open streams, etc.)
 * and should be closed when no longer needed. This interface extends
 * {@link Closeable} so callers can release such resources via {@link #close()}.
 */
public interface DeltaFileComputer extends Closeable {

  /**
   * Compute the delta files that describe changes from {@code fromSnapshot}
   * to {@code toSnapshot}.
   *
   * @param fromSnapshot the earlier snapshot (the baseline). Not {@code null}.
   * @param toSnapshot the later snapshot (the target). Not {@code null}.
   * @param tablesToLookup an optional set of table names used by the
   *                       computation to resolve metadata or perform lookups.
   *                       Implementations should tolerate an empty set.
   * @return an {@link Optional} containing a {@link List} of {@link Path} to the
   *         computed delta files. Returns {@link Optional#empty()} if no delta
   *         files are produced or if computation is not applicable.
   * @throws IOException if an I/O error occurs while computing the delta.
   */
  Optional<Collection<Pair<Path, SstFileInfo>>> getDeltaFiles(SnapshotInfo fromSnapshot, SnapshotInfo toSnapshot,
      Set<String> tablesToLookup) throws IOException;
}
