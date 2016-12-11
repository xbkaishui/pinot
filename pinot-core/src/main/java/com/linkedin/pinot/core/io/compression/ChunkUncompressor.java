/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.io.compression;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Interface to uncompress a chunk of data.
 */
public interface ChunkUncompressor {

  /**
   * This method uncompresses chunk of data that was compressed using {@link ChunkCompressor}.
   *
   * @param inCompressed Compressed data
   * @param outUncompressed ByteBuffer where the uncompressed data is put.
   * @return Size of uncompressed data.
   * @throws IOException
   */
  int uncompress(ByteBuffer inCompressed, ByteBuffer outUncompressed)
      throws IOException;
}
