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

/**
 * Factory for Chunk compressors/uncompressors.
 */
public class ChunkCompressorFactory {

  private static final String SNAPPY = "SNAPPY";

  /**
   * Returns the chunk compressor for the specified name.
   *
   * @param compressor Name of compressor.
   * @return Compressor for the specified name.
   */
  public static ChunkCompressor getCompressor(String compressor) {
    switch (compressor.toUpperCase()) {
      case SNAPPY:
        return new SnappyCompressor();

      default:
        throw new IllegalArgumentException("Illegal compressor name " + compressor);
    }
  }

  /**
   * Returns the chunk uncompressor for the specified name.
   *
   * @param uncompressor Name of chunk uncompressor
   * @return Uncompressor for the specified name
   */
  public static ChunkUncompressor getUncompressor(String uncompressor) {
    switch (uncompressor.toUpperCase()) {
      case SNAPPY:
        return new SnappyUncompressor();

      default:
        throw new IllegalArgumentException("Illegal compressor name " + uncompressor);
    }
  }
}
