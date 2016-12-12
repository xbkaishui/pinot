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
package com.linkedin.pinot.tools.segment.converter;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.io.writer.impl.v1.VarByteSingleValueWriter;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to convert segment with dictionary encoded column to raw index (without dictionary).
 */
@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class DictionaryToRawIndexConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryToRawIndexConverter.class);
  private static final int DEFAULT_NUM_DOCS_PER_CHUNK = 1000;

  @Option(name = "-dataDir", required = true, usage = "Directory containing uncompressed segments")
  private String _dataDir = null;

  @Option(name = "-columns", required = true, usage = "Comma separated list of column names to convert")
  private String _columns = null;

  @Option(name = "-outputDir", required = true, usage = "Output directory for writing results")
  private String _outputDir = null;

  @Option(name = "-overwrite", required = true, usage = "Overwrite output directory")
  private boolean _overwrite = false;

  @Option(name = "-numDocsPerChunk", required = false, usage = "Number of docs per chunk in the output raw index.")
  private int _numDocsPerChunk = DEFAULT_NUM_DOCS_PER_CHUNK;

  @Option(name = "-help", required = false, help = true, aliases = {"-h"}, usage = "print this message")
  private boolean _help = false;

  /**
   * Setter for {@link #_dataDir}
   * @param dataDir Data directory containing un-tarred segments.
   * @return this
   */
  public DictionaryToRawIndexConverter setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  /**
   * Setter for {@link #_outputDir}
   *
   * @param outputDir Directory where output segments should be written
   * @return this
   */
  public DictionaryToRawIndexConverter setOutputDir(String outputDir) {
    _outputDir = outputDir;
    return this;
  }

  /**
   * Setter for columns to convert.
   *
   * @param columns Comma separated list of columns
   * @return this
   */
  public DictionaryToRawIndexConverter setColumns(String columns) {
    _columns = columns;
    return this;
  }

  /**
   * Setter for {@link #_overwrite}
   * When set to true, already existing output directory is overwritten.
   *
   * @param overwrite True for overwriting existing output dir, False otherwise
   * @return this
   */
  public DictionaryToRawIndexConverter setOverwrite(boolean overwrite) {
    _overwrite = overwrite;
    return this;
  }

  /**
   * Setter for {@link #_numDocsPerChunk}. Specifies number of docs per chunk
   * in the raw index to be created.
   *
   * @param numDocsPerChunk Number of docs per chunk
   * @return this
   */
  public DictionaryToRawIndexConverter setNumDocsPerChunk(int numDocsPerChunk) {
    _numDocsPerChunk = numDocsPerChunk;
    return this;
  }

  /**
   * Method to perform the conversion for a set of segments in the {@link #_dataDir}
   *
   * @return True if successful, False otherwise
   * @throws Exception
   */
  public boolean convert()
      throws Exception {
    if (_help) {
      printUsage();
      return true;
    }

    File dataDir = new File(_dataDir);
    File outputDir = new File(_outputDir);

    if (!dataDir.exists()) {
      LOGGER.error("Data directory '{}' does not exist.", _dataDir);
      return false;
    } else if (outputDir.exists()) {
      if (_overwrite) {
        LOGGER.info("Overwriting existing output directory '{}'", _outputDir);
        FileUtils.deleteQuietly(outputDir);
        outputDir = new File(_outputDir);
        outputDir.mkdir();
      } else {
        LOGGER.error("Output directory '{}' already exists, use -overwrite to overwrite");
        return false;
      }
    }

    File[] segmentFiles = dataDir.listFiles();
    if (segmentFiles == null || segmentFiles.length == 0) {
      LOGGER.error("Empty data directory '{}'.", _dataDir);
      return false;
    }

    boolean ret = true;
    for (File segmentDir : segmentFiles) {
      ret = ret && convertSegment(segmentDir, _columns.split("\\s*,\\s*"), outputDir);
    }
    return ret;
  }

  /**
   * This method converts the specified columns of the given segment from dictionary encoded
   * forward index to raw index without dictionary.
   *
   * @param segmentDir Segment directory
   * @param columns Columns to convert
   * @param outputDir Directory for writing output segment
   * @return True if successful, False otherwise
   * @throws Exception
   */
  public boolean convertSegment(File segmentDir, String[] columns, File outputDir)
      throws Exception {
    if (segmentDir.isFile()) {
      LOGGER.warn("Skipping non-segment file '{}'", segmentDir.getAbsoluteFile());
      return false;
    }

    File newSegment = new File(outputDir, segmentDir.getName());
    newSegment.mkdir();
    FileUtils.copyDirectory(segmentDir, newSegment);
    IndexSegment segment = Loaders.IndexSegment.load(newSegment, ReadMode.mmap);

    for (String column : columns) {
      File rawIndexFile = new File(newSegment, column + V1Constants.Indexes.RAW_SV_FWD_IDX_FILE_EXTENTION);
      convertOneColumn(segment, column, rawIndexFile);
    }

    updateMetadata(newSegment, columns);
    return true;
  }

  /**
   * Helper method to update the metadata.properties for the converted segment.
   *
   * @param segmentDir Segment directory
   * @param columns Converted columns
   * @throws IOException
   * @throws ConfigurationException
   */
  private void updateMetadata(File segmentDir, String[] columns)
      throws IOException, ConfigurationException {
    File metadataFile = new File(segmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    PropertiesConfiguration properties = new PropertiesConfiguration(metadataFile);

    for (String column : columns) {
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_DICTIONARY), false);
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT), -1);
    }
    properties.save();
  }

  /**
   * Helper method to print usage at the command line interface.
   */
  private static void printUsage() {
    System.out.println("Usage: DictionaryTORawIndexConverter");
    for (Field field : ColumnarToStarTreeConverter.class.getDeclaredFields()) {

      if (field.isAnnotationPresent(Option.class)) {
        Option option = field.getAnnotation(Option.class);

        System.out.println(
            String.format("\t%-15s: %s (required=%s)", option.name(), option.usage(), option.required()));
      }
    }
  }

  /**
   * Helper method to perform conversion for the specific column.
   *
   * @param segment Input segment to convert
   * @param column Column to convert
   * @param rawIndexFile File where raw index to be written
   * @throws IOException
   */
  private void convertOneColumn(IndexSegment segment, String column, File rawIndexFile)
      throws IOException {
    DataSource dataSource = segment.getDataSource(column);
    Dictionary dictionary = dataSource.getDictionary();

    if (dictionary == null) {
      LOGGER.error("Column '{}' does not have dictionary, cannot convert to raw index.", column);
      return;
    }

    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    if (!dataSourceMetadata.isSingleValue()) {
      LOGGER.error("Cannot convert multi-valued columns '{}'", column);
      return;
    }

    if (dataSourceMetadata.getDataType() != FieldSpec.DataType.STRING) {
      LOGGER.error("Cannot convert non-string type column '{}'.", column);
    }

    int totalDocs = segment.getSegmentMetadata().getTotalDocs();
    BlockSingleValIterator bvIter = (BlockSingleValIterator) dataSource.getNextBlock().getBlockValueSet().iterator();
    int lengthOfLongestEntry = getLengthOfLongestEntry(bvIter, dictionary);

    VarByteSingleValueWriter rawIndexWriter =
        new VarByteSingleValueWriter(rawIndexFile, ChunkCompressorFactory.getCompressor("snappy"), totalDocs,
            _numDocsPerChunk, lengthOfLongestEntry);

    int docId = 0;
    bvIter.reset();
    while (bvIter.hasNext()) {
      int dictId = bvIter.nextIntVal();
      String value = (String) dictionary.get(dictId);
      rawIndexWriter.setString(docId++, value);

      if (docId % 1000000 == 0) {
        LOGGER.info("Converted {} records.", docId);
      }
    }
    rawIndexWriter.close();
    deleteForwardIndex(rawIndexFile.getParentFile(), column, dataSourceMetadata.isSorted());
  }

  /**
   * Helper method to remove the forward index for the given column.
   *
   * @param segmentDir Segment directory from which to remove the forward index.
   * @param column Column for which to remove the index.
   * @param sorted True if column is sorted, False otherwise
   */
  private void deleteForwardIndex(File segmentDir, String column, boolean sorted) {
    File dictionaryFile = new File(segmentDir, (column + V1Constants.Dict.FILE_EXTENTION));
    FileUtils.deleteQuietly(dictionaryFile);

    String fwdIndexFileExtension = (sorted) ? V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION
        : V1Constants.Indexes.UN_SORTED_SV_FWD_IDX_FILE_EXTENTION;
    File fwdIndexFile = new File(segmentDir, (column + fwdIndexFileExtension));
    FileUtils.deleteQuietly(fwdIndexFile);
  }

  /**
   * Helper method to get the length
   * @param bvIter Data source blockvalset iterator
   * @param dictionary Column dictionary
   * @return Length of longest entry
   */
  private int getLengthOfLongestEntry(BlockSingleValIterator bvIter, Dictionary dictionary) {
    int lengthOfLongestEntry = 0;

    bvIter.reset();
    while (bvIter.hasNext()) {
      int dictId = bvIter.nextIntVal();
      String value = (String) dictionary.get(dictId);
      lengthOfLongestEntry = Math.max(lengthOfLongestEntry, value.getBytes().length);
    }

    return lengthOfLongestEntry;
  }

  /**
   * Main method for the class.
   *
   * @param args Arguments for the converter
   * @throws Exception
   */
  public static void main(String[] args)
      throws Exception {
    DictionaryToRawIndexConverter converter = new DictionaryToRawIndexConverter();
    CmdLineParser parser = new CmdLineParser(converter);
    parser.parseArgument(args);
    converter.convert();
  }
}
