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
package com.linkedin.pinot.perf;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.TestRecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.operator.ArrayBasedFilterBlock;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public class RawIndexBenchmark {
  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "rawIndexPerf";
  private static final String SEGMENT_NAME = "perfTestSegment";
  private static final int NUM_COLUMNS = 2;

  private static final String RAW_INDEX_COLUMN = "column_0";
  private static final String FWD_INDEX_COLUMN = "column_1";
  private static final int DEFAULT_NUM_LOOKUP = 100_000;
  private static final int DEFAULT_NUM_CONSECUTIVE_LOOKUP = 20;

  @Option(name = "-dataFile", required = true, usage = "File containing input data (one string per line)")
  private String _dataFile = null;

  @Option(name = "-loadMode", required = false, usage = "Load mode for data (mmap|heap")
  private String _loadMode = "heap";

  @Option(name = "-numLookups", required = false, usage = "Number of lookups to be performed for benchmark")
  private int _numLookups = DEFAULT_NUM_LOOKUP;

  @Option(name = "-numConsecutiveLookups", required = false, usage = "Number of consecutive docIds to lookup")
  private int _numConsecutiveLookups = DEFAULT_NUM_CONSECUTIVE_LOOKUP;

  @Option(name = "-help", required = false, help = true, aliases = {"-h"}, usage = "print this message")
  private boolean _help = false;

  private int _numRows = 0;

  public void run()
      throws Exception {
    File segmentFile = buildSegment();
    IndexSegment segment = Loaders.IndexSegment.load(segmentFile, ReadMode.valueOf(_loadMode));
    compareIndexSizes(segment);
    compareLookups(segment);
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }

  /**
   * Helper method that builds a segment containing two columns both with data from input file.
   * The first column has raw indices (no dictionary), where as the second column is dictionary encoded.
   *
   * @throws Exception
   */
  private File buildSegment()
      throws Exception {
    Schema schema = new Schema();

    for (int i = 0; i < NUM_COLUMNS; i++) {
      String column = "column_" + i;
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(column, FieldSpec.DataType.STRING, true);
      schema.addField(dimensionFieldSpec);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setRawIndexCreationColumns(Collections.singletonList(RAW_INDEX_COLUMN));

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    BufferedReader reader = new BufferedReader(new FileReader(_dataFile));
    String value;

    final List<GenericRow> rows = new ArrayList<>();

    System.out.println("Reading data...");
    while ((value = reader.readLine()) != null) {
      HashMap<String, Object> map = new HashMap<>();

      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        map.put(fieldSpec.getName(), new String(value));
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
      _numRows++;

      if (_numRows % 1000000 == 0) {
        System.out.println("Read rows: " + _numRows);
      }
    }

    System.out.println("Generating segment...");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    RecordReader recordReader = new TestRecordReader(rows, schema);

    driver.init(config, recordReader);
    driver.build();

    return new File(SEGMENT_DIR_NAME, SEGMENT_NAME);
  }

  /**
   * Compares and prints the index size for the raw and dictionary encoded columns.
   * @param segment
   */
  private void compareIndexSizes(IndexSegment segment) {
    String filePrefix = SEGMENT_DIR_NAME + File.separator + SEGMENT_NAME + File.separator;
    File rawIndexFile = new File(filePrefix + RAW_INDEX_COLUMN + V1Constants.Indexes.RAW_SV_FWD_IDX_FILE_EXTENTION);

    String extension = (segment.getDataSource(FWD_INDEX_COLUMN).getDataSourceMetadata().isSorted())
        ? V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION : V1Constants.Indexes.UN_SORTED_SV_FWD_IDX_FILE_EXTENTION;
    File fwdIndexFile = new File(filePrefix + FWD_INDEX_COLUMN + extension);

    long rawIndexSize = rawIndexFile.length();
    long fwdIndexSize = fwdIndexFile.length();

    System.out.println("Raw index size: " + FileUtils.byteCountToDisplaySize(rawIndexSize) + " ms.");
    System.out.println("Fwd index size: " + FileUtils.byteCountToDisplaySize(fwdIndexSize) + " ms.");
    System.out.println("Storage space saving: " + ((fwdIndexSize - rawIndexSize) * 100.0 / fwdIndexSize) + " %");
  }


  private void compareLookups(IndexSegment segment) {
    int[] filteredDocIds = generateFilteredDocIds(segment);
    long rawIndexTime = profileLookups(segment, RAW_INDEX_COLUMN, filteredDocIds);
    long fwdIndexTime = profileLookups(segment, FWD_INDEX_COLUMN, filteredDocIds);

    System.out.println("Raw index lookup time: " + rawIndexTime);
    System.out.println("Fwd index lookup time: " + fwdIndexTime);
    System.out.println("Percentage change: " + ((fwdIndexTime - rawIndexTime) * 100.0 / rawIndexTime) + " %");
  }

  private long profileLookups(IndexSegment segment, String column, int[] filteredDocIds) {
    BaseFilterOperator filterOperator = new TestFilterOperator(filteredDocIds);
    BReusableFilteredDocIdSetOperator
        docIdSetOperator = new BReusableFilteredDocIdSetOperator(filterOperator, filteredDocIds.length,
        DocIdSetPlanNode.MAX_DOC_PER_CALL);

    ProjectionBlock projectionBlock;
    MProjectionOperator projectionOperator = new MProjectionOperator(buildDataSource(segment), docIdSetOperator);

    long start = System.currentTimeMillis();
    while ((projectionBlock = (ProjectionBlock) projectionOperator.nextBlock()) != null) {
      Block dataBlock = projectionBlock.getDataBlock(column);
      ProjectionBlockValSet blockValueSet = (ProjectionBlockValSet) dataBlock.getBlockValueSet();
      blockValueSet.getSingleValues();
    }
    return (System.currentTimeMillis() - start);
  }

  private Map<String, BaseOperator> buildDataSource(IndexSegment segment) {
    Map<String, BaseOperator> dataSourceMap = new HashMap<>();
    for (String column : segment.getColumnNames()) {
      dataSourceMap.put(column, segment.getDataSource(column));
    }
    return dataSourceMap;
  }

  private int[] generateFilteredDocIds(IndexSegment segment) {
    Random random = new Random();
    int numDocs = segment.getSegmentMetadata().getTotalDocs();
    int maxDocId = numDocs - _numConsecutiveLookups - 1;

    int[] docIdSet = new int[_numLookups];
    int j = 0;
    for (int i = 0; i < (_numLookups / _numConsecutiveLookups); i++) {
      int startDocId = random.nextInt(maxDocId);
      int endDocId = startDocId + _numConsecutiveLookups;

      for (int docId = startDocId; docId < endDocId; docId++) {
        docIdSet[j++] = docId;
      }
    }

    int docId = random.nextInt(maxDocId);
    for (; j < _numLookups; ++j) {
      docIdSet[j] = docId++;
    }
    return docIdSet;
  }

  class TestFilterOperator extends BaseFilterOperator {
    private int[] _filteredDocIds;

    public TestFilterOperator(int[] filteredDocIds) {
      _filteredDocIds = filteredDocIds;
    }

    @Override
    public BaseFilterBlock nextFilterBlock(BlockId blockId) {
      return new ArrayBasedFilterBlock(_filteredDocIds);
    }

    @Override
    public boolean open() {
      return true;
    }

    @Override
    public boolean close() {
      return true;
    }
  }

  public static void main(String[] args)
      throws Exception {
    RawIndexBenchmark benchmark = new RawIndexBenchmark();
    CmdLineParser parser = new CmdLineParser(benchmark);
    parser.parseArgument(args);
    benchmark.run();
  }
}
