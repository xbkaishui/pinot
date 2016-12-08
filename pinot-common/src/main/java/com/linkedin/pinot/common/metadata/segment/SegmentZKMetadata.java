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
package com.linkedin.pinot.common.metadata.segment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.metadata.ZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import static com.linkedin.pinot.common.utils.EqualityUtils.isEqual;
import static com.linkedin.pinot.common.utils.EqualityUtils.hashCodeOf;
import static com.linkedin.pinot.common.utils.EqualityUtils.isEqualIgnoreOrder;
import static com.linkedin.pinot.common.utils.EqualityUtils.isSameReference;
import static com.linkedin.pinot.common.utils.EqualityUtils.isNullOrNotSameClass;


public abstract class SegmentZKMetadata implements ZKMetadata {

  private static final String NULL = "null";

  private String _segmentName = null;
  private String _tableName = null;
  private SegmentType _segmentType = null;
  private long _startTime = -1;
  private long _endTime = -1;
  private TimeUnit _timeUnit = null;
  private String _indexVersion = null;
  private long _totalRawDocs = -1;
  private long _crc = -1;
  private long _creationTime = -1;
  private int _sizeThresholdToFlushSegment = -1;

  public SegmentZKMetadata() {
  }

  public SegmentZKMetadata(ZNRecord znRecord) {
    _segmentName = znRecord.getSimpleField(CommonConstants.Segment.SEGMENT_NAME);
    _tableName = znRecord.getSimpleField(CommonConstants.Segment.TABLE_NAME);
    _segmentType = znRecord.getEnumField(CommonConstants.Segment.SEGMENT_TYPE, SegmentType.class, SegmentType.OFFLINE);
    _startTime = znRecord.getLongField(CommonConstants.Segment.START_TIME, -1);
    _endTime = znRecord.getLongField(CommonConstants.Segment.END_TIME, -1);
    if (znRecord.getSimpleFields().containsKey(CommonConstants.Segment.TIME_UNIT) &&
        !znRecord.getSimpleField(CommonConstants.Segment.TIME_UNIT).equals(NULL)) {
      _timeUnit = znRecord.getEnumField(CommonConstants.Segment.TIME_UNIT, TimeUnit.class, TimeUnit.DAYS);
    }
    _indexVersion = znRecord.getSimpleField(CommonConstants.Segment.INDEX_VERSION);
    _totalRawDocs = znRecord.getLongField(CommonConstants.Segment.TOTAL_DOCS, -1);
    _crc = znRecord.getLongField(CommonConstants.Segment.CRC, -1);
    _creationTime = znRecord.getLongField(CommonConstants.Segment.CREATION_TIME, -1);
    _sizeThresholdToFlushSegment = znRecord.getIntField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, -1);
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public long getStartTime() {
    return _startTime;
  }

  public void setStartTime(long startTime) {
    _startTime = startTime;
  }

  public long getEndTime() {
    return _endTime;
  }

  public void setEndTime(long endTime) {
    _endTime = endTime;
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    _timeUnit = timeUnit;
  }

  public String getIndexVersion() {
    return _indexVersion;
  }

  public void setIndexVersion(String indexVersion) {
    _indexVersion = indexVersion;
  }

  public SegmentType getSegmentType() {
    return _segmentType;
  }

  public void setSegmentType(SegmentType segmentType) {
    _segmentType = segmentType;
  }

  public long getTotalRawDocs() {
    return _totalRawDocs;
  }

  public void setTotalRawDocs(long totalRawDocs) {
    _totalRawDocs = totalRawDocs;
  }

  public long getCrc() {
    return _crc;
  }

  public void setCrc(long crc) {
    _crc = crc;
  }

  public long getCreationTime() {
    return _creationTime;
  }

  public void setCreationTime(long creationTime) {
    _creationTime = creationTime;
  }

  public void setSizeThresholdToFlushSegment(int sizeThresholdToFlushSegment) {
    _sizeThresholdToFlushSegment = sizeThresholdToFlushSegment;
  }

  public int getSizeThresholdToFlushSegment() {
    return _sizeThresholdToFlushSegment;
  }

  @Override
  public boolean equals(Object segmentMetadata) {
    if (isSameReference(this, segmentMetadata)) {
      return true;
    }

    if (isNullOrNotSameClass(this, segmentMetadata)) {
      return false;
    }

    SegmentZKMetadata metadata = (SegmentZKMetadata) segmentMetadata;
    return isEqual(_segmentName, metadata._segmentName) &&
        isEqual(_tableName, metadata._tableName) &&
        isEqual(_indexVersion, metadata._indexVersion) &&
        isEqual(_timeUnit, metadata._timeUnit) &&
        isEqual(_startTime, metadata._startTime) &&
        isEqual(_endTime, metadata._endTime) &&
        isEqual(_segmentType, metadata._segmentType) &&
        isEqual(_totalRawDocs, metadata._totalRawDocs) &&
        isEqual(_crc, metadata._crc) &&
        isEqual(_creationTime, metadata._creationTime) &&
        isEqual(_sizeThresholdToFlushSegment, metadata._sizeThresholdToFlushSegment);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(_segmentName);
    result = hashCodeOf(result, _tableName);
    result = hashCodeOf(result, _segmentType);
    result = hashCodeOf(result, _startTime);
    result = hashCodeOf(result, _endTime);
    result = hashCodeOf(result, _timeUnit);
    result = hashCodeOf(result, _indexVersion);
    result = hashCodeOf(result, _totalRawDocs);
    result = hashCodeOf(result, _crc);
    result = hashCodeOf(result, _creationTime);
    result = hashCodeOf(result, _sizeThresholdToFlushSegment);
    return result;
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_segmentName);
    znRecord.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, _segmentName);
    znRecord.setSimpleField(CommonConstants.Segment.TABLE_NAME, _tableName);
    znRecord.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, _segmentType);
    if (_timeUnit == null) {
      znRecord.setSimpleField(CommonConstants.Segment.TIME_UNIT, NULL);
    } else {
      znRecord.setEnumField(CommonConstants.Segment.TIME_UNIT, _timeUnit);
    }
    znRecord.setLongField(CommonConstants.Segment.START_TIME, _startTime);
    znRecord.setLongField(CommonConstants.Segment.END_TIME, _endTime);

    znRecord.setSimpleField(CommonConstants.Segment.INDEX_VERSION, _indexVersion);
    znRecord.setLongField(CommonConstants.Segment.TOTAL_DOCS, _totalRawDocs);
    znRecord.setLongField(CommonConstants.Segment.CRC, _crc);
    znRecord.setLongField(CommonConstants.Segment.CREATION_TIME, _creationTime);
    znRecord.setLongField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, _sizeThresholdToFlushSegment);
    return znRecord;
  }

  public Map<String, String> toMap() {
    Map<String, String> configMap = new HashMap<String, String>();
    configMap.put(CommonConstants.Segment.SEGMENT_NAME, _segmentName);
    configMap.put(CommonConstants.Segment.TABLE_NAME, _tableName);
    configMap.put(CommonConstants.Segment.SEGMENT_TYPE, _segmentType.toString());
    if (_timeUnit == null) {
      configMap.put(CommonConstants.Segment.TIME_UNIT, null);
    } else {
      configMap.put(CommonConstants.Segment.TIME_UNIT, _timeUnit.toString());
    }
    configMap.put(CommonConstants.Segment.START_TIME, Long.toString(_startTime));
    configMap.put(CommonConstants.Segment.END_TIME, Long.toString(_endTime));

    configMap.put(CommonConstants.Segment.INDEX_VERSION, _indexVersion);
    configMap.put(CommonConstants.Segment.TOTAL_DOCS, Long.toString(_totalRawDocs));
    configMap.put(CommonConstants.Segment.CRC, Long.toString(_crc));
    configMap.put(CommonConstants.Segment.CREATION_TIME, Long.toString(_creationTime));
    configMap.put(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, Integer.toString(_sizeThresholdToFlushSegment));
    return configMap;
  }
}
