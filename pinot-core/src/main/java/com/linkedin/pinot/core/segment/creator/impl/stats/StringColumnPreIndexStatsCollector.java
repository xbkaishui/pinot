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
package com.linkedin.pinot.core.segment.creator.impl.stats;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.Arrays;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


/**
 * Nov 7, 2014
 */

public class StringColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {

  private String min = V1Constants.Str.NULL_STRING;
  private String max = V1Constants.Str.NULL_STRING;
  private int longestStringLength = 0;
  private final ObjectSet<String> rawStringSet;
  private final ObjectSet<String> aggregatedStringSet;
  private String[] sortedStringList;
  private boolean hasNull = false;
  private boolean sealed = false;

  public StringColumnPreIndexStatsCollector(FieldSpec spec) {
    super(spec);
    rawStringSet = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
    aggregatedStringSet = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
  }

  /**
   * Collect statistics for the given entry.
   * - Add it to the passed in set (which could be raw or aggregated)
   * - Update maximum number of values for Multi-valued entries
   * - Update Total number of entries
   * - Check if entry is sorted.
   * @param entry
   * @param set
   */
  private void collectEntry(Object entry, ObjectSet<String> set) {

    if (entry instanceof Object[]) {
      for (final Object e : (Object[]) entry) {
        String value = e.toString();
        set.add(value);
        longestStringLength = Math.max(longestStringLength, value.getBytes().length);
      }
      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
    } else {

      String value;
      if (entry != null) {
        value = entry.toString();
      } else {
        value = fieldSpec.getDefaultNullValue().toString();
      }
      addressSorted(value);
      set.add(value);
      longestStringLength = Math.max(longestStringLength, value.getBytes().length);
      totalNumberOfEntries++;
    }
  }

  /**
   * {@inheritDoc}
   * @param entry Entry to be collected
   * @param isAggregated True for aggregated, False for raw.
   */
  @Override
  public void collect(Object entry, boolean isAggregated) {
    if (isAggregated) {
      collectEntry(entry, aggregatedStringSet);
    } else {
      collectEntry(entry, rawStringSet);
    }
  }

  /**
   * {@inheritDoc}
   * @param entry Entry to be collected
   */
  @Override
  public void collect(Object entry) {
    collect(entry, false /* isAggregated */);
  }


  @Override
  public String getMinValue() throws Exception {
    if (sealed) {
      return min;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public String getMaxValue() throws Exception {
    if (sealed) {
      return max;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for max value");
  }

  @Override
  public Object[] getUniqueValuesSet() throws Exception {
    if (sealed) {
      return sortedStringList;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for unique values set");
  }

  @Override
  public int getLengthOfLargestElement() throws Exception {
    if (sealed) {
      return longestStringLength;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for longest value");
  }

  @Override
  public int getCardinality() throws Exception {
    if (sealed) {
      return sortedStringList.length;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for cardinality");
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public void seal() {
    sealed = true;
    sortedStringList = new String[rawStringSet.size()];
    rawStringSet.toArray(sortedStringList);

    Arrays.sort(sortedStringList);

    if (sortedStringList.length == 0) {
      min = null;
      max = null;
      return;
    }

    // Update min/max based on raw docs.
    min = sortedStringList[0];
    max = sortedStringList[sortedStringList.length - 1];

    // Merge the raw and aggregated docs, so stats for dictionary creation are collected correctly.
    int numAggregated = aggregatedStringSet.size();
    if (numAggregated > 0) {
      rawStringSet.addAll(aggregatedStringSet);
      sortedStringList = new String[rawStringSet.size()];
      rawStringSet.toArray(sortedStringList);
      Arrays.sort(sortedStringList);
    }
  }
}
