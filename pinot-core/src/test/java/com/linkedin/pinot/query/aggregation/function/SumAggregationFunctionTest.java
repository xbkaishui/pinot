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
package com.linkedin.pinot.query.aggregation.function;

import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.function.SumAggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.aggregation.ResultHolderFactory;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import java.util.Map;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


/**
 * Unit test for the SumAggregationFunction class.
 * - Generates random data and aggregates using the MaxAggregationFunction class.
 * - Computes the max locally as well.
 * - Asserts that the result returned by the class matches the local result.
 */
@Test
public class SumAggregationFunctionTest {
  private static final int NUM_VALUES_TO_AGGREGATE = 10000;
  private static final int MAX_NUM_GROUP_KEYS = 100;
  private static final double DEFAULT_VALUE = 0.0;
  private Random _random;

  @BeforeSuite
  void init() {
    _random = new Random(System.currentTimeMillis());
  }

  /**
   * Tests the method that performs 'sum' aggregation.
   */
  @Test
  void testAggregate() {
    double[] valuesToAggregate = new double[NUM_VALUES_TO_AGGREGATE];
    double expected = DEFAULT_VALUE;

    for (int i = 0; i < NUM_VALUES_TO_AGGREGATE; i++) {
      valuesToAggregate[i] = _random.nextDouble();
      expected += valuesToAggregate[i];
    }

    SumAggregationFunction sumAggregationFunction = new SumAggregationFunction();
    AggregationResultHolder resultHolder = ResultHolderFactory.getAggregationResultHolder(sumAggregationFunction);

    sumAggregationFunction.aggregate(NUM_VALUES_TO_AGGREGATE, resultHolder, valuesToAggregate);
    double actual = resultHolder.getDoubleResult();

    Assert.assertEquals(actual, expected, "Sum Aggregation test failed Expected: " + expected + " Actual: " + actual);
  }

  /**
   * Tests the method that performs 'min' aggregation group-by.
   * - Input is randomly generated values that are assigned randomly generated group keys.
   * - Aggregation group-by performed using the 'SumAggregationFunction', and compared against
   *   aggregation group-by performed locally using a hash-map.
   */
  @Test
  void testAggregateGroupBySV() {
    double[] valuesToAggregate = new double[NUM_VALUES_TO_AGGREGATE];
    int[] groupKeysForValues = new int[NUM_VALUES_TO_AGGREGATE];

    Int2DoubleOpenHashMap expectedGroupByResultMap = new Int2DoubleOpenHashMap();
    expectedGroupByResultMap.defaultReturnValue(DEFAULT_VALUE);

    for (int i = 0; i < MAX_NUM_GROUP_KEYS; i++) {
      groupKeysForValues[i] = Math.abs(_random.nextInt()) % MAX_NUM_GROUP_KEYS;
    }

    for (int i = 0; i < NUM_VALUES_TO_AGGREGATE; i++) {
      valuesToAggregate[i] = _random.nextDouble();

      int key = groupKeysForValues[i];
      double oldValue = expectedGroupByResultMap.get(key);
      double newValue = oldValue + valuesToAggregate[i];
      expectedGroupByResultMap.put(key, newValue);
    }

    SumAggregationFunction sumAggregationFunction = new SumAggregationFunction();
    GroupByResultHolder resultHolder =
        ResultHolderFactory.getGroupByResultHolder(sumAggregationFunction, MAX_NUM_GROUP_KEYS, MAX_NUM_GROUP_KEYS);

    sumAggregationFunction.aggregateGroupBySV(NUM_VALUES_TO_AGGREGATE, groupKeysForValues, resultHolder,
        valuesToAggregate);

    for (Map.Entry<Integer, Double> entry : expectedGroupByResultMap.entrySet()) {
      int key = entry.getKey();
      double expected = entry.getValue();
      double actual = resultHolder.getDoubleResult(key);
      Assert.assertEquals(actual, expected, "Sum Aggregation test failed Expected: " + expectedGroupByResultMap + " Actual: " + actual);
    }
  }
}
