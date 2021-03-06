package com.linkedin.thirdeye.util;

import com.google.common.collect.HashMultimap;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.thirdeye.api.DimensionMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.MetricDataset;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;


public abstract class ThirdEyeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeUtils.class);
  private static final String FILTER_VALUE_ASSIGNMENT_SEPARATOR = "=";
  private static final String FILTER_CLAUSE_SEPARATOR = ";";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

  private ThirdEyeUtils () {

  }

  public static Multimap<String, String> getFilterSet(String filters) {
    Multimap<String, String> filterSet = ArrayListMultimap.create();
    if (StringUtils.isNotBlank(filters)) {
      String[] filterClauses = filters.split(FILTER_CLAUSE_SEPARATOR);
      for (String filterClause : filterClauses) {
        String[] values = filterClause.split(FILTER_VALUE_ASSIGNMENT_SEPARATOR, 2);
        if (values.length != 2) {
          throw new IllegalArgumentException("Filter values assigments should in pairs: " + filters);
        }
        filterSet.put(values[0], values[1]);
      }
    }
    return filterSet;
  }

  /**
   * Returns or modifies a filter that can be for querying the results corresponding to the given dimension map.
   *
   * For example, if a dimension map = {country=IN,page_name=front_page}, then the two entries will be added or
   * over-written to the given filter.
   *
   * Note that if the given filter contains an entry: country=["IN", "US", "TW",...], then this entry is replaced by
   * country=IN.
   *
   * @param dimensionMap the dimension map to add to the filter
   * @param filterToDecorate if it is null, a new filter will be created; otherwise, it is modified.
   * @return a filter that is modified according to the given dimension map.
   */
  public static Multimap<String, String> getFilterSetFromDimensionMap(DimensionMap dimensionMap,
      Multimap<String, String> filterToDecorate) {
    if (filterToDecorate == null) {
      filterToDecorate = HashMultimap.create();
    }

    for (Map.Entry<String, String> entry : dimensionMap.entrySet()) {
      String dimensionName = entry.getKey();
      String dimensionValue = entry.getValue();
      // If dimension value is "OTHER", then we need to get all data and calculate "OTHER" part.
      // In order to reproduce the data for "OTHER", the filter should remain as is.
      if ( !dimensionValue.equalsIgnoreCase("OTHER") ) {
        // Only add the specific dimension value to the filter because other dimension values will not be used
        filterToDecorate.removeAll(dimensionName);
        filterToDecorate.put(dimensionName, dimensionValue);
      }
    }

    return filterToDecorate;
  }

  public static String convertMultiMapToJson(Multimap<String, String> multimap)
      throws JsonProcessingException {
    Map<String, Collection<String>> map = multimap.asMap();
    return OBJECT_MAPPER.writeValueAsString(map);
  }

  public static Multimap<String, String> convertToMultiMap(String json) {
    ArrayListMultimap<String, String> multimap = ArrayListMultimap.create();
    if (json == null) {
      return multimap;
    }
    try {
      TypeReference<Map<String, ArrayList<String>>> valueTypeRef =
          new TypeReference<Map<String, ArrayList<String>>>() {
          };
      Map<String, ArrayList<String>> map;

      map = OBJECT_MAPPER.readValue(json, valueTypeRef);
      for (Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
        ArrayList<String> valueList = entry.getValue();
        ArrayList<String> trimmedList = new ArrayList<>();
        for (String value : valueList) {
          trimmedList.add(value.trim());
        }
        multimap.putAll(entry.getKey(), trimmedList);
      }
      return multimap;
    } catch (IOException e) {
      LOG.error("Error parsing json:{} message:{}", json, e.getMessage());
    }
    return multimap;
  }

  public static String getSortedFiltersFromMultiMap(Multimap<String, String> filterMultiMap) {
    Set<String> filterKeySet = filterMultiMap.keySet();
    ArrayList<String> filterKeyList = new ArrayList<String>(filterKeySet);
    Collections.sort(filterKeyList);

    StringBuilder sb = new StringBuilder();
    for (String filterKey : filterKeyList) {
      ArrayList<String> values = new ArrayList<String>(filterMultiMap.get(filterKey));
      Collections.sort(values);
      for (String value : values) {
        sb.append(filterKey);
        sb.append(FILTER_VALUE_ASSIGNMENT_SEPARATOR);
        sb.append(value);
        sb.append(FILTER_CLAUSE_SEPARATOR);
      }
    }
    return StringUtils.chop(sb.toString());
  }

  public static String getSortedFilters(String filters) {
    Multimap<String, String> filterMultiMap = getFilterSet(filters);
    String sortedFilters = getSortedFiltersFromMultiMap(filterMultiMap);

    if (StringUtils.isBlank(sortedFilters)) {
      return null;
    }
    return sortedFilters;
  }

  public static String getSortedFiltersFromJson(String filterJson) {
    Multimap<String, String> filterMultiMap = convertToMultiMap(filterJson);
    String sortedFilters = getSortedFiltersFromMultiMap(filterMultiMap);

    if (StringUtils.isBlank(sortedFilters)) {
      return null;
    }
    return sortedFilters;
  }

  public static TimeSpec getTimeSpecFromDatasetConfig(DatasetConfigDTO datasetConfig) {
    String timeFormat = datasetConfig.getTimeFormat();
    if (timeFormat.startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())) {
      timeFormat = getSDFPatternFromTimeFormat(timeFormat);
    }
    TimeSpec timespec = new TimeSpec(datasetConfig.getTimeColumn(),
        new TimeGranularity(datasetConfig.getTimeDuration(), datasetConfig.getTimeUnit()), timeFormat);
    return timespec;
  }

  private static String getSDFPatternFromTimeFormat(String timeFormat) {
    String pattern = timeFormat;
    String[] tokens = timeFormat.split(":");
    if (tokens.length == 2) {
      pattern = tokens[1];
    }
    return pattern;
  }

  public static MetricExpression getMetricExpressionFromMetricConfig(MetricConfigDTO metricConfig) {
    MetricExpression metricExpression = new MetricExpression();
    metricExpression.setExpressionName(metricConfig.getName());
    if (metricConfig.isDerived()) {
      metricExpression.setExpression(metricConfig.getDerivedMetricExpression());
    } else {
      metricExpression.setExpression(MetricConfigBean.DERIVED_METRIC_ID_PREFIX + metricConfig.getId());
    }
    return metricExpression;
  }

  // TODO: Write parser instead of looking for occurrence of every metric
  public static String substituteMetricIdsForMetrics(String metricExpression, String dataset) {
    MetricConfigManager metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    for (MetricConfigDTO metricConfig : metricConfigs) {
      if (metricConfig.isDerived()) {
        continue;
      }
      String metricName = metricConfig.getName();
      metricExpression = metricExpression.replaceAll(metricName,
          MetricConfigBean.DERIVED_METRIC_ID_PREFIX + metricConfig.getId());
    }
    return metricExpression;
  }

  public static String getDerivedMetricExpression(String metricExpressionName, String dataset) throws ExecutionException {
    String derivedMetricExpression = null;
    MetricDataset metricDataset = new MetricDataset(metricExpressionName, dataset);
    MetricConfigDTO metricConfig = CACHE_REGISTRY.getMetricConfigCache().get(metricDataset);
    if (metricConfig.isDerived()) {
      derivedMetricExpression = metricConfig.getDerivedMetricExpression();
    } else {
      derivedMetricExpression = MetricConfigBean.DERIVED_METRIC_ID_PREFIX + metricConfig.getId();
    }
    return derivedMetricExpression;
  }

  public static Map<String, Double> getMetricThresholdsMap(List<MetricFunction> metricFunctions) {
    Map<String, Double> metricThresholds = new HashMap<>();
    for (MetricFunction metricFunction : metricFunctions) {
      String derivedMetricExpression = metricFunction.getMetricName();
      String metricId = derivedMetricExpression.replaceAll(MetricConfigBean.DERIVED_METRIC_ID_PREFIX, "");
      MetricConfigDTO metricConfig = DAO_REGISTRY.getMetricConfigDAO().findById(Long.valueOf(metricId));
      metricThresholds.put(derivedMetricExpression, metricConfig.getRollupThreshold());
    }
    return metricThresholds;
  }

  public static String getMetricNameFromFunction(MetricFunction metricFunction) {
    String metricId = metricFunction.getMetricName().replace(MetricConfigBean.DERIVED_METRIC_ID_PREFIX, "");
    MetricConfigDTO metricConfig = DAO_REGISTRY.getMetricConfigDAO().findById(Long.valueOf(metricId));
    return metricConfig.getName();
  }

  public static Schema createSchema(CollectionSchema collectionSchema) {
    Schema schema = new Schema();

    for (DimensionSpec dimensionSpec : collectionSchema.getDimensions()) {
      FieldSpec fieldSpec = new DimensionFieldSpec();
      String dimensionName = dimensionSpec.getName();
      fieldSpec.setName(dimensionName);
      fieldSpec.setDataType(DataType.STRING);
      fieldSpec.setSingleValueField(true);
      schema.addField(dimensionName, fieldSpec);
    }
    for (MetricSpec metricSpec : collectionSchema.getMetrics()) {
      FieldSpec fieldSpec = new MetricFieldSpec();
      String metricName = metricSpec.getName();
      fieldSpec.setName(metricName);
      fieldSpec.setDataType(DataType.valueOf(metricSpec.getType().toString()));
      fieldSpec.setSingleValueField(true);
      schema.addField(metricName, fieldSpec);
    }
    TimeSpec timeSpec = collectionSchema.getTime();
    String timeFormat = timeSpec.getFormat().equals("sinceEpoch") ? TimeFormat.EPOCH.toString()
        : TimeFormat.SIMPLE_DATE_FORMAT.toString() + ":" + timeSpec.getFormat();
    TimeGranularitySpec incoming =
        new TimeGranularitySpec(DataType.LONG,
            timeSpec.getDataGranularity().getSize(),
            timeSpec.getDataGranularity().getUnit(),
            timeFormat,
            timeSpec.getColumnName());
    TimeGranularitySpec outgoing =
        new TimeGranularitySpec(DataType.LONG,
            timeSpec.getDataGranularity().getSize(),
            timeSpec.getDataGranularity().getUnit(),
            timeFormat,
            timeSpec.getColumnName());

    schema.addField(timeSpec.getColumnName(), new TimeFieldSpec(incoming, outgoing));

    schema.setSchemaName(collectionSchema.getCollection());

    return schema;
  }

  public static String constructMetricAlias(String datasetName, String metricName) {
    String alias = datasetName + MetricConfigBean.ALIAS_JOINER + metricName;
    return alias;
  }

  public static String getDefaultDashboardName(String dataset) {
    String dashboardName = DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + dataset;
    return dashboardName;
  }

}
