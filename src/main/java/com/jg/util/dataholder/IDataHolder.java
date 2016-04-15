package com.jg.util.dataholder;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;


public interface IDataHolder {

	public boolean recursiveMerge(IDataHolder toBeMerged);

	public List<String> getStringValues(String namespace, String... breadcrumb);

	public String getStringValue(String namespace, String... breadcrumb);

	public Long getLongValue(String namespace, String... breadcrumb);

	public Integer getIntegerValue(String namespace, String... breadcrumb);

	public Double getDoubleValue(String namespace, String... breadcrumb);

	public Date getDateValue(String namespace, String... breadcrumb);

	public Boolean getBooleanValue(String namespace, String... breadcrumb);

	public Double getDoubleValue(String namespace, String[] breadcrumb, Map<String, String> cutByCondition,
			String field);

	public String getStringValue(String namespace, String[] breadcrumb, Map<String, String> cutByCondition,
			String field);

	public Long getLongValue(String namespace, String[] breadcrumb, Map<String, String> cutByCondition, String field);

	public String getJsonValue(String namespace, String[] breadcrumb, Map<String, String> cutByCondition);

	public String getStringValue(String namespace, String[] breadcrumb, String field, int arrayIndex);

	public Date getSimpleDateValue(String namespace, String key);

	public String getSimpleStringValue(String namespace, String key);

	public Long getSimpleLongValue(String namespace, String key);

	public Integer getSimpleIntegerValue(String namespace, String key);
	// public List<Long> getSimpleLongValues( String namespace , String key);
	// public List<String> getSimpleStringValues(String namespace , String key);
	// public List<Integer> getSimpleIntegerValues(String namespace , String
	// key);
	// public List<Double> getSimpleDoubleValues(String namespace , String key);
	// public List<Date> getSimpleDateValues(String namespace , String key);

	public List<Long> getLongValues(String namespace, String... breadcrumb);

	public List<Integer> getIntegerValues(String namespace, String[] breadcrumb);

	public List<Double> getDoubleValues(String namespace, String[] breadcrumb);

	public List<Date> getDateValues(String namespace, String[] breadcrumb);

	public List<Boolean> getBooleanValues(String namespace, String[] breadcrumb);

	public List<String> getChildKeys(String namespace, String... breadcrumb);


	public boolean addValue(String namespace, String[] breadcrumb, String value);

	public boolean addValue(String namespace, String[] breadcrumb, Long value);

	public boolean addValue(String namespace, String[] breadcrumb, Integer value);

	public boolean addValue(String namespace, String[] breadcrumb, Double value);

	public boolean addValue(String namespace, String[] breadcrumb, BigDecimal value);

	public boolean addValue(String namespace, String[] breadcrumb, Date value);

	public boolean addValue(String namespace, String[] breadcrumb, Boolean value);

	// add operations.
	public boolean addValues(String namespace, String[] breadcrumb, String... values);

	public boolean addValues(String namespace, String[] breadcrumb, Long... values);

	public boolean addValues(String namespace, String[] breadcrumb, Integer... values);

	public boolean addValues(String namespace, String[] breadcrumb, Double... values);

	public boolean addValues(String namespace, String[] breadcrumb, BigDecimal... values);

	public boolean addValues(String namespace, String[] breadcrumb, Date... values);
	// public boolean addValues( String namespace, String[] breadcrumb,
	// Boolean... values) ;

	// add operations.
	public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, String... values);

	public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Long... values);

	public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Integer... values);

	public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Double... values);

	public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, BigDecimal... values);

	public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Date... values);

	public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Boolean... values);

	// public boolean addJsonNode( String namespace, String[] breadcrumb,
	// JsonNode value);
	public boolean addJsonString(String namespace, String[] breadcrumb, String value);

	/**
	 * Creates a json object from the json string and adds to the given path.
	 * 
	 * @param namespace
	 * @param breadcrumb
	 * @param doMerge:
	 *            if true, appends to the existing path else overwrite the leaf
	 *            node.
	 * @param value
	 * @return
	 */
	public boolean addJsonString(String namespace, String[] breadcrumb, boolean doMerge, String value);

	// public boolean addArray( String namespace, String[] breadcrumb, ArrayNode
	// array) ;
	public boolean isEmpty(String namespace, String[] breadcrumb);

	// contains operation.
	public boolean contains(String namespace, String[] breadcrumb);

	public boolean containsSimpleData(String namespace, String key);

	public boolean mergeOrCreate(IDataHolder toBeMerged);

	public boolean mergeOrCreate(IDataHolder toBeMerged, boolean collapseNameSpaceAndMerge);

	public boolean mergeOrCreate(String jsonData, String sourceNamespace, String destinationNameSpace);

	public boolean mergeOrCreate(String jsonData, boolean collapseNameSpaceAndMerge);

	public String getJSONData();

	public String getJSONData(String namespace, String[] breadcrumb);

	public boolean addDataHolder(String namespace, String[] completePath, IDataHolder dataHolder);

	public boolean deleteNode(String namespace, String[] breadcrumb);

	public boolean isPathPresent(String namespace, String[] breadcrumb);

	public int getArrayLength(String namespace, String[] breadcrumb);

	// Get or add values without knowing datatype of value(s)
	public Object getValue(String namespace, String[] breadcrumb);

	public List<Object> getObjectValues(String namespace, String[] breadcrumb);

	public boolean addObjectValue(String namespace, String[] breadcrumb, Object value);

	public boolean addObjectValues(String namespace, String[] breadcrumb, Object... values);

	public boolean addObjectValues(String namespace, String[] breadcrumb, boolean doMerge, Object... values);

	public boolean isEmpty();
	
	public Object getJSONRootNode();
	
	public DataTypeEnum getType(String namespace, String[] breadcrumb);
}
