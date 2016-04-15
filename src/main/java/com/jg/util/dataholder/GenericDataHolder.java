package com.jg.util.dataholder;


import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.MissingNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * This needs to have rich set of data and apis for carrying data
 * 
 * also it checks for existence of keys in attribute names when we put data in
 * breadcrum : a--B--C it will check if C is there in attribute name space
 * 
 * @author gkukal
 * 
 */

public class GenericDataHolder implements IDataHolder {

    private static Logger s_logger = LoggerFactory.getLogger(GenericDataHolder.class);


    protected ObjectMapper m_mapper = null;
    protected ObjectNode m_ObjectNode = null;
    public static String m_default_Name_space = "DNS";

    public GenericDataHolder() {
        m_mapper = new ObjectMapper();
        m_ObjectNode = (ObjectNode) m_mapper.createObjectNode();
    }

    public GenericDataHolder(String result) {
        m_mapper = new ObjectMapper();
        try {
            m_ObjectNode = (ObjectNode) m_mapper.readTree(result);
        } catch (JsonProcessingException e) {
            String message = "Error creationg generic data Holder from json string: " + result;
            s_logger.error(message, e);
        } catch (IOException e) {
            String message = "Error creationg generic data Holder from json string: " + result;
            s_logger.error(message, e);
        }

        if (m_ObjectNode == null) {
            m_ObjectNode = (ObjectNode) m_mapper.createObjectNode();
        }
    }

    public GenericDataHolder(String nameSpace, String result) {
        if (nameSpace == null)
            nameSpace = m_default_Name_space;
        try {
            m_mapper = new ObjectMapper();
            m_ObjectNode = (ObjectNode) m_mapper.createObjectNode();

            ObjectNode m_ObjectNode2 = (ObjectNode) m_mapper.readTree(result);

            m_ObjectNode.put(nameSpace, m_ObjectNode2);
        } catch (JsonProcessingException e) {
            String message = "Error creationg generic data Holder from json string: namespace: " + nameSpace + " json string" + result;
            s_logger.error(message, e);
        } catch (IOException e) {
            String message = "Error creationg generic data Holder from json string: namespace: " + nameSpace + " json string" + result;
            s_logger.error(message, e);
        }

        if (m_ObjectNode == null) {
            m_ObjectNode = (ObjectNode) m_mapper.createObjectNode();
        }
    }

    public GenericDataHolder(byte[] jsonInputStream) {
        m_mapper = new ObjectMapper();
        try {
            m_ObjectNode = (ObjectNode) m_mapper.readTree(jsonInputStream);

        } catch (JsonProcessingException e) {
            String message = "Error creationg generic data Holder from jsonInputStream";
            s_logger.error(message, e);
        } catch (IOException e) {
            String message = "Error creationg generic data Holder from jsonInputStream";
            s_logger.error(message, e);
        } // will be of type ObjectNode
        if (m_ObjectNode == null) {
            m_ObjectNode = (ObjectNode) m_mapper.createObjectNode();
        }
    }

    public Date getSimpleDateValue(String namespace, String key) {
        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode lastNode = root.get(key);
        return stringToDate(lastNode.asText());
    }

    public String getSimpleStringValue(String namespace, String key) {
        JsonNode root = m_ObjectNode.path(namespace);
        if(key == null) {
        	return root.asText();
        }
        JsonNode lastNode = root.get(key);
        return lastNode.asText();
    }

    public Long getSimpleLongValue(String namespace, String key) {
        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode lastNode = root.get(key);
        return lastNode.asLong();
    }

    public Integer getSimpleIntegerValue(String namespace, String key) {
        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode lastNode = root.get(key);
        return lastNode.asInt();
    }

    public List<String> getAnObjectAsStringFromArray(String namespace, String[] breadcrumbToTheArrayElement, String objectKey) {
        if (namespace == null)
            namespace = m_default_Name_space;
        List<String> returnList = new ArrayList<String>();
        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumbToTheArrayElement);
        String lastNode = basicReadlastNode(root, breadcrumbToTheArrayElement);
        JsonNode arrayElement = currentNode.path(lastNode);

        if (arrayElement.isArray()) {
            ArrayNode nodeArr = (ArrayNode) arrayElement;
            int size = nodeArr.size();
            for (int i = 0; i < size; i++) {
                JsonNode jsonNode = nodeArr.get(i);
                JsonNode value = jsonNode.findValue(objectKey);
                if (value != null) {
                    returnList.add(value.toString());
                }
            }
        }
        return returnList;
    }

    public List<IDataHolder> getObjectsAsDataHolderFromArray(String namespace, String[] breadcrumbToTheArrayElement) {
        if (namespace == null)
            namespace = m_default_Name_space;
        List<IDataHolder> returnList = new ArrayList<IDataHolder>();
        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumbToTheArrayElement);
        String lastNode = basicReadlastNode(root, breadcrumbToTheArrayElement);
        JsonNode arrayElement = currentNode.path(lastNode);

        if (arrayElement.isArray()) {
            ArrayNode nodeArr = (ArrayNode) arrayElement;
            int size = nodeArr.size();
            for (int i = 0; i < size; i++) {
                JsonNode jsonNode = nodeArr.get(i);
                returnList.add(new GenericDataHolder(null, jsonNode.toString()));
            }
        }
        return returnList;
    }

    public String getObjectsAsStringValue(String namespace, String[] breadcrumbToTheArrayElement) {
        if (namespace == null)
            namespace = m_default_Name_space;
        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumbToTheArrayElement);
        String lastNode = basicReadlastNode(root, breadcrumbToTheArrayElement);
        JsonNode obj = currentNode.path(lastNode);
        return obj.toString();
    }

    
    public List<String> getStringValues(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        List<String> listOfValues = new ArrayList<String>();
        for (JsonNode node : currentNode.path(lastNode)) {
            listOfValues.add(node.getTextValue());
        }
        return listOfValues;
    }

    
    public String getStringValue(String namespace, String...breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        String asText = currentNode.path(lastNode).asText();
        if (asText.isEmpty())
            return null;

        return asText;
    }

    public List<String> getChildKeys(String namespace, String ... breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode()) {
            return ListUtils.EMPTY_LIST;
        }

        JsonNode currentNode;
        String val;
        JsonNode node;
        if (breadcrumb == null || breadcrumb.length == 0) {
            node = root;
        } else {
            currentNode = basicReadPenultimate(root, breadcrumb);
            val = breadcrumb[breadcrumb.length - 1];
            node = currentNode.get(val);
        }
        List<String> keys = new ArrayList<String>();
        if (node != null) {
            Iterator<Entry<String, JsonNode>> keyValues = node.getFields();
            while (keyValues.hasNext()) {
                keys.add(keyValues.next().getKey());
            }
            return keys;
        } else {
            return keys;
        }

    }

    /**
     * Traverse the tree with the given namespace and breadcrumb and returns
     * second last node.
     * 
     * @param currentNode
     * @param breadcrumb
     * @return
     */
    private JsonNode basicReadPenultimate(JsonNode currentNode, String[] breadcrumb) {
        JsonNode currentNodeCur = currentNode;
        if (breadcrumb != null && breadcrumb.length > 1) {
            for (int i = 0; i < (breadcrumb.length - 1); i++) {
                String s = breadcrumb[i];
                if (s != null) {
                    currentNodeCur = currentNodeCur.path(s);
                }
            }
        }
        return currentNodeCur;
    }

    private String basicReadlastNode(JsonNode currentNode, String[] breadcrumb) {
        String lastNode = null;
        JsonNode currentNodeCur = currentNode;
        if (breadcrumb == null || breadcrumb.length == 0)
            return null;
        if (breadcrumb != null && breadcrumb.length > 1) {
            for (int i = 0; i < (breadcrumb.length - 1); i++) {
                String s = breadcrumb[i];
                if (s != null) {
                    currentNodeCur = currentNodeCur.path(s);
                }
            }
            lastNode = breadcrumb[breadcrumb.length - 1];
        }
        if (breadcrumb.length == 1) {
            lastNode = breadcrumb[0];
        }
        return lastNode;

    }

    
    public List<Long> getLongValues(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);

        List<Long> listOfValues = new ArrayList<Long>();
        for (JsonNode node : currentNode.path(lastNode)) {
            listOfValues.add(node.asLong());
        }
        return listOfValues;
    }

    
    public Long getLongValue(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode()) {
            // throw new Exception NodeNotFound;
            return null;
        }

        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        if (currentNode.isMissingNode())
            return null;
        String lastNode = basicReadlastNode(root, breadcrumb);
        JsonNode lastJNode = currentNode.path(lastNode);

        if (!lastJNode.isMissingNode()) {
            return lastJNode.asLong();
        }

        return null;
    }

    
    public Double getDoubleValue(String namespace, String[] breadcrumb, Map<String, String> cutByCondition, String field) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        JsonNode lastJNode = currentNode.path(lastNode);

        if (!lastJNode.isMissingNode()) {
            if (lastJNode.isArray()) {
                Iterator<JsonNode> iter = ((ArrayNode) lastJNode).iterator();
                while (iter.hasNext()) {
                    JsonNode node = iter.next();
                    if (checkCondition(cutByCondition, node)) {
                        JsonNode tnode = node.path(field);
                        if (!tnode.isMissingNode())
                            return tnode.asDouble();
                    }
                }
            } else {
                if (checkCondition(cutByCondition, lastJNode)) {
                    JsonNode tnode = lastJNode.path(field);
                    if (!tnode.isMissingNode())
                        return tnode.asDouble();
                }
            }
        }

        return null;
    }

    
    public Long getLongValue(String namespace, String[] breadcrumb, Map<String, String> cutByCondition, String field) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        JsonNode lastJNode = currentNode.path(lastNode);

        if (!lastJNode.isMissingNode()) {
            if (lastJNode.isArray()) {
                Iterator<JsonNode> iter = ((ArrayNode) lastJNode).iterator();
                while (iter.hasNext()) {
                    JsonNode node = iter.next();
                    if (checkCondition(cutByCondition, node)) {
                        JsonNode tnode = node.path(field);
                        if (!tnode.isMissingNode())
                            return tnode.asLong();
                    }
                }
            } else {
                if (checkCondition(cutByCondition, lastJNode)) {
                    JsonNode tnode = lastJNode.path(field);
                    if (!tnode.isMissingNode())
                        return tnode.asLong();
                }
            }
        }

        return null;
    }

    
    public String getStringValue(String namespace, String[] breadcrumb, Map<String, String> cutByCondition, String field) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        JsonNode lastJNode = currentNode.path(lastNode);

        if (!lastJNode.isMissingNode()) {
            if (lastJNode.isArray()) {
                Iterator<JsonNode> iter = ((ArrayNode) lastJNode).iterator();
                while (iter.hasNext()) {
                    JsonNode node = iter.next();
                    if (checkCondition(cutByCondition, node)) {
                        JsonNode tnode = node.path(field);
                        if (!tnode.isMissingNode())
                            return tnode.asText();
                    }
                }
            } else {
                if (checkCondition(cutByCondition, lastJNode)) {
                    JsonNode tnode = lastJNode.path(field);
                    if (!tnode.isMissingNode())
                        return tnode.asText();
                }
            }
        }

        return null;
    }

    
    public String getStringValue(String namespace, String[] breadcrumb, String field, int arrayIndex) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        JsonNode lastJNode = currentNode.path(lastNode);

        if (!lastJNode.isMissingNode()) {
            if (lastJNode.isArray()) {
                Iterator<JsonNode> iter = ((ArrayNode) lastJNode).iterator();
                int index = 0;
                while (iter.hasNext()) {
                    JsonNode node = iter.next();

                    if (index == arrayIndex) {
                        JsonNode tnode = node.path(field);
                        if (!tnode.isMissingNode()) {
                            return tnode.asText();
                        }
                    }
                }
            }
        }
        return null;
    }

    
    public String getJsonValue(String namespace, String[] breadcrumb, Map<String, String> cutByCondition) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        JsonNode lastJNode = null;

        if (breadcrumb.length == 0) {
            lastJNode = currentNode;
        } else {
            String lastNode = basicReadlastNode(root, breadcrumb);
            lastJNode = currentNode.path(lastNode);
        }

        if (!lastJNode.isMissingNode()) {
            if (lastJNode.isArray()) {
                Iterator<JsonNode> iter = ((ArrayNode) lastJNode).iterator();
                while (iter.hasNext()) {
                    JsonNode node = iter.next();
                    if (checkCondition(cutByCondition, node)) {
                        return node.toString();
                    }
                }
            } else {
                if (checkCondition(cutByCondition, lastJNode)) {
                    return lastJNode.toString();
                }
            }
        }

        return null;
    }

    private boolean checkCondition(Map<String, String> cutByCondition, JsonNode node) {
        for (Entry<String, String> entry : cutByCondition.entrySet()) {
            String fieldName = entry.getKey();
            String fieldValue = entry.getValue();

            JsonNode currNode = node.path(fieldName);
            if (!currNode.isMissingNode()) {
                String value = currNode.asText();
                if (value.equalsIgnoreCase(fieldValue))
                    continue;
                else
                    return false;
            }
        }

        return true;
    }

    public List<Integer> getIntegerValues(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return ListUtils.EMPTY_LIST;

        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);

        if (currentNode.isMissingNode())
            return ListUtils.EMPTY_LIST;

        String lastNode = basicReadlastNode(root, breadcrumb);
        List<Integer> listOfValues = new ArrayList<Integer>();
        for (JsonNode node : currentNode.path(lastNode)) {
            listOfValues.add(node.asInt());
        }
        return listOfValues;
    }

    public Integer getIntegerValue(String namespace, String[] breadcrumb) {

        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return null;

        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        if (currentNode.isMissingNode())
            return null;

        String lastNode = basicReadlastNode(root, breadcrumb);
        int value = currentNode.path(lastNode).asInt();
        return value;
    }

    
    public List<Double> getDoubleValues(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return ListUtils.EMPTY_LIST;

        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        List<Double> listOfValues = new ArrayList<Double>();
        for (JsonNode node : currentNode.path(lastNode)) {
            listOfValues.add(node.asDouble());
        }
        return listOfValues;
    }

    
    public Double getDoubleValue(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return null;

        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        JsonNode lastJNode = currentNode.path(lastNode);
        if (lastJNode.isMissingNode() || lastJNode.isArray()) {
            return null;
        }
        return lastJNode.asDouble();
    }

    
    public Boolean getBooleanValue(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return false;

        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        return currentNode.path(lastNode).asBoolean();
    }

    
    public List<Boolean> getBooleanValues(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return ListUtils.EMPTY_LIST;

        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        List<Boolean> listOfValues = new ArrayList<Boolean>();
        for (JsonNode node : currentNode.path(lastNode)) {
            listOfValues.add(node.asBoolean());
        }
        return listOfValues;
    }

    
    public boolean deleteNode(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return false;

        JsonNode currentNode1 = basicReadPenultimate(root, breadcrumb);
        if (!currentNode1.isMissingNode()) {
            ObjectNode currentNode = (ObjectNode) currentNode1;
            String lastNode = basicReadlastNode(root, breadcrumb);
            if (currentNode != null && lastNode != null) {
                currentNode.remove(lastNode);
            }
            return true;

        } else {
            return false;
        }
    }

    
    public int getArrayLength(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return 0;

        ObjectNode currentNode = (ObjectNode) basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);

        JsonNode lastJNode = currentNode.get(lastNode);

        if (lastJNode == null) {
            return 0;
        } else if (lastJNode.isArray()) {
            return ((ArrayNode) lastJNode).size();
        } else {
            return 1;
        }
    }

    
    public boolean isPathPresent(String namespace, String[] breadcrumb) {
        if (namespace == null)
            return false;

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return false;

        JsonNode currentNodeCur = root;
        if (breadcrumb != null && breadcrumb.length > 0) {
            for (int i = 0; i < breadcrumb.length; i++) {
                String s = breadcrumb[i];
                currentNodeCur = currentNodeCur.path(s);
                if (currentNodeCur.isMissingNode())
                    return false;
            }
        } else {
            return false;
        }

        return true;
    }

    public DataTypeEnum getType(String namespace, String[] breadcrumb) {
        if (namespace == null)
           namespace = "DNS";

        JsonNode root = m_ObjectNode.path(namespace);
        if (root.isMissingNode())
            return null;

        JsonNode currentNodeCur = root;
        if (breadcrumb != null && breadcrumb.length > 0) {
            for (int i = 0; i < breadcrumb.length; i++) {
                String s = breadcrumb[i];
                currentNodeCur = currentNodeCur.path(s);
                if (currentNodeCur.isMissingNode()){
                    return null;
                }else if ( currentNodeCur instanceof LongNode){
                	return DataTypeEnum.NUMERIC_LONG;
                }else if ( currentNodeCur instanceof NumericNode){
                	return DataTypeEnum.NUMERIC_INTEGER;
                }else if ( currentNodeCur instanceof NumericNode){
                	return DataTypeEnum.NUMERIC_INTEGER;
                }else if ( currentNodeCur instanceof TextNode){
                	return DataTypeEnum.STRING;
                }
            }
        } else {
            return  null;
        }

        return null;
    }
    
    public boolean isEmpty(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);

        if (root.isMissingNode())
            return true;

        ObjectNode currentNode = (ObjectNode) basicReadPenultimate(root, breadcrumb);

        if (currentNode == null)
            return true;

        String lastNode = basicReadlastNode(root, breadcrumb);
        if (lastNode == null)
            return true;

        JsonNode lastJNode = currentNode.get(lastNode);
        if (lastJNode == null)
            return true;

        Iterator<String> iter = lastJNode.getFieldNames();

        if (!iter.hasNext())
            return true;

        return false;
    }

    
    public List<Date> getDateValues(String namespace, String[] breadcrumb) {
        // TODO Auto-generated method stub
        return null;
    }

    
    public Date getDateValue(String namespace, String[] breadcrumb) {
        // TODO Auto-generated method stub
        return null;
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, String... values) {

        return addValues(namespace, breadcrumb, false, values);
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, Long... values) {

        return addValues(namespace, breadcrumb, false, values);
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, Integer... values) {

        return addValues(namespace, breadcrumb, false, values);
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, BigDecimal... values) {

        return addValues(namespace, breadcrumb, false, values);
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, Double... values) {

        return addValues(namespace, breadcrumb, false, values);
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, Date... values) {

        return addValues(namespace, breadcrumb, false, values);
    }

    
    public boolean addValue(String namespace, String[] breadcrumb, String value) {
        if (namespace == null){
        	return false;
        }
     
    	ObjectNode currentNode = addBasicData(namespace, breadcrumb, value);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        if (currentNode != null) {
            currentNode.put(lastNode, value);
            return true;
        } else {
            return false;
        }
    }

    
    public boolean addValue(String namespace, String[] breadcrumb, Long value) {
    	
    	if (namespace == null){
        	return false;
        }
    	
    	
        ObjectNode currentNode = addBasicData(namespace, breadcrumb, value);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        if (currentNode != null) {
            currentNode.put(lastNode, value);
            return true;
        } else {
            return false;
        }
    }
    

    
    public boolean addValue(String namespace, String[] breadcrumb, Integer value) {
    	
    	if (namespace == null){
        	return false;
        }
    	
       
        
        ObjectNode currentNode = addBasicData(namespace, breadcrumb, value);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        if (currentNode != null) {
            currentNode.put(lastNode, value);
            return true;
        } else {
            return false;
        }
    }

    
    public boolean addValue(String namespace, String[] breadcrumb, Double value) {
    	if (namespace == null){
        	return false;
        }
    	
      
        
        ObjectNode currentNode = addBasicData(namespace, breadcrumb, value);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        if (currentNode != null) {
            currentNode.put(lastNode, value);
            return true;
        } else {
            return false;
        }
    }

    
    public boolean addValue(String namespace, String[] breadcrumb, BigDecimal value) {
    	if (namespace == null){
        	return false;
        }
    	
       
        ObjectNode currentNode = addBasicData(namespace, breadcrumb, value);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        if (currentNode != null) {
            currentNode.put(lastNode, value);
            return true;
        } else {
            return false;
        }
    }

    
    public boolean addValue(String namespace, String[] breadcrumb, Date value) {
    	if (namespace == null){
        	return false;
        }
    	
    	ObjectNode currentNode = addBasicData(namespace, breadcrumb, value);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        if (currentNode != null) {
            currentNode.put(lastNode, dateToString(value));
            return true;
        } else {
            return false;
        }
    }

    
    public boolean addValue(String namespace, String[] breadcrumb, Boolean value) {
        ObjectNode currentNode = addBasicData(namespace, breadcrumb, value);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        if (currentNode != null) {
            currentNode.put(lastNode, value);
            return true;
        } else {
            return false;
        }
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, String... values) {

        ObjectNode currentNode = addBasicData(namespace, breadcrumb, values);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        JsonNode lastJNode = currentNode.get(lastNode);

        if (values.length == 0) {
            if (!doMerge || lastJNode == null)
                currentNode.put(lastNode, NullNode.getInstance());
        } else {

            if (!doMerge || lastJNode == null) {
                ArrayNode an = currentNode.arrayNode();
                for (String s : values) {
                    an.add(s);
                }
                currentNode.put(lastNode, an);

            } else {
                ArrayNode an = currentNode.arrayNode();
                if (lastJNode.isArray()) {
                    an = (ArrayNode) lastJNode;
                } else {
                    an.add(lastJNode.asText());
                }

                for (String s : values) {
                    an.add(s);
                }

                currentNode.put(lastNode, an);
            }
        }

        return true;
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Long... values) {

        ObjectNode currentNode = addBasicData(namespace, breadcrumb, values);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        JsonNode lastJNode = currentNode.get(lastNode);

        if (values.length == 0) {
            if (!doMerge || lastJNode == null)
                currentNode.put(lastNode, NullNode.getInstance());
        } else {

            if (!doMerge || lastJNode == null) {

                ArrayNode an = currentNode.arrayNode();
                for (Long s : values) {
                    an.add(s);
                }
                currentNode.put(lastNode, an);

            } else {
                ArrayNode an = currentNode.arrayNode();
                if (lastJNode.isArray()) {
                    an = (ArrayNode) lastJNode;
                } else {
                    an.add(lastJNode.asLong());
                }

                for (Long s : values) {
                    an.add(s);
                }

                currentNode.put(lastNode, an);
            }
        }

        return true;
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Integer... values) {

        ObjectNode currentNode = addBasicData(namespace, breadcrumb, values);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        JsonNode lastJNode = currentNode.get(lastNode);

        if (values.length == 0) {
            if (!doMerge || lastJNode == null)
                currentNode.put(lastNode, NullNode.getInstance());
        } else {

            if (!doMerge || lastJNode == null) {
                ArrayNode an = currentNode.arrayNode();
                for (Integer s : values) {
                    an.add(s);
                }
                currentNode.put(lastNode, an);

            } else {
                ArrayNode an = currentNode.arrayNode();
                if (lastJNode.isArray()) {
                    an = (ArrayNode) lastJNode;
                } else {
                    an.add(lastJNode.asInt());
                }

                for (Integer s : values) {
                    an.add(s);
                }

                currentNode.put(lastNode, an);
            }
        }

        return true;
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, BigDecimal... values) {

        ObjectNode currentNode = addBasicData(namespace, breadcrumb, values);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        JsonNode lastJNode = currentNode.get(lastNode);

        if (values.length == 0) {
            if (!doMerge || lastJNode == null)
                currentNode.put(lastNode, NullNode.getInstance());
        } else {

            if (!doMerge || lastJNode == null) {

                ArrayNode an = currentNode.arrayNode();
                for (BigDecimal s : values) {
                    an.add(s);
                }
                currentNode.put(lastNode, an);

            } else {
                ArrayNode an = currentNode.arrayNode();
                if (lastJNode.isArray()) {
                    an = (ArrayNode) lastJNode;
                } else {
                    an.add(lastJNode.asDouble());// TODO there is no BigDecimal
                                                 // method
                }

                for (BigDecimal s : values) {
                    an.add(s);
                }

                currentNode.put(lastNode, an);
            }
        }

        return true;
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Double... values) {

        ObjectNode currentNode = addBasicData(namespace, breadcrumb, values);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        JsonNode lastJNode = currentNode.get(lastNode);

        if (values.length == 0) {
            if (!doMerge || lastJNode == null)
                currentNode.put(lastNode, NullNode.getInstance());
        } else {

            if (!doMerge || lastJNode == null) {

                ArrayNode an = currentNode.arrayNode();
                for (Double s : values) {
                    an.add(s);
                }
                currentNode.put(lastNode, an);

            } else {
                ArrayNode an = currentNode.arrayNode();
                if (lastJNode.isArray()) {
                    an = (ArrayNode) lastJNode;
                } else {
                    an.add(lastJNode.asDouble());
                }

                for (Double s : values) {
                    an.add(s);
                }

                currentNode.put(lastNode, an);
            }
        }

        return true;
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Date... values) {

        ObjectNode currentNode = addBasicData(namespace, breadcrumb, values);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        JsonNode lastJNode = currentNode.get(lastNode);

        if (values.length == 0) {
            if (!doMerge || lastJNode == null) // overwrite
                currentNode.put(lastNode, NullNode.getInstance());
        } else {

            if (!doMerge || lastJNode == null) { // overwrite

                ArrayNode an = currentNode.arrayNode();
                for (Date s : values) {
                    an.add(dateToString(s));
                }
                currentNode.put(lastNode, an);

            } else { // merge
                ArrayNode an = currentNode.arrayNode();
                if (lastJNode.isArray()) {
                    an = (ArrayNode) lastJNode;
                } else {
                    an.add(lastJNode.asDouble());
                }

                for (Date s : values) {
                    an.add(dateToString(s));
                }

                currentNode.put(lastNode, an);
            }
        }

        return true;
    }

    
    public boolean addValues(String namespace, String[] breadcrumb, boolean doMerge, Boolean... values) {

        ObjectNode currentNode = addBasicData(namespace, breadcrumb, values);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        JsonNode lastJNode = currentNode.get(lastNode);

        if (values.length == 0) {
            if (!doMerge || lastJNode == null)
                currentNode.put(lastNode, NullNode.getInstance());
        } else {

            if (!doMerge || lastJNode == null) {
                ArrayNode an = currentNode.arrayNode();
                for (Boolean s : values) {
                    an.add(s);
                }
                currentNode.put(lastNode, an);

            } else {
                ArrayNode an = currentNode.arrayNode();
                if (lastJNode.isArray()) {
                    an = (ArrayNode) lastJNode;
                } else {
                    an.add(lastJNode.asDouble());
                }

                for (Boolean s : values) {
                    an.add(s);
                }

                currentNode.put(lastNode, an);
            }
        }

        return true;
    }

    private boolean addJsonNode(String namespace, String[] breadcrumb, boolean doMerge, JsonNode values) {

    	if(namespace == null) {
    		return false;
    	}
    	
        ObjectNode currentNode = addBasicData(namespace, breadcrumb, values);
        String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[breadcrumb.length - 1];

        JsonNode lastJNode = currentNode.get(lastNode);

        if (values == null) {
            return false;
        } else {

            if (!doMerge || lastJNode == null) {
                currentNode.put(lastNode, values);
            } else {
                ArrayNode an = currentNode.arrayNode();
                if (lastJNode.isArray()) {
                    an = (ArrayNode) lastJNode;
                } else {
                    an.add(lastJNode);
                }
                an.add(values);
                currentNode.put(lastNode, an);
            }
        }

        return true;
    }

    // private boolean addJsonNode( String namespace, String[] breadcrumb,
    // JsonNode value) {
    // return addJsonNode(namespace, breadcrumb, false, value);
    // }

    
    public boolean addDataHolder(String namespace, String[] breadcrumb, IDataHolder toBeMerged) {
        return addJsonString(namespace, breadcrumb, toBeMerged.getJSONData());
    }

    
    public boolean addJsonString(String namespace, String[] breadcrumb, String value) {
        return addJsonString(namespace, breadcrumb, false, value);
    }

    
    public boolean addJsonString(String namespace, String[] breadcrumb, boolean doMerge, String value) {
        JsonNode json = null;
        try {
            json = m_mapper.readTree(value);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        if (json != null && json.size() != 0) {
            return addJsonNode(namespace, breadcrumb, doMerge, json);
        }

        return false;
    }

    
    public String getJSONData(String namespace, String[] breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);

        if (root == MissingNode.getInstance()) {
            return StringUtils.EMPTY;
        }

        ObjectNode currentNode = (ObjectNode) basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        JsonNode lastJNode = currentNode;
        if (lastNode != null) {
            lastJNode = currentNode.path(lastNode);
        }
        if (lastJNode != null) {
            return lastJNode.toString();
        } else {
            return StringUtils.EMPTY;
        }
    }

    private String dateToString(Date date) {
        // DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        // return df.format(date);
        return String.valueOf(date.getTime() / 1000);
    }

    private Date stringToDate(String date) {
        DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        try {
            return df.parse(date);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private ObjectNode addBasicData(String namespace, String[] breadcrumb, Object[] values) {
        if (breadcrumb == null || breadcrumb.length == 0 || values == null)
            return null;
        if (namespace == null)
            namespace = m_default_Name_space;

        ObjectNode withNameSpace = m_ObjectNode.with(namespace);
        ObjectNode currentNode = withNameSpace;
        if (breadcrumb.length > 1) {
            for (int i = 0; i < (breadcrumb.length - 1); i++) {
                currentNode = currentNode.with(breadcrumb[i]);
            }
        }
        return currentNode;
    }

    /**
     * Adds a path with the given namespace and breadcrumb to the tree.
     * 
     * @param namespace
     * @param breadcrumb
     *            : should not be null and length > 0
     * @param value
     * @return the leafnode of the path. returns null if breadcrumb is null or
     *         length = 0
     */
    private ObjectNode addBasicData(String namespace, String[] breadcrumb, Object value) {
        if (breadcrumb == null || breadcrumb.length == 0 || value == null)
            return null;
        if (namespace == null)
            namespace = m_default_Name_space;

        ObjectNode withNameSpace = m_ObjectNode.with(namespace);
        ObjectNode currentNode = withNameSpace;
        if (breadcrumb.length > 1) {
            for (int i = 0; i < (breadcrumb.length - 1); i++) {
                currentNode = currentNode.with(breadcrumb[i]);
            }
        }
        return currentNode;
    }

    


    
    public boolean contains(String namespace, String[] breadcrumb) {
        // TODO Auto-generated method stub
        return false;
    }

    
    public boolean containsSimpleData(String namespace, String key) {
        // TODO Auto-generated method stub
        return false;
    }

    
    public boolean mergeOrCreate(IDataHolder toBeMerged) {

        Object jsonRootNode = toBeMerged.getJSONData();
        ObjectNode nodeToBeMerged = (ObjectNode) jsonRootNode;
        this.m_ObjectNode.putAll(nodeToBeMerged);
        return true;
    }

    public boolean recursiveMerge(IDataHolder toBeMerged) {
        Object jsonRootNode = toBeMerged.getJSONRootNode();
        ObjectNode nodeToBeMerged = (ObjectNode) jsonRootNode;

        return recursiveMerge(nodeToBeMerged, this.m_ObjectNode);

    }

    private boolean recursiveMerge(ObjectNode toBeMerged, ObjectNode parent) {
        Iterator<String> iter = toBeMerged.getFieldNames();

        while (iter.hasNext()) {
            String fieldName = iter.next();

            JsonNode currNode = parent.get(fieldName);

            if (currNode == null) {
                parent.put(fieldName, toBeMerged.get(fieldName));
            } else if (currNode instanceof ObjectNode) {
                recursiveMerge((ObjectNode) toBeMerged.get(fieldName), (ObjectNode) currNode);
            }
        }
        return true;
    }

    
    public boolean mergeOrCreate(IDataHolder toBeMerged, boolean collapseNameSpaceAndMerge) {
        // TODO Auto-generated method stub
        return false;
    }

    
    public boolean mergeOrCreate(String jsonData, String sourceNamespace, String destinationNameSpace) {
        JsonNode rootNode = null;
        try {
            rootNode = m_mapper.readTree(new StringReader(jsonData));
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // ObjectNode jsonNodeDestination =
        // (ObjectNode)m_ObjectNode.get(destinationNameSpace);
        ObjectNode jsonNodeSource = (ObjectNode) rootNode.get(sourceNamespace);

        m_ObjectNode.put(destinationNameSpace, jsonNodeSource);

        return false;
    }

    
    public boolean mergeOrCreate(String jsonData, boolean collapseNameSpaceAndMerge) {
        // TODO Auto-generated method stub
        return false;
    }

    
    public String getJSONData() {
        return m_ObjectNode == null ? "" : m_ObjectNode.toString();
    }

    public DataHolderIterator getArrayIterator(String namespace, String... breadcrumb) {
        if (namespace == null)
            namespace = m_default_Name_space;

        JsonNode root = m_ObjectNode.path(namespace);
        JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
        String lastNode = basicReadlastNode(root, breadcrumb);
        JsonNode lastJNode = currentNode.path(lastNode);

        if (!lastJNode.isMissingNode()) {

        }
        return new DataHolderIterator(namespace, breadcrumb[breadcrumb.length - 1], lastJNode);
    }
    

    
    public Object getValue(String namespace, String[] breadcrumb) {
            if (namespace == null)
                    namespace = m_default_Name_space;

            JsonNode root = m_ObjectNode.path(namespace);
            if (root.isMissingNode())
                    return null;

            
            if(breadcrumb != null){
                JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
                String lastNode = basicReadlastNode(root, breadcrumb);
    
                if (currentNode.path(lastNode).isMissingNode())
                        return null;
    
                return currentNode.path(lastNode);
            } else {
                return root;
            }
    }
    
    
    public boolean addObjectValue(String namespace, String[] breadcrumb, Object value) {
            ObjectNode currentNode;
            String lastNode = null;
            if (breadcrumb == null) {
                currentNode = m_ObjectNode;
                lastNode = namespace;
            } else {
                    
                    currentNode =        addBasicData(namespace, breadcrumb, value);
                    lastNode = breadcrumb.length == 1 ? breadcrumb[0]
                             : breadcrumb[breadcrumb.length - 1];
            }
            
            if (currentNode != null) {
                    if (value instanceof Long) {
                            currentNode.put(lastNode, (Long)value);
                    }else if (value instanceof Boolean) {
                            currentNode.put(lastNode, (Boolean)value);
                    } else if (value instanceof String) {
                            currentNode.put(lastNode, (String)value);
                    } else if (value instanceof Double) {
                            currentNode.put(lastNode, (Double)value);
                    } else if (value instanceof JsonNode) {
                            currentNode.put(lastNode, (JsonNode)value);
                    } else if (value instanceof Integer) {
                            currentNode.put(lastNode, (Integer)value);
                    }
                    return true;
            } else {
                    return false;
            }
    }
    
    
    public List<Object> getObjectValues(String namespace, String[] breadcrumb) {
            if (namespace == null)
                    namespace = m_default_Name_space;

            JsonNode root = m_ObjectNode.path(namespace);
            if (root.isMissingNode())
                    return null;
            
            JsonNode currentNode = basicReadPenultimate(root, breadcrumb);
            String lastNode = basicReadlastNode(root, breadcrumb);
            List<Object> listOfValues = new ArrayList<Object>();
            for (JsonNode node : currentNode.path(lastNode)) {
                    if (!currentNode.path(lastNode).isMissingNode())
                            listOfValues.add(node);
            }
            return listOfValues;
    }
    
    
    public boolean addObjectValues(String namespace, String[] breadcrumb, Object... values){
            return addObjectValues(namespace, breadcrumb, false, values);
    }
    
    
    public boolean addObjectValues(String namespace, String[] breadcrumb,
                    boolean doMerge, Object... values) {

            ObjectNode currentNode = addBasicData(namespace, breadcrumb, values);
            String lastNode = breadcrumb.length == 1 ? breadcrumb[0] : breadcrumb[ breadcrumb.length -1 ];
            
            JsonNode lastJNode = currentNode.get(lastNode);
            
            if( values.length == 0){
                    if(!doMerge || lastJNode == null)
                            currentNode.put( lastNode , NullNode.getInstance() );
            }else {
                    
                    if(!doMerge || lastJNode == null){
                            ArrayNode an = currentNode.arrayNode();
                            for( Object value : values ){
                                    if (value instanceof Long) {
                                            an.add((Long)value);
                                    }else if (value instanceof Boolean) {
                                            an.add((Boolean)value);
                                    } else if (value instanceof String) {
                                            an.add((String)value);
                                    } else if (value instanceof Double) {
                                            an.add((Double)value);
                                    } else if (value instanceof JsonNode) {
                                            an.add((JsonNode)value);
                                    } else if (value instanceof Integer) {
                                            an.add((Integer)value);
                                    }else {
                                        s_logger.error("Unsupported dataType namespace " + namespace + " path " + breadcrumb + " value " + value);
                                    }
                            }
                            currentNode.put(lastNode, an);
                            
                    } else{
                            ArrayNode an = currentNode.arrayNode();
                            if(lastJNode.isArray()){
                                    an = (ArrayNode)lastJNode;
                            }else{
                                    an.add(lastJNode.asText());
                            }
                            
                            for( Object value : values ){
                                    if (value instanceof Long) {
                                            an.add((Long)value);
                                    }else if (value instanceof Boolean) {
                                            an.add((Boolean)value);
                                    } else if (value instanceof String) {
                                            an.add((String)value);
                                    } else if (value instanceof Double) {
                                            an.add((Double)value);
                                    } else if (value instanceof JsonNode) {
                                            an.add((JsonNode)value);
                                    } else if (value instanceof Integer) {
                                            an.add((Integer)value);
                                    }else {
                                        s_logger.error("Unsupported dataType namespace " + namespace + " path " + breadcrumb + " value " + value);
                                    }
                            }
                            
                            currentNode.put(lastNode, an);
                    }
            }
            
            return true;
    }

    public static void main(String[] arguments) {

        String s = "{\"item_id\":\"4747589297\",\"item_data\":{ \"prod_ref_id\":\"3485414\",\"leaf_categ_id\":\"176984\",\"slr_id\":\"6815682\",\"curnt_price_lstg_curncy\":\"13.95\",\"qty_sold\":\"0\",\"qty_avail\":\"1\",\"item_site_id\":\"0\",\"item_cndtn_id\":\"1000\",\"cndtn_rollup_id\":\"1\",\"fdbk_pct\":\"100\",\"shpmt_fee_amt_lstg_curncy\":\"3.0\",\"meta_categ_name\":\"Music\",\"categ_lvl2_name\":\"CDs\",\"categ_lvl3_name\":\"CDs\"},\"gmv_metrics\":{ \"most_recent_transaction_dt\":\"null\",\"gmv_7_days\":\"null\",\"gmv_14_days\":\"null\",\"gmv_30_days\":\"null\",\"gmv_90_days\":\"null\",\"gmv_180_days\":\"null\",\"gmv_450_days\":\"null\",\"qty_sold_7_days\":\"null\",\"qty_sold_14_days\":\"null\",\"qty_sold_30_days\":\"null\",\"qty_sold_90_days\":\"null\",\"qty_sold_180_days\":\"null\",\"qty_sold_450_days\":\"null\",\"monthly_qty_sold_pct\":\"null\"},\"image_metrics\":{ \"low_qlty_image_count\":\"null\",\"high_qlty_image_count\":\"null\",\"high_qlty_image_score\":\"null\"},\"vi_stats\":{ \"cumulative_wtchr_count\":\"null\",\"cumulative_vi_count\":\"null\"},\"soj_metrics\":{ \"srp_imprsn_cnt\":\"null\",\"soj_vi_cnt\":\"null\"}}";
        GenericDataHolder dataHolder = new GenericDataHolder("IT", s);

        System.out.println(dataHolder.getStringValue("IT", new String[] { "item_id" }));

        System.out.println(s.toString());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.createObjectNode(); // will be of type
                                                       // ObjectNode
        ((ObjectNode) rootNode).with("Namespace").with("ItemActiveNode").put("ItemId", 1212121211);
        ((ObjectNode) rootNode).with("Namespace").with("ItemActiveNode").put("Title", " This is Item Title");
        ((ObjectNode) rootNode).with("Namespace").with("ItemActiveNode").put("NBINPrice", 20.22);
        ((ObjectNode) rootNode).with("Namespace").with("ItemActiveNode").with("NestedData").put("Nestedname_1", 12121);
        ((ObjectNode) rootNode).with("Namespace").with("ItemActiveNode").with("NestedData").put("Nestedname_2", "Hello World");

        ArrayNode an = ((ObjectNode) rootNode).arrayNode();
        an.add("v1");
        an.add("v2");
        an.add("v3");
        ((ObjectNode) rootNode).with("Namespace").with("ItemActiveNode").with("NestedData").put("Nestedname_3", an);
        System.out.println(rootNode.toString());

        GenericDataHolder holder1 = new GenericDataHolder();
        holder1.addValues("JG", new String[] { "Level1_name", "Level2_name" }, false, "Value 1");
        holder1.addValues("JG", new String[] { "Level1_name", "Level3_name_double" }, false, 2.0);
        System.out.println(holder1.getJSONData());
        // String jsonData1 = holder1.getJSONData();

        GenericDataHolder holder3 = new GenericDataHolder();
        Long itemId = new Long(1234);
        holder3.addValues("ActiveItem", new String[] { itemId.toString(), "NBINPrice" }, false, 99.99);
        holder3.addValues("ActiveItem", new String[] { itemId.toString(), "ItemID" }, false, 1212122);
        holder3.addValues("ActiveItem", new String[] { itemId.toString(), "Categories" }, false, new Integer[] { 601, 4050, 302 });
        holder3.addValues("ActiveItem", new String[] { itemId.toString(), "epid" }, false, 1111);

        GenericDataHolder holder2 = new GenericDataHolder();
        holder2.addValues("JG_NS_1", new String[] { "Level1_name", "Level2_name" }, true, "Value 1_NS1");
        holder2.addValues("JG_NS_1", new String[] { "Level1_name", "Level2_name" }, true, "33333");
        holder2.addValues("JG_NS_1", new String[] { "Level1_name", "Level3_name_double" }, false, 3.0);
        holder2.addValues("JG_NS_1", new String[] { "Level1_name", "Level4_name_double" }, false, 121212);
        holder2.addValues("offlineIdex", new String[] { "A", "B", "GMV" }, false, 700.00);

        boolean val = holder2.isPathPresent("JG_NS_1", new String[] { "Level1_name", "Level4_name_double" });
        val = holder2.isPathPresent("JG_NS_1", new String[] { "Level1_name", "Level4_name_double2" });
        holder2.deleteNode("JG_NS_1", new String[] { "Level1_name" });

        // holder3.addDataHolder("ActiveItem", new String[]{"1234"}, holder2.ge,
        // "RS_ITEM_CASSINI_NS_1", new String[]{"Level1_name"});
        // holder3.addJsonNode("ActiveItem", new String[]{}, holder2.ge)

        // for( int j=0 ; j< 1 ; j++ ){
        // for( int i=0 ; i < 200000 ; i++ ){
        // holder2.addValues("RS_ITEM_CASSINI_NS_1", new String[]{"Level1_name",
        // "Level"+i+"_name_double" } , i+"value");
        // if( i > 0 && ( i %1000 ) == 0 ){
        // System.out.println( i );
        // long totalMemory = Runtime.getRuntime().totalMemory();
        // long freeMemory = Runtime.getRuntime().freeMemory();
        // System.out.println( totalMemory - freeMemory );
        // System.out.println( "t "+totalMemory);
        // double percentage = (( new Double( totalMemory) - new Double(
        // freeMemory) )/new Double( totalMemory))*100 ;
        // System.out.println("% used " + ( percentage));
        // }
        //
        // }
        // }

        System.out.println(holder2.getJSONData());

        System.out.println(holder2.getDoubleValue("offlineIdex", new String[] { "A", "B", "GMV" }));

        // holder1.mergeOrCreate(holder2.getJSONData(), "RS_ITEM_CASSINI_NS_1",
        // "RS_ITEM_CASSINI");

        // System.out.println( holder1.getJSONData());

    }

    
    public String toString() {
        // TODO Auto-generated method stub
        return this.getJSONData();
    }

	
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return m_ObjectNode == null || !m_ObjectNode.getFields().hasNext();
	}

    
    public Object getJSONRootNode() {
        return m_ObjectNode;
    }
}
