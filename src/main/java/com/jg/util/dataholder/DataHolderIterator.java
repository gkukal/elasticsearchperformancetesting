package com.jg.util.dataholder;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

/**
 * Iterator over subdocument at given path in the dataholder.
 * The path can point to an array or an object node.
 * @author gkukal
 *
 */
public class DataHolderIterator implements Iterator<IDataHolder>{

    private Iterator<JsonNode> arrayIter = null;
    boolean isObjectNode = false;
    boolean objectNodeIterationDone = false;
    JsonNode jsonNode = null;
    String namespace = null;
    String endNodeKey = null;
    
    public DataHolderIterator( String namespace, String endNode, JsonNode jsonNode){
        this.namespace = namespace;
        this.endNodeKey = endNode;
        if (!jsonNode.isMissingNode()) {
            this.jsonNode = jsonNode;
            if (jsonNode.isArray()) {
                this.arrayIter = jsonNode.iterator();
            } else {
                isObjectNode = true;    
            }
        }
    }
    
    
    public boolean hasNext() {
        if (arrayIter != null) {
            return arrayIter.hasNext();
        } else if (isObjectNode && !objectNodeIterationDone) {
            return true;
        } else {
            return false;
        }
    }

    
    public IDataHolder next() {
        
        if (isObjectNode) {
            if (objectNodeIterationDone) {
                return null;
            } else {
               IDataHolder dataHolder = new GenericDataHolder();
               dataHolder.addJsonString(namespace, new String[] {endNodeKey}, jsonNode.toString());
               objectNodeIterationDone = true;
               return dataHolder;
            }
        }
        
        if (arrayIter == null || !arrayIter.hasNext()) {
            return null;
        }
        
        JsonNode node = arrayIter.next();
        String json = node.toString();
        IDataHolder dataHolder = new GenericDataHolder();
        dataHolder.addJsonString(namespace, new String[] {endNodeKey}, json);
        
        return dataHolder;
    }

    
    public void remove() {
    	arrayIter.remove();
//        throw new UnsupportedOperationException("remove is not supported for DataHolderArrayIterator");    
        
    }

}

