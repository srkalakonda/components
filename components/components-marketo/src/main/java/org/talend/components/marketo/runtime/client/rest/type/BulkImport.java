// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.runtime.client.rest.type;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.talend.components.marketo.MarketoConstants;

public class BulkImport {

    /**
     * Unique integer id of the import batch
     */
    Integer batchId;

    /**
     * Time spent on the batch
     */
    String importTime;

    /**
     * 
     */
    String importId; // Leads only

    /**
     * Status message of the batch
     */
    String message;

    /**
     * Number of rows processed so far
     * 
     */
    Integer numOfLeadsProcessed; // Leads only

    /**
     * Number of rows processed so far
     */
    Integer numOfObjectsProcessed; // CO only

    /**
     * Number of rows failed so far
     */
    Integer numOfRowsFailed;

    /**
     * Number of rows with a warning so far
     */
    Integer numOfRowsWithWarning;

    /**
     * Object API Name
     */
    String objectApiName;

    /**
     * Bulk operation type. Can be import or export
     */
    String operation; // CO only

    /**
     * Status of the batch
     */
    String status;

    public Integer getBatchId() {
        return batchId;
    }

    public void setBatchId(Integer batchId) {
        this.batchId = batchId;
    }

    public String getImportTime() {
        return importTime;
    }

    public void setImportTime(String importTime) {
        this.importTime = importTime;
    }

    public String getImportId() {
        return importId;
    }

    public void setImportId(String importId) {
        this.importId = importId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getNumOfLeadsProcessed() {
        return numOfLeadsProcessed;
    }

    public void setNumOfLeadsProcessed(Integer numOfLeadsProcessed) {
        this.numOfLeadsProcessed = numOfLeadsProcessed;
    }

    public Integer getNumOfObjectsProcessed() {
        return numOfObjectsProcessed;
    }

    public void setNumOfObjectsProcessed(Integer numOfObjectsProcessed) {
        this.numOfObjectsProcessed = numOfObjectsProcessed;
    }

    public Integer getNumOfRowsFailed() {
        return numOfRowsFailed;
    }

    public void setNumOfRowsFailed(Integer numOfRowsFailed) {
        this.numOfRowsFailed = numOfRowsFailed;
    }

    public Integer getNumOfRowsWithWarning() {
        return numOfRowsWithWarning;
    }

    public void setNumOfRowsWithWarning(Integer numOfRowsWithWarning) {
        this.numOfRowsWithWarning = numOfRowsWithWarning;
    }

    public String getObjectApiName() {
        return objectApiName;
    }

    public void setObjectApiName(String objectApiName) {
        this.objectApiName = objectApiName;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getNumOfRowsProcessed() {
        return numOfLeadsProcessed != null ? numOfLeadsProcessed : numOfObjectsProcessed;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("BulkImport{");
        sb.append("batchId=").append(batchId);
        sb.append(", importTime='").append(importTime).append('\'');
        sb.append(", importId='").append(importId).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", numOfRowsProcessed=").append(getNumOfRowsProcessed());
        sb.append(", numOfLeadsProcessed=").append(numOfLeadsProcessed);
        sb.append(", numOfObjectsProcessed=").append(numOfObjectsProcessed);
        sb.append(", numOfRowsFailed=").append(numOfRowsFailed);
        sb.append(", numOfRowsWithWarning=").append(numOfRowsWithWarning);
        sb.append(", objectApiName='").append(objectApiName).append('\'');
        sb.append(", operation='").append(operation).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public IndexedRecord toIndexedRecord() {
        IndexedRecord record = new GenericData.Record(MarketoConstants.getBulkImportCustomObjectSchema());
        record.put(0, getBatchId());
        record.put(1, getImportTime());
        record.put(2, getImportId());
        record.put(3, getMessage());
        record.put(4, getNumOfObjectsProcessed());
        record.put(5, getNumOfRowsFailed());
        record.put(6, getNumOfRowsWithWarning());
        record.put(7, getObjectApiName());
        record.put(8, getOperation());
        record.put(9, getStatus());

        return record;
    }
}
