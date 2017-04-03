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

package org.talend.components.netsuite.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.talend.components.api.component.runtime.Result;
import org.talend.components.api.component.runtime.WriteOperation;
import org.talend.components.api.component.runtime.WriterWithFeedback;
import org.talend.components.netsuite.SchemaCustomMetaDataSource;
import org.talend.components.netsuite.client.MetaDataSource;
import org.talend.components.netsuite.client.NetSuiteClientService;
import org.talend.components.netsuite.client.NetSuiteException;
import org.talend.components.netsuite.client.NsReadResponse;
import org.talend.components.netsuite.client.NsRef;
import org.talend.components.netsuite.client.NsWriteResponse;
import org.talend.components.netsuite.client.model.TypeDesc;

/**
 *
 */
public class NetSuiteOutputWriter implements WriterWithFeedback<Result, IndexedRecord, IndexedRecord> {

    public static final int DEFAULT_BATCH_SIZE = 100;

    protected final NetSuiteWriteOperation writeOperation;

    protected final List<IndexedRecord> successfulWrites = new ArrayList<>();

    protected final List<IndexedRecord> rejectedWrites = new ArrayList<>();

    protected boolean exceptionForErrors = true;

    protected NetSuiteClientService<?> clientService;

    protected MetaDataSource metaDataSource;

    protected OutputAction action;

    protected TypeDesc typeDesc;

    protected NsObjectOutputTransducer transducer;

    protected BulkRecordSource bulkRecordSource;

    protected BulkWriter<?, ?> bulkWriter;

    protected int dataCount = 0;

    public NetSuiteOutputWriter(NetSuiteWriteOperation writeOperation) {
        this.writeOperation = writeOperation;
    }

    @Override
    public Iterable<IndexedRecord> getSuccessfulWrites() {
        return successfulWrites;
    }

    @Override
    public Iterable<IndexedRecord> getRejectedWrites() {
        return rejectedWrites;
    }

    @Override
    public void open(String uId) throws IOException {
        try {
            clientService = writeOperation.getSink().getClientService();

            Schema schema = writeOperation.getSchema();

            MetaDataSource originalMetaDataSource = clientService.getMetaDataSource();
            metaDataSource = clientService.createDefaultMetaDataSource();
            metaDataSource.setCustomizationEnabled(originalMetaDataSource.isCustomizationEnabled());
            SchemaCustomMetaDataSource schemaCustomMetaDataSource = new SchemaCustomMetaDataSource(
                    clientService.getBasicMetaData(), originalMetaDataSource.getCustomMetaDataSource(), schema);
            metaDataSource.setCustomMetaDataSource(schemaCustomMetaDataSource);

            action = writeOperation.getProperties().module.action.getValue();

            String typeName = writeOperation.getProperties().module.moduleName.getValue();
            typeDesc = metaDataSource.getTypeInfo(typeName);

            transducer = new NsObjectOutputTransducer(clientService, typeDesc.getTypeName());
            transducer.setMetaDataSource(metaDataSource);

            switch (action) {
            case ADD:
                bulkWriter = new BulkAdd<>();
                break;
            case UPDATE:
                bulkWriter = new BulkUpdate<>();
                bulkRecordSource = new BulkRecordSource();
                transducer.setRecordSource(bulkRecordSource);
                break;
            case UPSERT:
                bulkWriter = new BulkUpsert<>();
                bulkRecordSource = new BulkRecordSource();
                transducer.setRecordSource(bulkRecordSource);
                break;
            case DELETE:
                bulkWriter = new BulkDelete<>();
                transducer.setReference(true);
                break;
            default:
                throw new NetSuiteException("Output operation not implemented: " + action);
            }

        } catch (NetSuiteException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void write(Object object) throws IOException {
        IndexedRecord record = (IndexedRecord) object;
        bulkWriter.write(record);
    }

    @Override
    public Result close() throws IOException {
        bulkWriter.flush();

        Result result = new Result();
        result.totalCount = dataCount;
        result.successCount = successfulWrites.size();
        result.rejectCount = rejectedWrites.size();
        return result;
    }

    @Override
    public WriteOperation<Result> getWriteOperation() {
        return writeOperation;
    }

    abstract class BulkWriter<T, RefT> {
        private int batchSize = DEFAULT_BATCH_SIZE;
        private List<IndexedRecord> inputRecordList = new ArrayList<>();

        int getBatchSize() {
            return batchSize;
        }

        void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        void write(IndexedRecord indexedRecord) {
            if (inputRecordList.size() == batchSize) {
                flush();
            }

            inputRecordList.add(indexedRecord);
        }

        void flush() {
            try {
                write(inputRecordList);
            } finally {
                inputRecordList.clear();
            }
        }

        private void write(List<IndexedRecord> indexedRecordList) {
            if (indexedRecordList.isEmpty()) {
                return;
            }

            if (bulkRecordSource != null) {
                List<NsRef> refList = new ArrayList<>(indexedRecordList.size());
                for (IndexedRecord indexedRecord : indexedRecordList) {
                    NsRef ref = transducer.getRef(indexedRecord);
                    if (ref != null) {
                        refList.add(ref);
                    }
                }

                bulkRecordSource.retrieve(refList);
            }

            List<T> nsObjectList = new ArrayList<>(indexedRecordList.size());
            for (IndexedRecord indexedRecord : indexedRecordList) {
                Object nsObject = transducer.write(indexedRecord);
                nsObjectList.add((T) nsObject);
            }

            List<NsWriteResponse<RefT>> responseList = doWrite(nsObjectList);

            for (int i = 0; i < responseList.size(); i++) {
                NsWriteResponse<RefT> response = responseList.get(i);
                IndexedRecord indexedRecord = indexedRecordList.get(i);
                processResponse(response, indexedRecord);
            }
        }

        private void processResponse(NsWriteResponse<RefT> response, IndexedRecord indexedRecord) {
            if (response.getStatus().isSuccess()) {
                successfulWrites.add(indexedRecord);
            } else {
                if (exceptionForErrors) {
                    NetSuiteClientService.checkError(response.getStatus());
                }
                rejectedWrites.add(indexedRecord);
            }

            dataCount++;
        }

        protected abstract List<NsWriteResponse<RefT>> doWrite(List<T> nsObjectList);
    }

    class BulkAdd<T, RefT> extends BulkWriter<T, RefT> {

        @Override
        protected List<NsWriteResponse<RefT>> doWrite(List<T> nsObjectList) {
            return clientService.addList(nsObjectList);
        }
    }

    class BulkUpdate<T, RefT> extends BulkWriter<T, RefT> {

        @Override
        protected List<NsWriteResponse<RefT>> doWrite(List<T> nsObjectList) {
            return clientService.updateList(nsObjectList);
        }
    }

    class BulkUpsert<T, RefT> extends BulkWriter<T, RefT> {

        @Override
        protected List<NsWriteResponse<RefT>> doWrite(List<T> nsObjectList) {
            return clientService.upsertList(nsObjectList);
        }
    }

    class BulkDelete<RefT> extends BulkWriter<RefT, RefT> {

        @Override
        protected List<NsWriteResponse<RefT>> doWrite(List<RefT> nsObjectList) {
            return clientService.deleteList(nsObjectList);
        }
    }

    class BulkRecordSource implements NsObjectOutputTransducer.RecordSource {
        private Map<String, Object> recordMap;

        public void retrieve(final List<NsRef> refList) {
            recordMap = new HashMap<>(refList.size());

            List<Object> nativeRefList = new ArrayList<>(refList.size());
            for (int i = 0; i < refList.size(); i++) {
                NsRef ref = refList.get(i);
                Object nativeRef = ref.toNativeRef(clientService.getBasicMetaData());
                nativeRefList.add(nativeRef);
            }

            List<NsReadResponse<Object>> responseList = clientService.getList(nativeRefList);
            for (int i = 0; i < refList.size(); i++) {
                NsRef ref = refList.get(i);
                NsReadResponse<Object> response = responseList.get(i);
                if (response.getStatus().isSuccess()) {
                    recordMap.put(ref.getInternalId(), response.getRecord());
                }
            }
        }

        @Override
        public Object get(NsRef ref) throws NetSuiteException {
            return recordMap.get(ref);
        }
    }
}
