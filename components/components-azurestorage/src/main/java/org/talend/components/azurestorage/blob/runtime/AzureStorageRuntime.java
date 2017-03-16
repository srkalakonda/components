package org.talend.components.azurestorage.blob.runtime;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.talend.components.api.component.runtime.RuntimableRuntime;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azurestorage.AzureStorageProvideConnectionProperties;
import org.talend.components.azurestorage.tazurestorageconnection.TAzureStorageConnectionProperties;
import org.talend.components.azurestorage.utils.SharedAccessSignatureUtils;
import org.talend.daikon.properties.ValidationResult;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class AzureStorageRuntime implements RuntimableRuntime<ComponentProperties> {
	
    public static final String KEY_CONNECTION_PROPERTIES = "connection";

	public AzureStorageProvideConnectionProperties properties;
	
    @Override
    public ValidationResult initialize(RuntimeContainer container, ComponentProperties properties) {
        this.properties = (AzureStorageProvideConnectionProperties) properties;
        return ValidationResult.OK;
    }
    
    public TAzureStorageConnectionProperties validateConnection(RuntimeContainer container) {
        TAzureStorageConnectionProperties connProps = getConnectionProperties();
        String refComponentId = connProps.getReferencedComponentId();
        TAzureStorageConnectionProperties sharedConn;
        // Using another component's connection
        if (refComponentId != null) {
            // In a runtime container
            if (container != null) {
                sharedConn = (TAzureStorageConnectionProperties) container.getComponentData(refComponentId,
                        KEY_CONNECTION_PROPERTIES);
                if (sharedConn != null) {
                    return sharedConn;
                }
            }
            // Design time
            connProps = connProps.getReferencedConnectionProperties();
        }
        if (container != null) {
            container.setComponentData(container.getCurrentComponentId(), KEY_CONNECTION_PROPERTIES, connProps);
        }
        return connProps;
    }

    public CloudStorageAccount getStorageAccount(RuntimeContainer container) throws URISyntaxException, InvalidKeyException {
        TAzureStorageConnectionProperties conn = validateConnection(container);
        CloudStorageAccount account;
        if (conn.useSharedAccessSignature.getValue()) {
            SharedAccessSignatureUtils sas = SharedAccessSignatureUtils
                    .getSharedAccessSignatureUtils(conn.sharedAccessSignature.getValue());
            StorageCredentials credentials = new StorageCredentialsSharedAccessSignature(sas.getSharedAccessSignature());
            account = new CloudStorageAccount(credentials, true, null, sas.getAccount());

        } else {
            String acct = conn.accountName.getValue();
            String key = conn.accountKey.getValue();
            String protocol = conn.protocol.getValue().toString().toLowerCase();
            String storageConnectionString = "DefaultEndpointsProtocol=" + protocol + ";" + "AccountName=" + acct + ";"
                    + "AccountKey=" + key;
            account = CloudStorageAccount.parse(storageConnectionString);
        }

        return account;
    }

    /**
     * getServiceClient.
     *
     * @param container {@link RuntimeContainer} container
     * @return {@link CloudBlobClient} cloud blob client
     */
    public CloudBlobClient getServiceClient(RuntimeContainer container) throws InvalidKeyException, URISyntaxException {
        return getStorageAccount(container).createCloudBlobClient();
    }

    /**
     * getStorageContainerReference.
     *
     * @param container {@link RuntimeContainer} container
     * @param storageContainer {@link String} storage container
     * @return {@link CloudBlobContainer} cloud blob container
     * @throws StorageException
     * @throws URISyntaxException
     * @throws InvalidKeyException
     */
    public CloudBlobContainer getStorageContainerReference(RuntimeContainer container, String storageContainer)
            throws InvalidKeyException, URISyntaxException, StorageException {
        return getServiceClient(container).getContainerReference(storageContainer);
    }
    
    public TAzureStorageConnectionProperties getConnectionProperties() {
        return properties.getConnectionProperties();
    }

}
