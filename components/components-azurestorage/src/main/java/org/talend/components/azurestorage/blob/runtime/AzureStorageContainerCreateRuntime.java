package org.talend.components.azurestorage.blob.runtime;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.ComponentDriverInitialization;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.azurestorage.blob.AzureStorageContainerDefinition;
import org.talend.components.azurestorage.blob.tazurestoragecontainercreate.TAzureStorageContainerCreateProperties;
import org.talend.components.azurestorage.blob.tazurestoragecontainercreate.TAzureStorageContainerCreateProperties.AccessControl;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;

import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

/**
 * Runtime implementation for AzureStorage container create feature. Creates
 * container These methods are called only on Driver node in following order: 1)
 * {@link this#initialize(RuntimeContainer,
 * TAzureStorageContainerCreateProperties)} 2)
 * {@link this#runAtDriver(RuntimeContainer)} Instances of this class should not
 * be serialized and sent on worker nodes
 */
public class AzureStorageContainerCreateRuntime extends AzureStorageRuntime
		implements ComponentDriverInitialization<ComponentProperties> {

	private static final long serialVersionUID = -8413348199906078372L;

	private static final Logger LOGGER = LoggerFactory.getLogger(AzureStorageContainerCreateRuntime.class);

	private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider()
			.getI18nMessages(AzureStorageContainerCreateRuntime.class);

	private String containerName;

	private AccessControl access;

	private boolean dieOnError;

	@Override
	public ValidationResult initialize(RuntimeContainer container, ComponentProperties properties) {
		ValidationResult validationResult = super.initialize(container, properties);
		if (validationResult.getStatus() == ValidationResult.Result.ERROR) {
			return validationResult;
		}
		TAzureStorageContainerCreateProperties componentProperties = (TAzureStorageContainerCreateProperties) properties;
		this.containerName = componentProperties.container.getValue();
		this.access = componentProperties.accessControl.getValue();
		this.dieOnError = componentProperties.dieOnError.getValue();
		if (containerName == null || containerName.isEmpty()) {
			validationResult = new ValidationResult();
			validationResult.setStatus(ValidationResult.Result.ERROR);
			validationResult.setMessage(messages.getMessage("error.ContainerNameWasNotSet"));
		}
		return validationResult;
	}

	@Override
	public void runAtDriver(RuntimeContainer container) {
		createAzureContainer(container);
		setReturnValues(container);
	}

	private void createAzureContainer(RuntimeContainer container) {
		try {
			boolean result;
			CloudBlobContainer azureContainer = getStorageContainerReference(container, containerName);
			try {
				result = azureContainer.createIfNotExists();
			} catch (StorageException e) {
				if (!e.getErrorCode().equals(StorageErrorCodeStrings.CONTAINER_BEING_DELETED)) {
					throw e;
				}
				LOGGER.error(messages.getMessage("error.CONTAINER_BEING_DELETED", containerName));
				// wait 40 seconds (min is 30s) before retrying.
				// See
				// https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/delete-container
				try {
					Thread.sleep(40000);
				} catch (InterruptedException eint) {
					LOGGER.error(messages.getMessage("error.InterruptedException"));
					throw new ComponentException(eint);
				}
				result = azureContainer.createIfNotExists();
				LOGGER.debug(messages.getMessage("debug.ContainerCreated", containerName));
			}
			// Manage accessControl
			if (access.equals("Public") && result) {
				// Create a permissions object.
				BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
				// Include public access in the permissions object.
				containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
				// Set the permissions on the container.
				azureContainer.uploadPermissions(containerPermissions);
			}
			if (!result) {
				LOGGER.warn(messages.getMessage("warn.ContainerExists", containerName));
			}
		} catch (StorageException | InvalidKeyException | URISyntaxException e) {
			LOGGER.error(e.getLocalizedMessage());
			if (dieOnError)
				throw new ComponentException(e);
		}
	}

	private void setReturnValues(RuntimeContainer container) {
		String componentId = container.getCurrentComponentId();
		container.setComponentData(componentId, AzureStorageContainerDefinition.RETURN_CONTAINER.toUpperCase(), containerName);
	}

}
