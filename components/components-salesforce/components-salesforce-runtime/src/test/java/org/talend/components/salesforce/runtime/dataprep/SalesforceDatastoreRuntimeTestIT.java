package org.talend.components.salesforce.runtime.dataprep;

import org.junit.Assert;
import org.junit.Test;
import org.talend.components.salesforce.datastore.SalesforceDatastoreProperties;
import org.talend.daikon.properties.ValidationResult;

public class SalesforceDatastoreRuntimeTestIT {

    @Test
    public void testDoHealthChecksWithSuccess() {
        SalesforceDatastoreProperties datastore = new SalesforceDatastoreProperties("datastore");
        CommonTestUtils.setValueForDatastoreProperties(datastore);

        SalesforceDatastoreRuntime runtime = new SalesforceDatastoreRuntime();
        runtime.initialize(null, datastore);
        Iterable<ValidationResult> results = runtime.doHealthChecks(null);

        Assert.assertNotNull(results);
        for (ValidationResult result : results) {
            Assert.assertTrue(result.getMessage(), result.getStatus() == ValidationResult.Result.OK);
        }
    }
    
    @Test
    public void testDoHealthChecksWithFail() {
        SalesforceDatastoreProperties datastore = new SalesforceDatastoreProperties("datastore");
        CommonTestUtils.setValueForDatastoreProperties(datastore);
        datastore.password.setValue("wrongone");

        SalesforceDatastoreRuntime runtime = new SalesforceDatastoreRuntime();
        runtime.initialize(null, datastore);
        Iterable<ValidationResult> results = runtime.doHealthChecks(null);

        Assert.assertNotNull(results);
        for (ValidationResult result : results) {
            Assert.assertTrue(result.getMessage(), result.getStatus() == ValidationResult.Result.ERROR);
            Assert.assertNotNull(result.getMessage());
        }
    }

}
