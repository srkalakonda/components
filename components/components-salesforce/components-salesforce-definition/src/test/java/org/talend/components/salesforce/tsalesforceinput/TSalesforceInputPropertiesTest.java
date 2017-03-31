package org.talend.components.salesforce.tsalesforceinput;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.salesforce.SalesforceModuleProperties;
import org.talend.daikon.properties.ValidationResult;

/**
 * Unit-tests for {@link TSalesforceInputProperties} class
 */
public class TSalesforceInputPropertiesTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TSalesforceInputPropertiesTest.class);

    private static final String SELECT = "SELECT";

    private static final String SPACE = " ";

    private static final String COMMA = ",";

    private static final String QUOTES = "\"";

    private static final String FROM = "FROM";

    private TSalesforceInputProperties tSalesforceInputProperties;

    @Before
    public void setUp() throws Exception {
        tSalesforceInputProperties = new TSalesforceInputProperties("name");
    }

    /**
     * Checks {@link TSalesforceInputProperties#guessQuery} returns correct soql-query
     */
    @Test
    public void testValidateGuessQuery() throws Exception {
        final String field1 = "Id";
        final String field2 = "Name";
        final String moduleName = "Module";

        String expectedQuery = new StringBuilder()
                .append(QUOTES).append(SELECT).append(SPACE)
                .append(field1).append(COMMA).append(SPACE)
                .append(field2).append(SPACE)
                .append(FROM).append(SPACE).append(moduleName).append(QUOTES).toString();

        Schema schema = SchemaBuilder.record("Result").fields()
                .name(field1).type().stringType().noDefault()
                .name(field2).type().stringType().noDefault()
                .endRecord();

        SalesforceModuleProperties properties = new SalesforceModuleProperties("properties");
        properties.moduleName.setValue(moduleName);
        properties.main.schema.setValue(schema);

        tSalesforceInputProperties.module = properties;

        ValidationResult.Result resultStatus = tSalesforceInputProperties.validateGuessQuery().status;
        String expectedMessage = tSalesforceInputProperties.validateGuessQuery().getMessage();

        LOGGER.debug("validation result status" + resultStatus);
        Assert.assertEquals(ValidationResult.Result.OK, resultStatus);
        Assert.assertNotEquals(ValidationResult.Result.ERROR, resultStatus);
        Assert.assertNotEquals(ValidationResult.Result.WARNING, resultStatus);

        String resultQuery = tSalesforceInputProperties.query.getValue();
        LOGGER.debug("result query " + resultQuery);
        Assert.assertNotNull(resultQuery);
        Assert.assertNull(expectedMessage);
        Assert.assertEquals(expectedQuery, resultQuery);

    }

    /**
     * Checks {@link TSalesforceInputProperties#guessQuery} returns empty {@link java.lang.String}
     * when schema does not include any fields
     */
    @Test
    public void testValidateGuessQueryEmptySchema() throws Exception {
        final String field1 = "Id";
        final String field2 = "Name";
        final String moduleName = "Module";

        String expectedQuery = "";

        Schema schema = SchemaBuilder.record("Result").fields()
                .endRecord();

        SalesforceModuleProperties properties = new SalesforceModuleProperties("properties");
        properties.moduleName.setValue(moduleName);
        properties.main.schema.setValue(schema);

        tSalesforceInputProperties.module = properties;

        ValidationResult.Result resultStatus = tSalesforceInputProperties.validateGuessQuery().status;
        String expectedMessage = tSalesforceInputProperties.validateGuessQuery().getMessage();

        LOGGER.debug("validation result status" + resultStatus);
        Assert.assertEquals(ValidationResult.Result.ERROR, resultStatus);
        Assert.assertEquals(expectedMessage, "Schema does not contain any field. Query cannot be guessed");
        Assert.assertNotNull(expectedMessage);
        Assert.assertNotEquals(ValidationResult.Result.OK, resultStatus);
        Assert.assertNotEquals(ValidationResult.Result.WARNING, resultStatus);

        String resultQuery = tSalesforceInputProperties.query.getValue();
        LOGGER.debug("result query " + resultQuery);
        Assert.assertNotNull(resultQuery);
        Assert.assertEquals(expectedQuery, resultQuery);
    }
}
