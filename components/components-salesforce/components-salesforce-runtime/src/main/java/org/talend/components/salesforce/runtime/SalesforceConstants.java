package org.talend.components.salesforce.runtime;

import org.apache.avro.Schema;
import org.talend.daikon.avro.AvroUtils;

final public class SalesforceConstants {

    public static final String SALESFORCE_DEFAULT_DATE_PATTERN = "yyyy-MM-dd";

    public static final String REUSE_SESSION_FAILS = "Reuse session fails!";

    public static final Schema DEFAULT_GUESS_SCHEMA_TYPE = AvroUtils._string();

    public static final int DEFAULT_LENGTH = 255;

    public static final int DEFAULT_PRECISION = 15;

    private SalesforceConstants() {
        throw new AssertionError();
    }
}
