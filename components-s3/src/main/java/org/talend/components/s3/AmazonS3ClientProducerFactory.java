// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.s3;

import java.io.IOException;

import org.talend.components.s3.AbstractAmazonS3ClientProducer.AsymmetricKeyEncryptionAmazonS3ClientProvider;
import org.talend.components.s3.AbstractAmazonS3ClientProducer.KmsCmkEncryptionAmazonS3ClientProducer;
import org.talend.components.s3.AbstractAmazonS3ClientProducer.NonEncryptedAmazonS3ClientProducer;
import org.talend.components.s3.AbstractAmazonS3ClientProducer.SymmetricKeyEncryptionAmazonS3ClientProvider;;

/**
 * created by dmytro.chmyga on Jul 26, 2016
 */
public class AmazonS3ClientProducerFactory {

    public static AbstractAmazonS3ClientProducer createClientProducer(AwsS3ConnectionProperties connectionProperties)
            throws IOException {
        if (!connectionProperties.encrypt.getValue()) {
            return new NonEncryptedAmazonS3ClientProducer();
        } else {
            EncryptionKeyType keyType = connectionProperties.encryptionProperties.encryptionKeyType.getValue();
            switch (keyType) {
            case ASYMMETRIC_MASTER_KEY:
                return new AsymmetricKeyEncryptionAmazonS3ClientProvider();
            case KMS_MANAGED_CUSTOMER_MASTER_KEY:
                return new KmsCmkEncryptionAmazonS3ClientProducer();
            case SYMMETRIC_MASTER_KEY:
                return new SymmetricKeyEncryptionAmazonS3ClientProvider();
            default:
                throw new IOException("Unexpected encryption key type value: " + keyType);
            }
        }
    }

}