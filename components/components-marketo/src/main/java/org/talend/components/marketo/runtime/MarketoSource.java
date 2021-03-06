package org.talend.components.marketo.runtime;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.BoundedReader;
import org.talend.components.api.component.runtime.BoundedSource;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.marketo.MarketoConstants;
import org.talend.components.marketo.tmarketoconnection.TMarketoConnectionProperties.APIMode;
import org.talend.components.marketo.tmarketoinput.TMarketoInputProperties;
import org.talend.components.marketo.tmarketoinput.TMarketoInputProperties.CustomObjectAction;
import org.talend.components.marketo.tmarketoinput.TMarketoInputProperties.InputOperation;
import org.talend.components.marketo.tmarketoinput.TMarketoInputProperties.LeadSelector;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.ValidationResult.Result;

public class MarketoSource extends MarketoSourceOrSink implements BoundedSource {

    private transient static final Logger LOG = LoggerFactory.getLogger(MarketoSource.class);

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider().getI18nMessages(MarketoSource.class);

    public MarketoSource() {
    }

    @Override
    public List<? extends BoundedSource> splitIntoBundles(long desiredBundleSizeBytes, RuntimeContainer adaptor)
            throws Exception {
        List<BoundedSource> list = new ArrayList<>();
        list.add(this);
        return list;
    }

    @Override
    public long getEstimatedSizeBytes(RuntimeContainer adaptor) {
        return 0;
    }

    @Override
    public boolean producesSortedKeys(RuntimeContainer adaptor) {
        return false;
    }

    public boolean isInvalidDate(String datetime) {
        try {
            Date dt = new SimpleDateFormat(MarketoConstants.DATETIME_PATTERN_PARAM).parse(datetime);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    @Override
    public ValidationResult validate(RuntimeContainer container) {
        ValidationResult vr = super.validate(container);
        if (vr.getStatus().equals(Result.ERROR)) {
            return vr;
        }
        if (properties instanceof TMarketoInputProperties) {
            TMarketoInputProperties p = (TMarketoInputProperties) properties;
            boolean useSOAP = properties.getConnectionProperties().apiMode.getValue().equals(APIMode.SOAP);
            // Validate dynamic schema if needed
            Boolean isDynamic = AvroUtils.isIncludeAllFields(p.schemaInput.schema.getValue());
            if (useSOAP) { // no dynamic schema for SOAP !
                if (isDynamic) {
                    vr.setStatus(Result.ERROR);
                    vr.setMessage(messages.getMessage("error.validation.soap.dynamicschema"));
                    return vr;
                }
            }
            ////////////
            // Leads
            ////////////
            // getLead
            if (p.inputOperation.getValue().equals(InputOperation.getLead)) {
                if (p.leadKeyValue.getValue().isEmpty()) {
                    vr.setStatus(Result.ERROR);
                    vr.setMessage(messages.getMessage("error.validation.leadkeyvalue"));
                    return vr;
                }
            }
            // getMultipleLeads
            if (p.inputOperation.getValue().equals(InputOperation.getMultipleLeads)) {
                LeadSelector sel;
                if (useSOAP) {
                    sel = p.leadSelectorSOAP.getValue();
                } else {
                    sel = p.leadSelectorREST.getValue();
                }
                switch (sel) {
                case LeadKeySelector:
                    if (p.leadKeyValues.getValue().isEmpty()) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.leadkeyvalues"));
                        return vr;
                    }
                    break;
                case StaticListSelector:
                    if (p.listParamValue.getValue().isEmpty()) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.listparamvalue"));
                        return vr;
                    }
                    break;
                case LastUpdateAtSelector:
                    if (p.oldestUpdateDate.getValue().isEmpty() || p.latestUpdateDate.getValue().isEmpty()
                            || isInvalidDate(p.oldestUpdateDate.getValue()) || isInvalidDate(p.latestUpdateDate.getValue())) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.updatedates"));
                        return vr;
                    }
                    break;
                }
            }
            // getLeadActivity
            if (p.inputOperation.getValue().equals(InputOperation.getLeadActivity)) {
                if (isDynamic) {
                    vr.setStatus(Result.ERROR);
                    vr.setMessage(messages.getMessage("error.validation.operation.dynamicschema"));
                    return vr;
                }
                if (useSOAP) {
                    if (p.leadKeyValue.getValue().isEmpty()) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.leadkeyvalue"));
                        return vr;
                    }
                } else {
                    if (p.sinceDateTime.getValue().isEmpty() || isInvalidDate(p.sinceDateTime.getValue())) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.sincedatetime"));
                        return vr;
                    }
                }
            }
            // getLeadChanges
            if (p.inputOperation.getValue().equals(InputOperation.getLeadChanges)) {
                if (isDynamic) {
                    vr.setStatus(Result.ERROR);
                    vr.setMessage(messages.getMessage("error.validation.operation.dynamicschema"));
                    return vr;
                }
                if (useSOAP) {
                    if (p.oldestCreateDate.getValue().isEmpty() || p.latestCreateDate.getValue().isEmpty()
                            || isInvalidDate(p.oldestCreateDate.getValue()) || isInvalidDate(p.latestCreateDate.getValue())) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.createdates"));
                        return vr;
                    }
                } else {
                    if (p.sinceDateTime.getValue().isEmpty() || isInvalidDate(p.sinceDateTime.getValue())) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.sincedatetime"));
                        return vr;
                    }
                }
            }
            /////////////////////
            // Custom Objects
            /////////////////////
            if (p.inputOperation.getValue().equals(InputOperation.CustomObject)) {
                // get Action
                if (p.customObjectAction.getValue().equals(CustomObjectAction.get)) {
                    if (p.customObjectName.getValue().isEmpty()) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.customobject.customobjectname"));
                        return vr;
                    }
                    // filterType & filterValue
                    if (p.customObjectFilterType.getValue().isEmpty()) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.customobject.filtertype"));
                        return vr;
                    }
                    if (p.customObjectFilterValues.getValue().isEmpty()) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.customobject.filtervalues"));
                        return vr;
                    }
                }
                // list no checking...
                if (p.customObjectAction.getValue().equals(CustomObjectAction.list)) {
                    if (isDynamic) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.operation.dynamicschema"));
                        return vr;
                    }
                }
                // describe
                if (p.customObjectAction.getValue().equals(CustomObjectAction.describe)) {
                    if (isDynamic) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.operation.dynamicschema"));
                        return vr;
                    }
                    if (p.customObjectName.getValue().isEmpty()) {
                        vr.setStatus(Result.ERROR);
                        vr.setMessage(messages.getMessage("error.validation.customobject.customobjectname"));
                        return vr;
                    }
                }
            }
        }
        return vr;
    }

    @Override
    public BoundedReader createReader(RuntimeContainer adaptor) {
        return new MarketoInputReader(adaptor, this, (TMarketoInputProperties) properties);
    }

}
