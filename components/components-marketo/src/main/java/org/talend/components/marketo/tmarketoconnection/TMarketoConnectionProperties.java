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
package org.talend.components.marketo.tmarketoconnection;

import static org.talend.daikon.properties.presentation.Widget.widget;
import static org.talend.daikon.properties.property.PropertyFactory.newEnum;
import static org.talend.daikon.properties.property.PropertyFactory.newInteger;
import static org.talend.daikon.properties.property.PropertyFactory.newString;

import java.util.EnumSet;

import org.talend.components.api.properties.ComponentPropertiesImpl;
import org.talend.components.api.properties.ComponentReferenceProperties;
import org.talend.components.marketo.MarketoProvideConnectionProperties;
import org.talend.components.marketo.runtime.MarketoSource;
import org.talend.components.marketo.runtime.MarketoSourceOrSink;
import org.talend.daikon.properties.PresentationItem;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.service.Repository;

public class TMarketoConnectionProperties extends ComponentPropertiesImpl implements MarketoProvideConnectionProperties {

    public enum APIMode {
        REST,
        SOAP
    }

    public Property<APIMode> apiMode = newEnum("apiMode", APIMode.class);

    public static final String FORM_WIZARD = "Wizard";

    public Property<String> name = newString("name").setRequired();

    //
    public PresentationItem testConnection = new PresentationItem("testConnection", "Test connection");

    public Property<String> endpoint = newString("endpoint").setRequired();

    public Property<String> secretKey = newString("secretKey").setRequired()
            .setFlags(EnumSet.of(Property.Flags.ENCRYPT, Property.Flags.SUPPRESS_LOGGING));

    public Property<String> clientAccessId = newString("clientAccessId").setRequired();

    // advanced
    public Property<Integer> timeout = newInteger("timeout");

    public Property<Integer> attemptsIntervalTime = newInteger("attemptsIntervalTime");

    public Property<Integer> maxReconnAttemps = newInteger("maxReconnAttemps");

    public ComponentReferenceProperties<TMarketoConnectionProperties> referencedComponent = new ComponentReferenceProperties<>(
            "referencedComponent", TMarketoConnectionDefinition.COMPONENT_NAME);

    private String repositoryLocation;

    //
    private static final long serialVersionUID = 145738798798151L;

    public TMarketoConnectionProperties(String name) {
        super(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setupProperties() {
        super.setupProperties();

        endpoint.setValue("");
        secretKey.setValue("");
        clientAccessId.setValue("");

        apiMode.setPossibleValues((Object[]) APIMode.values());
        apiMode.setValue(APIMode.REST);

        timeout.setValue(60000);
        maxReconnAttemps.setValue(5);
        attemptsIntervalTime.setValue(1000);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form mainForm = Form.create(this, Form.MAIN);
        mainForm.addRow(endpoint);
        mainForm.addRow(clientAccessId);
        mainForm.addColumn(widget(secretKey).setWidgetType(Widget.HIDDEN_TEXT_WIDGET_TYPE));

        // Advanced
        Form advancedForm = Form.create(this, Form.ADVANCED);
        advancedForm.addRow(apiMode);
        advancedForm.addColumn(timeout);
        advancedForm.addRow(maxReconnAttemps);
        advancedForm.addColumn(attemptsIntervalTime);

        // A form for a reference to a connection
        Form refForm = Form.create(this, Form.REFERENCE);
        Widget compListWidget = widget(referencedComponent).setWidgetType(Widget.COMPONENT_REFERENCE_WIDGET_TYPE);
        refForm.addRow(compListWidget);
        refForm.addRow(mainForm);

        // Wizard
        Form wizardForm = Form.create(this, FORM_WIZARD);
        wizardForm.addRow(name);
        wizardForm.addRow(endpoint);
        wizardForm.addRow(clientAccessId);
        wizardForm.addRow(widget(secretKey).setWidgetType(Widget.HIDDEN_TEXT_WIDGET_TYPE));
        wizardForm.addRow(widget(testConnection).setLongRunning(true).setWidgetType(Widget.BUTTON_WIDGET_TYPE));
        wizardForm.addRow(timeout);
        wizardForm.addRow(maxReconnAttemps);
        wizardForm.addRow(attemptsIntervalTime);
    }

    @Override
    public void refreshLayout(Form form) {
        super.refreshLayout(form);

        String refComponentIdValue = getReferencedComponentId();
        boolean useOtherConnection = refComponentIdValue != null
                && refComponentIdValue.startsWith(TMarketoConnectionDefinition.COMPONENT_NAME);

        if (form.getName().equals(Form.MAIN) || form.getName().equals(FORM_WIZARD)) {
            form.getWidget(endpoint.getName()).setHidden(useOtherConnection);
            form.getWidget(clientAccessId.getName()).setHidden(useOtherConnection);
            form.getWidget(secretKey.getName()).setHidden(useOtherConnection);
            //
        }
        if (form.getName().equals(Form.ADVANCED)) {
            form.getWidget(apiMode.getName()).setHidden(useOtherConnection);
            form.getWidget(timeout.getName()).setHidden(useOtherConnection);
            form.getWidget(maxReconnAttemps.getName()).setHidden(useOtherConnection);
            form.getWidget(attemptsIntervalTime.getName()).setHidden(useOtherConnection);
        }
    }

    public void afterReferencedComponent() {
        refreshLayout(getForm(Form.MAIN));
        refreshLayout(getForm(Form.ADVANCED));
        refreshLayout(getForm(Form.REFERENCE));
    }

    public void afterApiMode() {
        refreshLayout(getForm(Form.MAIN));
        refreshLayout(getForm(Form.ADVANCED));
        refreshLayout(getForm(Form.REFERENCE));
    }

    public ValidationResult validateTestConnection() {
        ValidationResult vr = MarketoSource.validateConnection(this);
        if (vr.getStatus() == ValidationResult.Result.OK) {
            getForm(FORM_WIZARD).setAllowForward(true);
            getForm(FORM_WIZARD).setAllowFinish(true);
        } else {
            getForm(FORM_WIZARD).setAllowForward(false);
        }
        return vr;
    }

    public ValidationResult afterFormFinishWizard(Repository<Properties> repo) {

        ValidationResult vr = MarketoSourceOrSink.validateConnection(this);
        if (vr.getStatus() != ValidationResult.Result.OK) {
            return vr;
        }
        repo.storeProperties(this, this.name.getValue(), repositoryLocation, null);
        return ValidationResult.OK;
    }

    @Override
    public TMarketoConnectionProperties getConnectionProperties() {
        return this;
    }

    public String getReferencedComponentId() {
        return referencedComponent.componentInstanceId.getValue();
    }

    public TMarketoConnectionProperties getReferencedConnectionProperties() {
        TMarketoConnectionProperties refProps = referencedComponent.getReference();
        if (refProps != null) {
            return refProps;
        }
        return null;
    }

    public TMarketoConnectionProperties setRepositoryLocation(String repositoryLocation) {
        this.repositoryLocation = repositoryLocation;
        return this;
    }

}
