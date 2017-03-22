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
package org.talend.components.azurestorage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.talend.components.api.container.RuntimeContainer;

public class RuntimeContainerMock implements RuntimeContainer {

    private Map<String, Object> map = new HashMap<String, Object>();

    @Override
    public Object getComponentData(String componentId, String key) {

        return map.get(componentId + key);
    }

    @Override
    public void setComponentData(String componentId, String key, Object data) {
        map.put(componentId + key, data);

    }

    @Override
    public String getCurrentComponentId() {

        return "component_" + UUID.randomUUID().toString().replace("-", "").substring(0, 3).toLowerCase();
    }

    @Override
    public Object getGlobalData(String key) {

        return map.get(key);
    }

}
