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
package org.talend.components.api.test.junit;

import java.lang.reflect.Field;
import java.util.List;

import org.databene.contiperf.junit.ContiPerfRule;
import org.junit.internal.runners.statements.RunAfters;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * The purpose of this class is to create adapters for ContiPerf library, so it would be able to work with JUnit 4.12
 * and higher. In JUnit 4.12 "fNext" field name in {@link RunBefores} and {@link RunAfters} was changed to "next". Due
 * to that, ContiPerf is unable to work with JUnit 4.12 or higher, as the Reflection in {@link ContiPerfRule} uses
 * "fNext" field name.
 * 
 * </br>
 * For that purpose, we've created some adapters, which include fields with name "fNext".
 */
public class ContiPerfRuleAdaptor extends ContiPerfRule {

    /**
     * The purpose of this method is to recreate statement with JUnit 4.12 and higher adapters.
     */
    private Statement recreateStatementWithAdapters(Statement base) {
        RunBefores runBefores = null;
        RunAfters runAfters = null;
        while (base instanceof RunAfters || base instanceof RunBefores) {
            try {
                Field next = null;
                if (base instanceof RunAfters) {
                    runAfters = (RunAfters) base;
                    next = base.getClass().getDeclaredField("next");
                } else if (base instanceof RunBefores) {
                    runBefores = (RunBefores) base;
                    next = base.getClass().getDeclaredField("next");
                }
                if (next != null) {
                    next.setAccessible(true);
                    base = (Statement) next.get(base);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        try {
            // if runBefores has been removed, reapply it
            if (runBefores != null) {
                runBefores = recreateFromRunBefores(runBefores);
                Field fNext = runBefores.getClass().getDeclaredField("fNext");
                fNext.setAccessible(true);
                fNext.set(runBefores, base);
                base = runBefores;
            }
            // if runAfters has been removed, reapply it
            if (runAfters != null) {
                runAfters = recreateFromRunAfters(runAfters);
                Field fNext = runAfters.getClass().getDeclaredField("fNext");
                fNext.setAccessible(true);
                fNext.set(runAfters, base);
                base = runAfters;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return base;
    }

    private RunBefores recreateFromRunBefores(RunBefores runBefores)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        return new RunBeforesContiPerfAdapter(getObjectByField(runBefores, runBefores.getClass(), "next", Statement.class),
                getObjectByField(runBefores, runBefores.getClass(), "befores", List.class),
                getObjectByField(runBefores, runBefores.getClass(), "target", Object.class));
    }

    private RunAfters recreateFromRunAfters(RunAfters runAfters)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        return new RunAftersContiPerfAdapter(getObjectByField(runAfters, runAfters.getClass(), "next", Statement.class),
                getObjectByField(runAfters, runAfters.getClass(), "afters", List.class),
                getObjectByField(runAfters, runAfters.getClass(), "target", Object.class));
    }

    /**
     * Method is used to retrieve the field values using Reflection.
     */
    private <T> T getObjectByField(Object delegate, Class<?> delegateClass, String fieldName, Class<T> c)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Field f = delegateClass.getDeclaredField(fieldName);
        f.setAccessible(true);
        T object = (T) f.get(delegate);
        return object;
    }

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        return super.apply(recreateStatementWithAdapters(base), method, target);
    }

}
