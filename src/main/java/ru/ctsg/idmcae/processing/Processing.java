package ru.ctsg.idmcae.processing;

import java.util.Set;

import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;

public class Processing {
    
    protected boolean getBool(Set<Attribute> createAttributes, String attrIsActive, boolean b) {
        Attribute attribute = AttributeUtil.find(attrIsActive, createAttributes);
        return (boolean) attribute.getValue().get(0);
    }

    protected String getString(Set<Attribute> createAttributes, String attrName) {
        Attribute attribute = AttributeUtil.find(attrName, createAttributes);
        return (String) attribute.getValue().get(0);
    }
    
}
