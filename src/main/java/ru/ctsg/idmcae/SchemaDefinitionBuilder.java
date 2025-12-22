package ru.ctsg.idmcae;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.Uid;

public class SchemaDefinitionBuilder {

    private final Set<String> attributeSetAccount = new HashSet<>();
    private final Set<String> attributeSetPermission = new HashSet<>();
    private final Set<String> multiValueAttributes = new HashSet<>();
    private final Set<String> booleanAttributes = new HashSet<>();

    public SchemaDefinitionBuilder(){

        attributeSetAccount.addAll(List.of(
        "account_id", 
        "username", 
        "full_name", 
        "email", 
        "memberOf",
        "is_active", 
        "created_at", 
        "last_modified_at"
        ));

        attributeSetPermission.addAll(List.of(
            "permission_uid", 
            "code", 
            "display_name", 
            "members", 
            "category", 
            "created_at"
        ));

        multiValueAttributes.addAll(List.of(
            "members", 
            "memberOf"
        ));

        booleanAttributes.add("is_active");

    }


    public Schema buildSchema() {

        SchemaBuilder schemaBuilder = new SchemaBuilder(ADLKConnector.class);

        // User object class
        ObjectClassInfoBuilder accountBuilder = new ObjectClassInfoBuilder();
        accountBuilder.setType(ObjectClass.ACCOUNT_NAME);
        addConnIdCoreAttrs(accountBuilder);
        addBusinessAttrs(accountBuilder, attributeSetAccount);
        schemaBuilder.defineObjectClass(accountBuilder.build());

        // Permission object class
        ObjectClassInfoBuilder permissionBuilder = new ObjectClassInfoBuilder();
        permissionBuilder.setType("Permission");
        addConnIdCoreAttrs(permissionBuilder);
        addBusinessAttrs(permissionBuilder, attributeSetPermission);
        schemaBuilder.defineObjectClass(permissionBuilder.build());

        return schemaBuilder.build();
    }

    private void addConnIdCoreAttrs(ObjectClassInfoBuilder builder) {
        // __UID__
        builder.addAttributeInfo(
                AttributeInfoBuilder.define(Uid.NAME)
                        .setType(String.class)
                        .setRequired(false)
                        .setCreateable(false)
                        .setUpdateable(false)
                        .setReadable(true)
                        .build()
        );

        // __NAME__
        builder.addAttributeInfo(
                AttributeInfoBuilder.define(Name.NAME)
                        .setType(String.class)
                        .setRequired(true)
                        .setCreateable(true)
                        .setUpdateable(false)
                        .setReadable(true)
                        .build()
        );
    }

    private void addBusinessAttrs(ObjectClassInfoBuilder builder, Set<String>  attrs) {
    for (String attr : attrs) {
            Class<?> type = booleanAttributes.contains(attr) ? Boolean.class : String.class;
            boolean isMulti = multiValueAttributes.contains(attr);

            builder.addAttributeInfo(
                    AttributeInfoBuilder.define(attr)
                            .setType(type)
                            .setMultiValued(isMulti)
                            .build()
            );
        }
    }


}
