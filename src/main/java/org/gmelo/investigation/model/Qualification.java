package org.gmelo.investigation.model;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class Qualification {

    private LocalDate date;
    private String name;
    private Type type;
    private String institution;
    private String field;

    private final Map<String, String> linkingKeys = new HashMap<>(1);

    public enum Type {
        ACADEMIC, PROFESSIONAL
    }

    public Map<String, String> getLinkingKeys() {
        return linkingKeys;
    }

    public void addLinkingKeys(String key, String value) {
        linkingKeys.put(key, value);
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getInstitution() {
        return institution;
    }

    public void setInstitution(String institution) {
        this.institution = institution;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

}
