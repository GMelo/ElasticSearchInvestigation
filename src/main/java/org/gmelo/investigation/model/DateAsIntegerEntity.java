package org.gmelo.investigation.model;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Created by gmelo on 01/06/16.
 * DateAsIntegerEntity
 */
public class DateAsIntegerEntity {

    private final String localDateRepresentation;
    private final String name;

    public DateAsIntegerEntity(String name,LocalDate localdate) {
        this.name = name;
        this.localDateRepresentation = localdate.format(DateTimeFormatter.BASIC_ISO_DATE);
    }

    public String getLocalDateRepresentation() {
        return localDateRepresentation;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateAsIntegerEntity that = (DateAsIntegerEntity) o;

        if (!localDateRepresentation.equals(that.localDateRepresentation)) return false;
        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = localDateRepresentation.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DateAsIntegerEntity{" +
                "localDateRepresentation='" + localDateRepresentation + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
