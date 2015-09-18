package org.gmelo.investigation.model;

import org.gmelo.investigation.es.IndexConstants;
import org.gmelo.investigation.es.creation.ElasticSearchService;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class Customer {

    private final String id;
    private final String firstName;
    private final String lastName;
    private final String title;
    private final String occupation;
    private final String email;
    private final Set<String> interestSet;
    private final Set<Address> addressSet;
    private final Set<Telephone> telephoneSet;


    public Customer(String id, String firstName, String lastName, String title, String occupation, String email, Set<String> interestSet, Set<Address> addressSet, Set<Telephone> telephoneSet) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.title = title;
        this.occupation = occupation;
        this.email = email;
        this.interestSet = interestSet;
        this.addressSet = addressSet;
        this.telephoneSet = telephoneSet;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getTitle() {
        return title;
    }

    public Set<String> getInterestSet() {
        return interestSet;
    }

    public Set<Address> getAddressSet() {
        return addressSet;
    }

    public Set<Telephone> getTelephoneSet() {
        return telephoneSet;
    }

    public String getOccupation() {
        return occupation;
    }

    public String getEmail() {
        return email;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Customer customer = (Customer) o;

        if (addressSet != null ? !addressSet.equals(customer.addressSet) : customer.addressSet != null) return false;
        if (firstName != null ? !firstName.equals(customer.firstName) : customer.firstName != null) return false;
        if (interestSet != null ? !interestSet.equals(customer.interestSet) : customer.interestSet != null)
            return false;
        if (lastName != null ? !lastName.equals(customer.lastName) : customer.lastName != null) return false;
        if (occupation != null ? !occupation.equals(customer.occupation) : customer.occupation != null) return false;
        if (telephoneSet != null ? !telephoneSet.equals(customer.telephoneSet) : customer.telephoneSet != null)
            return false;
        if (title != null ? !title.equals(customer.title) : customer.title != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = firstName != null ? firstName.hashCode() : 0;
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (occupation != null ? occupation.hashCode() : 0);
        result = 31 * result + (interestSet != null ? interestSet.hashCode() : 0);
        result = 31 * result + (addressSet != null ? addressSet.hashCode() : 0);
        result = 31 * result + (telephoneSet != null ? telephoneSet.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", title='" + title + '\'' +
                ", occupation='" + occupation + '\'' +
                ", interestSet=" + interestSet +
                ", addressSet=" + addressSet +
                ", telephoneSet=" + telephoneSet +
                '}';
    }

    public static String indexProperties() {
        String mapping = null;
        try {
            mapping = jsonBuilder()
                    .startObject()
                    .startObject(ElasticSearchService.Index.customer.name())
                    .startObject("properties")
                            //sort fields
                            //name
                    .startObject(IndexConstants.FIRST_NAME_FIELD)
                    .field("type").value("string")
                    .startObject("fields")
                    .startObject("raw")
                    .field("type").value("string")
                    .field("index").value("not_analyzed")
                    .endObject()
                    .endObject()
                    .endObject()
                            //lastName
                    .startObject(IndexConstants.LAST_NAME_FIELD)
                    .field("type").value("string")
                    .startObject("fields")
                    .startObject("raw")
                    .field("type").value("string")
                    .field("index").value("not_analyzed")
                    .endObject()
                    .endObject()
                    .endObject()
                            //email
                    .startObject(IndexConstants.EMAIL_FIELD)
                    .field("type").value("string")
                    .startObject("fields")
                    .startObject("raw")
                    .field("type").value("string")
                    .field("index").value("not_analyzed")
                    .endObject()
                    .endObject()
                    .endObject()
                            //title
                    .startObject(IndexConstants.TITLE_FIELD)
                    .field("type").value("string")
                    .startObject("fields")
                    .startObject("raw")
                    .field("type").value("string")
                    .field("index").value("not_analyzed")
                    .endObject()
                    .endObject()
                    .endObject()

                            //end sort fields
                    .endObject()
                    .endObject()
                    .endObject()
                    .string();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return mapping;
    }
}
