package org.gmelo.investigation.model;

public class Address {

    private final String number;
    private final String streetName;
    private final String complement;
    private final String city;
    private final String country;

    public Address(String number, String streetName, String complement, String city, String country) {
        this.number = number;
        this.streetName = streetName;
        this.complement = complement;
        this.city = city;
        this.country = country;
    }

    public String getNumber() {
        return number;
    }

    public String getStreetName() {
        return streetName;
    }

    public String getComplement() {
        return complement;
    }

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Address address = (Address) o;

        if (city != null ? !city.equals(address.city) : address.city != null) return false;
        if (complement != null ? !complement.equals(address.complement) : address.complement != null) return false;
        if (country != null ? !country.equals(address.country) : address.country != null) return false;
        if (number != null ? !number.equals(address.number) : address.number != null) return false;
        if (streetName != null ? !streetName.equals(address.streetName) : address.streetName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = number != null ? number.hashCode() : 0;
        result = 31 * result + (streetName != null ? streetName.hashCode() : 0);
        result = 31 * result + (complement != null ? complement.hashCode() : 0);
        result = 31 * result + (city != null ? city.hashCode() : 0);
        result = 31 * result + (country != null ? country.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Address{" +
                "number='" + number + '\'' +
                ", streetName='" + streetName + '\'' +
                ", complement='" + complement + '\'' +
                ", city='" + city + '\'' +
                ", country='" + country + '\'' +
                '}';
    }
}
