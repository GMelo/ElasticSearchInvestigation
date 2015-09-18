package org.gmelo.investigation.model;

public class Telephone {

    private final String countryCode;
    private final String PhoneNumber;

    public Telephone(String countryCode, String phoneNumber) {
        this.countryCode = countryCode;
        PhoneNumber = phoneNumber;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public String getPhoneNumber() {
        return PhoneNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Telephone telephone = (Telephone) o;

        if (PhoneNumber != null ? !PhoneNumber.equals(telephone.PhoneNumber) : telephone.PhoneNumber != null)
            return false;
        if (countryCode != null ? !countryCode.equals(telephone.countryCode) : telephone.countryCode != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = countryCode != null ? countryCode.hashCode() : 0;
        result = 31 * result + (PhoneNumber != null ? PhoneNumber.hashCode() : 0);
        return result;
    }
}
