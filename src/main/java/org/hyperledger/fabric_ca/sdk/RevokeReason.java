package org.hyperledger.fabric_ca.sdk;

public enum RevokeReason {

    UNSPECIFIED,
    KEY_COMPROMISE,
    CA_COMPROMISE,
    AFFILIATION_CHANGED,
    SUPERSEDED,
    CESSATION_OF_OPERATION,
    CERTIFICATE_HOLD,
    // value 7 is not used
    REMOVE_FROM_CRL,
    PRIVILEGE_WITHDRAWN,
    AA_COMPROMISE;

    @Override
    public String toString(){
        return this.name();
    }

}