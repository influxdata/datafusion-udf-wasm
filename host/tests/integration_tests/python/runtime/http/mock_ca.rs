use rcgen::DnValue::PrintableString;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

pub(crate) fn new_ca() -> (Certificate, Issuer<'static, KeyPair>) {
    let mut params =
        CertificateParams::new(Vec::default()).expect("empty subject alt name can't produce error");
    let (yesterday, tomorrow) = validity_period();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(
        DnType::CountryName,
        PrintableString("BR".try_into().unwrap()),
    );
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Test CA");
    // Unique CommonName per CA so a client trust store containing one test CA does not match the
    // issuer DN of certs signed by a different test CA. Without this, webpki picks the wrong trust
    // anchor by name and reports `BadSignature` instead of `UnknownIssuer`, which TLS surfaces as
    // a `decrypt_error` alert rather than `unknown_ca`.
    params
        .distinguished_name
        .push(DnType::CommonName, Uuid::new_v4().to_string());
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    params.key_usages.push(KeyUsagePurpose::CrlSign);

    params.not_before = yesterday;
    params.not_after = tomorrow;

    let key_pair = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    (cert, Issuer::new(params, key_pair))
}

pub(crate) fn new_end_entity(
    issuer: &Issuer<'static, KeyPair>,
    name: &str,
) -> (Certificate, KeyPair) {
    let mut params = CertificateParams::new(vec![name.into()]).expect("we know the name is valid");
    let (yesterday, tomorrow) = validity_period();
    params.distinguished_name.push(DnType::CommonName, name);
    params.use_authority_key_identifier_extension = true;
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    params.not_before = yesterday;
    params.not_after = tomorrow;

    let key_pair = KeyPair::generate().unwrap();
    (params.signed_by(&key_pair, issuer).unwrap(), key_pair)
}

fn validity_period() -> (OffsetDateTime, OffsetDateTime) {
    let day = Duration::new(86400, 0);
    let yesterday = OffsetDateTime::now_utc().checked_sub(day).unwrap();
    let tomorrow = OffsetDateTime::now_utc().checked_add(day).unwrap();
    (yesterday, tomorrow)
}
