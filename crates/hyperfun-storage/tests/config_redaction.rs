use hyperfun_storage::config::parse_database_url_redacted;

#[test]
fn redacts_password() {
    let url = "postgres://user:secret123@localhost:5432/hyperfun";
    let (_opts, redacted) = parse_database_url_redacted(url).expect("parse");
    assert!(!redacted.contains("secret123"), "password must be redacted: {}", redacted);
    assert!(redacted.contains("user"), "username should remain: {}", redacted);
    assert!(redacted.contains("localhost"), "host should remain: {}", redacted);
    assert!(redacted.contains("hyperfun"), "dbname should remain: {}", redacted);
}

#[test]
fn handles_url_without_password() {
    let url = "postgres://user@localhost/hyperfun";
    let (_opts, redacted) = parse_database_url_redacted(url).expect("parse");
    assert!(redacted.contains("user"));
}

#[test]
fn rejects_malformed_url() {
    let result = parse_database_url_redacted("not-a-url");
    assert!(result.is_err());
}
