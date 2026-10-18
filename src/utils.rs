use crate::arg::S3Path;

const DELIMITER: char = '/';

/// The last segment of a key: `some/path` → `path`.
fn key_name(key: &str) -> &str {
    key.rsplit(DELIMITER).next().unwrap_or(key)
}

/// Joins a key onto a prefix with a single `/` between them.
fn join_key(prefix: &str, key: &str) -> String {
    if prefix.is_empty() || prefix.ends_with(DELIMITER) {
        format!("{prefix}{key}")
    } else {
        format!("{prefix}{DELIMITER}{key}")
    }
}

/// The destination key for `source` when copying under `destination`;
/// `flat` keeps only the source's last segment.
pub fn combine_keys(flat: bool, source: &str, destination: Option<&str>) -> String {
    let key = if flat { key_name(source) } else { source };

    match destination {
        Some(prefix) => join_key(prefix, key),
        None => key.to_owned(),
    }
}

/// Whether copies made under `destination` are themselves under `source`, so
/// a find over `source` would list them too.
pub fn copies_are_listed(source: &S3Path, destination: &S3Path) -> bool {
    let destination_dir = combine_keys(false, "", destination.prefix.as_deref());
    source.bucket == destination.bucket
        && destination_dir.starts_with(source.prefix.as_deref().unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_name() {
        assert_eq!(key_name("path"), "path");
        assert_eq!(key_name("some/path"), "path");
        assert_eq!(key_name(""), "");
        assert_eq!(key_name("some/path/"), "");
    }

    #[test]
    fn test_join_key() {
        assert_eq!(join_key("some", "path"), "some/path");
        assert_eq!(join_key("some/", "path"), "some/path");
        assert_eq!(join_key("some", ""), "some/");
        assert_eq!(join_key("", "path"), "path");
        assert_eq!(join_key("", ""), "");
    }

    #[test]
    fn test_combine_keys() {
        let destination = Some("somepath/anotherpath");

        assert_eq!(
            combine_keys(false, "path", destination),
            "somepath/anotherpath/path"
        );
        assert_eq!(
            combine_keys(true, "path", destination),
            "somepath/anotherpath/path"
        );
        assert_eq!(
            combine_keys(false, "some/path", destination),
            "somepath/anotherpath/some/path"
        );
        assert_eq!(
            combine_keys(true, "some/path", destination),
            "somepath/anotherpath/path"
        );
        assert_eq!(combine_keys(false, "some/path", None), "some/path");
        assert_eq!(combine_keys(true, "some/path", None), "path");
    }
}
