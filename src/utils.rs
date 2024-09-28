const DELIMETER: char = '/';

trait S3Key {
    fn key_name(self) -> Self;
    fn join_key(self, dest: &Self) -> Self;
}

impl S3Key for String {
    #[inline]
    fn key_name(self) -> Self {
        self.rsplit(DELIMETER)
            .next()
            .map(|x| x.to_owned())
            .unwrap_or(self)
    }

    #[inline]
    fn join_key(mut self, dest: &Self) -> Self {
        if !self.ends_with(DELIMETER) && !self.is_empty() {
            self.push(DELIMETER);
        }
        self.push_str(dest);
        self
    }
}

#[inline]
pub fn combine_keys(flat: bool, source: &str, destination: &Option<String>) -> String {
    let key = if flat {
        source.to_owned().key_name()
    } else {
        source.to_owned()
    };

    if let Some(ref destination) = destination {
        destination.to_owned().join_key(&key)
    } else {
        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_name() {
        assert_eq!("path".to_owned().key_name(), "path");
        assert_eq!("some/path".to_owned().key_name(), "path");
        assert_eq!("".to_owned().key_name(), "");
        assert_eq!("some/path/".to_owned().key_name(), "");
    }

    #[test]
    fn test_join_key() {
        assert_eq!("some".to_owned().join_key(&"path".to_string()), "some/path");
        assert_eq!("some".to_owned().join_key(&"".to_string()), "some/");
        assert_eq!("".to_owned().join_key(&"path".to_string()), "path");
        assert_eq!("".to_owned().join_key(&"".to_string()), "");
    }

    #[test]
    fn test_combine_keys() {
        assert_eq!(
            &combine_keys(false, "path", &Some("somepath/anotherpath".to_owned())),
            "somepath/anotherpath/path",
        );
        assert_eq!(
            &combine_keys(true, "path", &Some("somepath/anotherpath".to_owned())),
            "somepath/anotherpath/path",
        );
        assert_eq!(
            &combine_keys(false, "some/path", &Some("somepath/anotherpath".to_owned())),
            "somepath/anotherpath/some/path",
        );
        assert_eq!(
            &combine_keys(true, "some/path", &Some("somepath/anotherpath".to_owned())),
            "somepath/anotherpath/path",
        );
        assert_eq!(&combine_keys(false, "some/path", &None), "some/path",);
        assert_eq!(&combine_keys(true, "some/path", &None), "path",);
    }
}
