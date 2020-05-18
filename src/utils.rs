const DELIMETER: char = '/';

pub trait S3Key {
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
        if self.chars().rev().next() != Some(DELIMETER) && !self.is_empty() {
            self.push(DELIMETER);
        }
        self.push_str(dest);
        self
    }
}

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
