use glob::Pattern;

#[derive(Debug, Clone, PartialEq)]
pub struct S3path {
    pub bucket: String,
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FindSize {
    Equal(i64),
    Bigger(i64),
    Lower(i64),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FindTime {
    Upper(i64),
    Lower(i64),
}

pub type NameGlob = Pattern;

#[derive(Debug, Clone, PartialEq)]
pub struct InameGlob(pub Pattern);

#[derive(Debug, PartialEq, Clone)]
pub struct FindTag {
    pub key: String,
    pub value: String,
}
