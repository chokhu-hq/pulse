use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SocketId(String);

#[derive(Debug, thiserror::Error)]
#[error("socket id must be two dot separated integers")]
pub(crate) struct InvalidSocketId;

impl SocketId {
    // the official sdks reject anything that does not match \d+\.\d+
    pub(crate) fn generate() -> Result<Self, getrandom::Error> {
        let bits = getrandom::u64()?;
        let (high, low) = (bits >> 32, bits & u64::from(u32::MAX));

        Ok(Self(format!("{high}.{low}")))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for SocketId {
    type Err = InvalidSocketId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let is_number = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
        let (left, right) = s.split_once('.').ok_or(InvalidSocketId)?;
        if !is_number(left) || !is_number(right) {
            return Err(InvalidSocketId);
        }

        Ok(Self(s.to_owned()))
    }
}

impl fmt::Display for SocketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_parse() {
        let id = SocketId::generate().unwrap();

        assert_eq!(
            id.as_str().parse::<SocketId>().unwrap(),
            id,
            "generated id {id} round trips"
        );
    }

    #[test]
    fn parse() {
        let cases = [
            ("123.456", true),
            ("1.2", true),
            ("123", false),
            (".1", false),
            ("1.", false),
            ("a.1", false),
        ];

        for (input, valid) in cases {
            assert_eq!(
                input.parse::<SocketId>().is_ok(),
                valid,
                "socket id {input:?}"
            );
        }
    }
}
