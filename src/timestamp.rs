use time::{
    OffsetDateTime, UtcOffset, error::Parse, format_description::well_known::Rfc3339,
    macros::format_description,
};

pub(crate) fn parse(value: &str) -> Result<OffsetDateTime, Parse> {
    OffsetDateTime::parse(value, &Rfc3339).map(|value| value.to_offset(UtcOffset::UTC))
}

pub(crate) fn from_millis(value: i64) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(value) * 1_000_000).ok()
}

pub(crate) fn millis(value: OffsetDateTime) -> i64 {
    value.unix_timestamp_nanos().div_euclid(1_000_000) as i64
}

pub(crate) fn micros(value: OffsetDateTime) -> i64 {
    value.unix_timestamp_nanos().div_euclid(1_000) as i64
}

// Public status timestamps keep the same UTC suffix and fractional precision.
pub(crate) fn format_utc(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    let date = value
        .format(format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second]"
        ))
        .expect("UTC timestamp has all required components");
    let nanos = value.nanosecond();
    match nanos {
        0 => format!("{date}+00:00"),
        n if n.is_multiple_of(1_000_000) => format!("{date}.{:03}+00:00", n / 1_000_000),
        n if n.is_multiple_of(1_000) => format!("{date}.{:06}+00:00", n / 1_000),
        n => format!("{date}.{n:09}+00:00"),
    }
}

pub(crate) fn format_millis(microseconds: i64) -> Option<String> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(microseconds) * 1_000)
        .ok()?
        .format(format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
        ))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_wire_format_keeps_the_previous_precision_and_offset() {
        for (input, output) in [
            ("2026-11-18T07:30:00+05:00", "2026-11-18T02:30:00+00:00"),
            ("2026-11-18T02:30:00.12Z", "2026-11-18T02:30:00.120+00:00"),
            (
                "2026-11-18T02:30:00.1234Z",
                "2026-11-18T02:30:00.123400+00:00",
            ),
            (
                "2026-11-18T02:30:00.123456789Z",
                "2026-11-18T02:30:00.123456789+00:00",
            ),
        ] {
            assert_eq!(format_utc(parse(input).unwrap()), output);
        }
    }

    #[test]
    fn unix_units_round_down_and_search_keeps_three_digits() {
        let time = parse("1969-12-31T23:59:59.999999999Z").unwrap();
        assert_eq!(millis(time), -1);
        assert_eq!(micros(time), -1);
        assert_eq!(
            format_millis(-1).as_deref(),
            Some("1969-12-31T23:59:59.999Z")
        );
        assert_eq!(millis(from_millis(-1).unwrap()), -1);
    }
}
