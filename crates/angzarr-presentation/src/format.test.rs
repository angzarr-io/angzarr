//! Choosing a format.

use super::Format;

crate::english_for!(&[crate::EN_US.1]);

#[test]
fn test_json_wins_then_the_format_asked_then_the_terminal() {
    use Format::{Json, Text};
    for (json, asked, terminal, chosen) in [
        (true, Some(Text), true, Json),
        (true, None, true, Json),
        (false, Some(Json), true, Json),
        (false, Some(Text), false, Text),
        (false, None, true, Text),
        (false, None, false, Json),
    ] {
        assert_eq!(
            Format::choose(json, asked, terminal),
            chosen,
            "json {json}, asked {asked:?}, terminal {terminal}"
        );
    }
}

#[test]
fn test_a_format_is_named_text_or_json() {
    assert_eq!("text".parse(), Ok(Format::Text));
    assert_eq!("json".parse(), Ok(Format::Json));
    let unknown = "yaml".parse::<Format>().unwrap_err();
    assert_eq!(unknown.value, "yaml");
    assert_eq!(
        unknown.english(),
        "the format yaml is not one of text or json"
    );
}
