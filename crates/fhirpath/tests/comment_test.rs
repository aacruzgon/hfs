#[cfg(test)]
mod tests {
    use chumsky::Parser;
    use helios_fhirpath::parser::parser;

    #[test]
    fn test_single_line_comment() {
        let expr = "2 + 2 // This is a comment";
        let result = parser().parse(expr);
        assert!(result.is_ok(), "Failed to parse expression with single-line comment: {:?}", result);
    }

    #[test]
    fn test_multi_line_comment() {
        let expr = "2 + /* inline comment */ 2";
        let result = parser().parse(expr);
        assert!(result.is_ok(), "Failed to parse expression with multi-line comment");
    }

    #[test]
    fn test_comment_at_start() {
        let expr = "// comment at start\n3 + 3";
        let result = parser().parse(expr);
        assert!(result.is_ok(), "Failed to parse expression with comment at start");
    }

    #[test]
    fn test_nested_multi_line_comment() {
        let expr = "/* multi\nline\ncomment */ 5";
        let result = parser().parse(expr);
        assert!(result.is_ok(), "Failed to parse expression with multi-line comment");
    }
}