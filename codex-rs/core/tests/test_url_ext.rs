use codex_core::util::UrlExt;
use url::Url;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_path_basic() {
        let base_url = Url::parse("https://api.example.com/v1").unwrap();
        let result = base_url.append_path("/models").unwrap();
        assert_eq!(result.as_str(), "https://api.example.com/v1/models");
    }

    #[test]
    fn test_append_path_with_query_params() {
        let base_url = Url::parse("https://api.example.com/v1?version=2023").unwrap();
        let result = base_url.append_path("/models").unwrap();
        assert_eq!(
            result.as_str(),
            "https://api.example.com/v1/models?version=2023"
        );
    }

    #[test]
    fn test_append_path_with_multiple_segments() {
        let base_url = Url::parse("https://api.example.com").unwrap();
        let result = base_url.append_path("/v1/models/gpt-4").unwrap();
        assert_eq!(result.as_str(), "https://api.example.com/v1/models/gpt-4");
    }

    #[test]
    fn test_append_path_with_existing_path() {
        let base_url = Url::parse("https://api.example.com/v1").unwrap();
        let result = base_url.append_path("/models/gpt-4").unwrap();
        assert_eq!(result.as_str(), "https://api.example.com/v1/models/gpt-4");
    }

    #[test]
    fn test_append_path_with_trailing_slash() {
        let base_url = Url::parse("https://api.example.com/v1/").unwrap();
        let result = base_url.append_path("models").unwrap();
        assert_eq!(result.as_str(), "https://api.example.com/v1/models");
    }

    #[test]
    fn test_append_path_with_complex_query() {
        let base_url = Url::parse("https://api.example.com/v1?version=2023&api-key=123").unwrap();
        let result = base_url.append_path("/models/gpt-4").unwrap();
        assert_eq!(
            result.as_str(),
            "https://api.example.com/v1/models/gpt-4?version=2023&api-key=123"
        );
    }

    #[test]
    fn test_append_path_with_fragment() {
        let base_url = Url::parse("https://api.example.com/v1#section").unwrap();
        let result = base_url.append_path("/models").unwrap();
        assert_eq!(result.as_str(), "https://api.example.com/v1/models#section");
    }

    #[test]
    fn test_append_path_with_invalid_path() {
        let base_url = Url::parse("https://api.example.com").unwrap();
        let result = base_url.append_path("invalid path with spaces");
        assert!(result.is_err());
    }
}
