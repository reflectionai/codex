/// Computes the total cost in USD for the given token usage using OpenAI pricing.
/// Returns `None` if the model is unknown or pricing is unavailable.
pub fn compute_openai_cost(model: &str, input_tokens: u32, output_tokens: u32) -> Option<f64> {
    let (per_input_token_cost, per_output_token_cost) = get_openai_pricing(model)?;
    // Rates are per-token. Multiply directly.
    let cost = (input_tokens as f64) * per_input_token_cost
        + (output_tokens as f64) * per_output_token_cost;
    Some(cost)
}

/// Returns the OpenAI per-token pricing (input, output) **in USD** for
/// a given model name. The list is not exhaustive – it only covers the most
/// common public models so we offer reasonable estimates without hard-coding
/// every single variant. Unknown models return `None` so callers can fall
/// back gracefully.
pub fn get_openai_pricing(model: &str) -> Option<(f64, f64)> {
    // Exact mapping (per *token* rates, not per-1K)
    // Order matters: more specific matches must come before general ones
    let detailed: &[(&str, (f64, f64))] = &[
        // (model, (input, output))
        ("o3", (10.0 / 1_000_000.0, 40.0 / 1_000_000.0)),
        ("o4-mini", (1.1 / 1_000_000.0, 4.4 / 1_000_000.0)),
        ("gpt-4.1-nano", (0.1 / 1_000_000.0, 0.4 / 1_000_000.0)),
        ("gpt-4.1-mini", (0.4 / 1_000_000.0, 1.6 / 1_000_000.0)),
        ("gpt-4.1", (2.0 / 1_000_000.0, 8.0 / 1_000_000.0)),
        ("gpt-4o-mini", (0.6 / 1_000_000.0, 2.4 / 1_000_000.0)),
        ("gpt-4o", (5.0 / 1_000_000.0, 20.0 / 1_000_000.0)),
        ("codex-mini-latest", (1.5 / 1_000_000.0, 6.0 / 1_000_000.0)),
    ];

    let key = model.to_ascii_lowercase();
    detailed
        .iter()
        // We use starts_with to match model variants (e.g., "gpt-4o-2024-11-20" matches "gpt-4o")
        .find(|(m, _)| key.starts_with(*m))
        .map(|(_, r)| *r)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_openai_cost() {
        // Test cost computation for known model
        let cost = compute_openai_cost("gpt-4o-mini", 1000, 500).unwrap();
        let expected = (1000.0 * 0.6 / 1_000_000.0) + (500.0 * 2.4 / 1_000_000.0);
        assert_eq!(cost, expected);

        // Test cost computation for unknown model
        assert_eq!(compute_openai_cost("unknown-model", 1000, 500), None);

        // Test zero tokens
        let cost = compute_openai_cost("gpt-4o-mini", 0, 0).unwrap();
        assert_eq!(cost, 0.0);
    }

    #[test]
    fn test_openai_pricing() {
        // Test exact matches
        assert_eq!(
            get_openai_pricing("gpt-4o-mini"),
            Some((0.6 / 1_000_000.0, 2.4 / 1_000_000.0))
        );
        assert_eq!(
            get_openai_pricing("gpt-4o"),
            Some((5.0 / 1_000_000.0, 20.0 / 1_000_000.0))
        );
        assert_eq!(
            get_openai_pricing("codex-mini-latest"),
            Some((1.5 / 1_000_000.0, 6.0 / 1_000_000.0))
        );

        // Test model variants (should match prefix)
        assert_eq!(
            get_openai_pricing("gpt-4o-2024-11-20"),
            Some((5.0 / 1_000_000.0, 20.0 / 1_000_000.0))
        );

        // Test unknown model
        assert_eq!(get_openai_pricing("unknown-model"), None);

        // Test case insensitive
        assert_eq!(
            get_openai_pricing("GPT-4O-MINI"),
            Some((0.6 / 1_000_000.0, 2.4 / 1_000_000.0))
        );
    }
}
