/// Returns the OpenAI per-token pricing (input, output) **in USD** for
/// a given model name. The list is not exhaustive – it only covers the most
/// common public models so we offer reasonable estimates without hard-coding
/// every single variant. Unknown models return `None` so callers can fall
/// back gracefully.
pub fn get_openai_pricing(model: &str) -> Option<(f64, f64)> {
  // Exact mapping (per *token* rates, not per-1K)
  // Order matters: more specific matches must come before general ones
  let detailed: &[(&str, (f64, f64))] = &[ // (model, (input, output))
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