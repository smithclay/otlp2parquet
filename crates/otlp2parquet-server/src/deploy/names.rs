//! Fun default name generation for stacks and workers

use rand::Rng;

#[allow(dead_code)]
const ADJECTIVES: &[&str] = &[
    "swift", "eager", "bright", "cosmic", "dapper", "fluent", "golden", "humble", "jovial", "keen",
    "lively", "mellow", "nimble", "plucky", "quick", "rustic", "snappy", "trusty", "vivid",
    "witty",
];

#[allow(dead_code)]
const NOUNS: &[&str] = &[
    "arrow", "beacon", "conduit", "depot", "emitter", "funnel", "gauge", "harbor", "inlet",
    "journal", "keeper", "ledger", "metric", "nexus", "outlet", "parquet", "queue", "relay",
    "signal", "tracer",
];

/// Generate a fun default name like "nimble-relay-2847"
#[allow(dead_code)]
pub fn generate() -> String {
    let mut rng = rand::thread_rng();
    let adjective = ADJECTIVES[rng.gen_range(0..ADJECTIVES.len())];
    let noun = NOUNS[rng.gen_range(0..NOUNS.len())];
    let number: u16 = rng.gen_range(1000..10000);
    format!("{}-{}-{}", adjective, noun, number)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_name_format() {
        let name = generate();
        let parts: Vec<&str> = name.split('-').collect();
        assert_eq!(parts.len(), 3);
        assert!(ADJECTIVES.contains(&parts[0]));
        assert!(NOUNS.contains(&parts[1]));
        let number: u16 = parts[2].parse().unwrap();
        assert!((1000..10000).contains(&number));
    }
}
