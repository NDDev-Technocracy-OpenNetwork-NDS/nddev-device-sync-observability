// Generated; do not edit. AGPL-3.0-only. NDDev OpenNetwork — https://nddev.ai
// Source: NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-protocol/contracts/v1/source-response.schema.json
// Schema SHA-256: ef73380eac46d9f46fbd4776bae3c2d91a1236176c67582ed0df407a584bc6d8
// Generator script SHA-256: ba71ce353a691d59227fbe948f45edf4b0667be7e5a105741f551d3186f6a8b8
// Tool: quicktype-core 26.0.0; dependency lock SHA-256: ec1d8541bcae0771e2fe0e95e572f60898fd62e6e31fbfbea80451d244aff5b9
// DTO generation is not schema, authorization or cryptographic validation.

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceResponse {
    pub license: License,

    pub source_url: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum License {
    #[serde(rename = "AGPL-3.0-only")]
    Agpl30Only,
}
