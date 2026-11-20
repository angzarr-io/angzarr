//! The json fixture's wire types and their proto3 JSON, generated into
//! OUT_DIR, beside the descriptor set protoc wrote for them
//! (`angzarr.descriptors`), which the tests cross-check with.

const PROTOS: &[&str] = &["json/v1/json.proto"];
const INCLUDES: &[&str] = &["../../../../angzarr-codegen/tests/fixtures"];

fn main() -> Result<(), angzarr_build::BuildError> {
    angzarr_build::wire(PROTOS, INCLUDES)?;
    angzarr_build::json(PROTOS, INCLUDES, &[("json.v1", "crate::wire::json::v1")])
}
