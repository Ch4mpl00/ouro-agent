// PDF → plain text, for the bills download_gmail_attachment saves. Pages are
// joined with one blank line so the agent can still see the page breaks.
// Cyrillic text extracts cleanly (checked against real NashDom bills).
//
// Sections:
//   1. extract — file → { text, numPages }
//   2. tools   — `pdf` toolset: read_pdf

use std::path::Path;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};

use crate::server::{McpTools, ToolResult, respond};

// ── 1. extract ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PdfText {
    pub text: String,
    pub num_pages: usize,
}

pub fn extract_bytes(bytes: &[u8]) -> anyhow::Result<PdfText> {
    let pages = pdf_extract::extract_text_from_mem_by_pages(bytes)?;
    Ok(PdfText { num_pages: pages.len(), text: pages.join("\n\n") })
}

// Parsing is CPU-bound and the parser can panic on a malformed file; a
// blocking task contains both.
pub async fn read_pdf(path: &Path) -> anyhow::Result<PdfText> {
    let bytes = tokio::fs::read(path).await?;
    tokio::task::spawn_blocking(move || extract_bytes(&bytes))
        .await
        .map_err(|err| anyhow::anyhow!("PDF parser crashed on this file: {err}"))?
}

// ── 2. tools ─────────────────────────────────────────────────────────────────

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ReadPdfParams {
    /// Absolute path to the PDF file.
    file_path: String,
}

#[tool_router(router = pdf_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "read_pdf",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "Read PDF",
        description = "Extract plain text from a PDF file at the given absolute path. Returns \
            the full text (pages separated by a blank line) and total page count. \
            Use after download_gmail_attachment to inspect the contents of a downloaded bill."
    )]
    async fn read_pdf(&self, Parameters(p): Parameters<ReadPdfParams>) -> ToolResult {
        respond(async { read_pdf(&self.deps.files.readable_path(&p.file_path).await?).await }.await)
    }
}
