//! Checks that Rust comments appear only in a file's leading header.

use std::{fs, io, ops::Range, process::Command};

use anyhow::{Context, ensure};
use ra_ap_rustc_lexer::{DocStyle, FrontmatterAllowed, TokenKind, strip_shebang, tokenize};

fn inline_comments(source: &str) -> impl Iterator<Item = Range<usize>> {
    let text = source.strip_prefix('\u{feff}').unwrap_or(source);
    let shebang = strip_shebang(text).unwrap_or_default();
    let body = text.get(shebang..).unwrap_or(text);
    let mut offset = source.len().saturating_sub(body.len());
    let mut header = true;

    tokenize(body, FrontmatterAllowed::No).filter_map(move |token| {
        let start = offset;
        let len = usize::try_from(token.len).unwrap_or(usize::MAX);
        offset = offset.saturating_add(len);

        match token.kind {
            TokenKind::Whitespace => None,
            TokenKind::LineComment { doc_style } | TokenKind::BlockComment { doc_style, .. } => {
                header &= doc_style != Some(DocStyle::Outer);
                (!header).then_some(start..offset)
            }
            _ => {
                header = false;
                None
            }
        }
    })
}

#[expect(clippy::print_stderr, reason = "source-lint diagnostics")]
fn main() -> anyhow::Result<()> {
    let files = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--deduplicate",
            "--",
            "*.rs",
        ])
        .output()
        .context("listing Rust source files")?;
    ensure!(files.status.success(), "git ls-files failed");
    let paths = String::from_utf8(files.stdout).context("decoding Rust source paths")?;

    let mut failed = false;
    for path in paths.split_terminator('\0') {
        let source = match fs::read_to_string(path) {
            Ok(source) => source,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).with_context(|| format!("reading {path}")),
        };
        for range in inline_comments(&source) {
            let prefix = source.get(..range.start).unwrap_or_default();
            let line = prefix.bytes().filter(|byte| *byte == b'\n').count();
            eprintln!(
                "{path}:{}: Rust comments are allowed only in the file header",
                line.saturating_add(1)
            );
            failed = true;
        }
    }
    ensure!(!failed, "Rust comment check failed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::inline_comments;

    fn comments(source: &str) -> Vec<&str> {
        inline_comments(source)
            .filter_map(|range| source.get(range))
            .collect()
    }

    #[test]
    fn accepts_the_file_header_and_rejects_comments_after_code() {
        let source = "\u{feff}#!/usr/bin/env rustx\n// license\n/*! file docs */\n\nfn run() {} // tail\n/* outside */";
        assert_eq!(comments(source), ["// tail", "/* outside */"]);
    }

    #[test]
    fn rejects_outer_doc_comments_even_before_the_first_item() {
        let source = "/// item docs\nfn run() {}\n/** more */ struct Other;";
        assert_eq!(comments(source), ["/// item docs", "/** more */"]);
    }

    #[test]
    fn recognizes_comments_instead_of_matching_markers_in_literals() {
        let source = r##"
            fn with_lifetime<'a>(text: &'a str) {
                let _ = "https://example.test";
                let _ = r#"/* raw text */"#;
                let _ = b"// bytes";
                let _ = cr#"// C literal"#;
                let _ = '/';
                let _ = text; /* outer /* nested */ end */
            }
        "##;
        assert_eq!(comments(source), ["/* outer /* nested */ end */"]);
    }
}
