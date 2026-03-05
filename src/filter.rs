use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};

#[derive(Debug, Clone)]
pub struct PathFilter {
    include: Option<GlobSet>,
    exclude: GlobSet,
}

impl PathFilter {
    pub fn from_patterns(include: &[String], exclude: &[String]) -> Result<Self> {
        let include = if include.is_empty() {
            None
        } else {
            Some(build_globset(include, "include")?)
        };
        let exclude = if exclude.is_empty() {
            GlobSetBuilder::new()
                .build()
                .context("build empty exclude matcher")?
        } else {
            build_globset(exclude, "exclude")?
        };
        Ok(Self { include, exclude })
    }

    pub fn allows(&self, relative_path: &str) -> bool {
        if let Some(include) = &self.include {
            if include.is_match(relative_path) {
                return true;
            }
            return false;
        }
        !self.exclude.is_match(relative_path)
    }
}

fn build_globset(patterns: &[String], label: &str) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob =
            Glob::new(pattern).with_context(|| format!("invalid {label} pattern '{}'", pattern))?;
        builder.add(glob);
    }
    builder
        .build()
        .with_context(|| format!("build {label} matcher"))
}
