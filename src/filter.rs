use anyhow::{Context, Result, bail};
use globset::{Glob, GlobMatcher};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuleAction {
    Include,
    Exclude,
}

#[derive(Debug, Clone)]
struct Rule {
    action: RuleAction,
    matcher: GlobMatcher,
}

#[derive(Debug, Clone)]
pub struct PathFilter {
    has_include: bool,
    rules: Vec<Rule>,
}

impl PathFilter {
    pub fn from_patterns(include: &[String], exclude: &[String]) -> Result<Self> {
        let mut rules = Vec::with_capacity(include.len().saturating_add(exclude.len()));
        for pattern in include {
            rules.push(compile_rule(pattern, RuleAction::Include)?);
        }
        for pattern in exclude {
            rules.push(compile_rule(pattern, RuleAction::Exclude)?);
        }
        Ok(Self {
            has_include: !include.is_empty(),
            rules,
        })
    }

    pub fn allows(&self, relative_path: &str) -> bool {
        for rule in &self.rules {
            if rule.matcher.is_match(relative_path) {
                return matches!(rule.action, RuleAction::Include);
            }
        }
        !self.has_include
    }
}

fn compile_rule(pattern: &str, action: RuleAction) -> Result<Rule> {
    let normalized = normalize_pattern(pattern)?;
    let glob = Glob::new(&normalized).with_context(|| {
        let label = match action {
            RuleAction::Include => "include",
            RuleAction::Exclude => "exclude",
        };
        format!("invalid {label} pattern '{}'", pattern)
    })?;
    Ok(Rule {
        action,
        matcher: glob.compile_matcher(),
    })
}

fn normalize_pattern(pattern: &str) -> Result<String> {
    let mut pattern = pattern.trim().replace('\\', "/");
    if pattern.is_empty() {
        bail!("pattern cannot be empty");
    }
    if let Some(stripped) = pattern.strip_prefix("./") {
        pattern = stripped.to_string();
    }
    if let Some(stripped) = pattern.strip_prefix('/') {
        pattern = stripped.to_string();
    }
    if pattern.is_empty() {
        bail!("pattern cannot be root-only ('/')");
    }
    if pattern.ends_with('/') {
        pattern.push_str("**");
    } else if !pattern.contains('/') {
        pattern = format!("**/{pattern}");
    }
    Ok(pattern)
}

#[cfg(test)]
mod tests {
    use super::PathFilter;

    #[test]
    fn include_and_exclude_apply_together() {
        let filter = PathFilter::from_patterns(
            &[String::from("**/*.txt")],
            &[String::from("**/secret*.txt")],
        )
        .expect("build filter");
        assert!(filter.allows("notes/readme.txt"));
        assert!(filter.allows("notes/secret.txt"));
        assert!(!filter.allows("notes/image.png"));
    }

    #[test]
    fn basename_pattern_matches_nested_paths() {
        let filter = PathFilter::from_patterns(&[], &[String::from("*.tmp")]).expect("build");
        assert!(!filter.allows("root/a.tmp"));
        assert!(!filter.allows("a.tmp"));
        assert!(filter.allows("a.txt"));
    }

    #[test]
    fn directory_suffix_pattern_matches_tree() {
        let filter = PathFilter::from_patterns(&[], &[String::from("cache/")]).expect("build");
        assert!(!filter.allows("cache/data.bin"));
        assert!(filter.allows("other/cache/data.bin"));
    }

    #[test]
    fn include_rules_default_deny() {
        let filter = PathFilter::from_patterns(&[String::from("docs/**")], &[]).expect("build");
        assert!(filter.allows("docs/readme.md"));
        assert!(!filter.allows("src/main.rs"));
    }
}
