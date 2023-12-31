use std::borrow::Cow;

use colored::Colorize;
use rustyline::{
    highlight::Highlighter,
    hint::{Hint, Hinter},
    Context,
};
use rustyline_derive::{Completer, Helper, Validator};

const COMMANDS: [&str; 8] = [
    "help", "select", "insert", "update", "delete", "create", "drop", "rename",
];

const FLAGS: [&str; 9] = [
    "-t",
    "-w",
    "-c",
    "-v",
    "--table",
    "--where",
    "--columns",
    "--values",
    "--help",
];

#[derive(Helper, Completer, Validator)]
pub struct PoorlyHelper {
    commands: Vec<String>,
    flags: Vec<String>,
}

impl Default for PoorlyHelper {
    fn default() -> Self {
        Self {
            commands: COMMANDS.map(|s| s.to_string()).to_vec(),
            flags: FLAGS.map(|s| s.to_string()).to_vec(),
        }
    }
}

#[derive(Hash, Debug, PartialEq, Eq, Default)]
pub struct CommandHint(String);

impl Hint for CommandHint {
    fn display(&self) -> &str {
        &self.0
    }

    fn completion(&self) -> Option<&str> {
        Some(&self.0)
    }
}

impl Hinter for PoorlyHelper {
    type Hint = CommandHint;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<CommandHint> {
        if line.trim().is_empty() || line.trim_end() != line || pos < line.len() {
            return None;
        }

        let words: Vec<_> = line.split_whitespace().collect();
        let last_word = words[words.len() - 1];
        let hints = if words.len() == 1 {
            self.commands.iter()
        } else {
            self.flags.iter()
        };

        hints
            .filter(|hint| !words.contains(&hint.as_str()))
            .find_map(|hint| {
                hint.strip_prefix(last_word)
                    .map(|stripped| CommandHint(stripped.to_string()))
            })
    }
}

impl Highlighter for PoorlyHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            prompt.bold().to_string().into()
        } else {
            prompt.into()
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        hint.dimmed().to_string().into()
    }

    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        line.split_whitespace()
            .map(|word| {
                let word = word.to_string();
                if self.commands.contains(&word) {
                    word.yellow().to_string()
                } else if self.flags.contains(&word) {
                    word.bright_white().italic().to_string()
                } else {
                    word
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
            .into()
    }
}
