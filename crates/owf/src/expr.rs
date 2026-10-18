// Runtime expressions: jq, evaluated by jaq. 1. strict-mode detection;
// 2. compile/evaluate with the runtime arguments as global variables;
// 3. templates (values whose strings may be `${ … }` expressions).
//
// Only expressions from the definition are ever evaluated: a template is
// walked once and the values it produces are inserted as data, never parsed
// again (the spec's injection rule).
use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Compiler, Ctx, Vars, data};
use jaq_json::Val;
use serde_json::{Map, Value};

// ── 1. strict mode ─────────────────────────────────────────────────────────

/// The jq inside `${ … }` when the whole (trimmed) string is one expression.
pub fn strip(text: &str) -> Option<&str> {
    let t = text.trim();
    t.strip_prefix("${").and_then(|r| r.strip_suffix('}')).map(str::trim)
}

/// An expression-typed field (`if`, `for.in`, `switch.when`, `output.as`, …)
/// takes jq with or without the `${ }` wrapper.
pub fn bare(text: &str) -> &str {
    strip(text).unwrap_or(text.trim())
}

/// A value-typed string that contains `${` without being one expression.
/// The DSL has no string interpolation outside jq, so this is an authoring
/// error (`"\(.x)"` inside one expression is the way).
pub fn is_embedded(text: &str) -> bool {
    strip(text).is_none() && text.contains("${")
}

// ── 2. compile / evaluate ──────────────────────────────────────────────────

/// Natives withheld from workflows: the process environment (secrets) and
/// the wall clock (time comes from `$workflow`/`$runtime`). `halt` stays
/// because the std defs use it, but `run` turns it into an error: jaq's own
/// `unwrap_valr` would `process::exit` the host.
const WITHHELD: &[&str] = &["env", "now", "input"];

/// The variables an expression sees, in declaration order.
#[derive(Clone, Debug, Default)]
pub struct Args {
    names: Vec<String>,
    values: Vec<Value>,
}

impl Args {
    pub fn new() -> Self {
        Self::default()
    }
    /// Binds `$name` (pass the name without `$`); a later binding shadows.
    pub fn with(mut self, name: &str, value: Value) -> Self {
        self.set(name, value);
        self
    }
    pub fn set(&mut self, name: &str, value: Value) {
        let name = format!("${name}");
        match self.names.iter().position(|n| *n == name) {
            Some(i) => self.values[i] = value,
            None => {
                self.names.push(name);
                self.values.push(value);
            }
        }
    }
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(|n| &n[1..])
    }
}

fn run(code: &str, args: &Args, input: Option<&Value>) -> Result<Value, String> {
    let defs = jaq_core::defs().chain(jaq_std::defs()).chain(jaq_json::defs());
    let funs =
        jaq_core::funs().chain(jaq_std::funs().filter(|(name, _, _)| !WITHHELD.contains(name))).chain(jaq_json::funs());
    let loader = Loader::new(defs);
    let arena = Arena::default();
    let modules = loader
        .load(&arena, File { code, path: () })
        .map_err(|errs| format!("invalid jq `{code}`: {:?}", errs.into_iter().map(|(_, e)| e).collect::<Vec<_>>()))?;
    let filter = Compiler::default()
        .with_funs(funs)
        .with_global_vars(args.names.iter().map(String::as_str))
        .compile(modules)
        .map_err(|errs| format!("invalid jq `{code}`: {:?}", errs.into_iter().map(|(_, e)| e).collect::<Vec<_>>()))?;
    let Some(input) = input else { return Ok(Value::Null) };
    let vars = args.values.iter().map(to_val).collect::<Result<Vec<_>, _>>()?;
    let ctx = Ctx::<data::JustLut<Val>>::new(&filter.lut, Vars::new(vars));
    let mut out = filter.id.run((ctx, to_val(input)?));
    match out.next() {
        // `empty` (no output) reads as null: an absent value, not a failure.
        None => Ok(Value::Null),
        Some(Ok(v)) => from_val(&v),
        Some(Err(exn)) => match exn.get_err() {
            Ok(e) => Err(format!("jq `{code}` failed: {e}")),
            Err(_) => Err(format!("jq `{code}` halted; halt is not allowed in workflows")),
        },
    }
}

/// Compiles `code` against the variables in scope (syntax, unknown names).
pub fn check(code: &str, args: &Args) -> Result<(), String> {
    run(code, args, None).map(drop)
}

/// The first output of `code` on `input` (null when it produces none).
pub fn eval(code: &str, input: &Value, args: &Args) -> Result<Value, String> {
    run(code, args, Some(input))
}

/// jq truthiness: everything but `false` and `null`.
pub fn truthy(v: &Value) -> bool {
    !matches!(v, Value::Null | Value::Bool(false))
}

fn to_val(v: &Value) -> Result<Val, String> {
    serde_json::from_value(v.clone()).map_err(|e| format!("value not representable in jq: {e}"))
}

fn from_val(v: &Val) -> Result<Value, String> {
    serde_json::from_str(&v.to_string()).map_err(|e| format!("jq produced a non-JSON value `{v}`: {e}"))
}

// ── 3. templates ───────────────────────────────────────────────────────────

/// Evaluates every `${ … }` string inside `template`; other values stay.
pub fn render(template: &Value, input: &Value, args: &Args) -> Result<Value, String> {
    Ok(match template {
        Value::String(s) => match strip(s) {
            Some(code) => eval(code, input, args)?,
            None => template.clone(),
        },
        Value::Array(items) => Value::Array(items.iter().map(|v| render(v, input, args)).collect::<Result<_, _>>()?),
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                out.insert(k.clone(), render(v, input, args)?);
            }
            Value::Object(out)
        }
        _ => template.clone(),
    })
}

/// The expressions a template contains, for validation.
pub fn template_exprs(template: &Value, out: &mut Vec<String>) {
    match template {
        Value::String(s) => {
            if let Some(code) = strip(s) {
                out.push(code.to_string());
            }
        }
        Value::Array(items) => items.iter().for_each(|v| template_exprs(v, out)),
        Value::Object(map) => map.values().for_each(|v| template_exprs(v, out)),
        _ => {}
    }
}

/// Strings in a template that embed `${` without being one expression.
pub fn embedded_strings(template: &Value, out: &mut Vec<String>) {
    match template {
        Value::String(s) if is_embedded(s) => out.push(s.clone()),
        Value::Array(items) => items.iter().for_each(|v| embedded_strings(v, out)),
        Value::Object(map) => map.values().for_each(|v| embedded_strings(v, out)),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strict_and_bare_forms() {
        assert_eq!(strip("${ .a }"), Some(".a"));
        assert_eq!(strip("  ${.a}  "), Some(".a"));
        assert_eq!(strip("plain"), None);
        assert_eq!(bare(".colors"), ".colors");
        assert_eq!(bare("${ .colors }"), ".colors");
        assert!(is_embedded("Hello ${ .name }!"));
        assert!(!is_embedded("${ .name }"));
    }

    #[test]
    fn evaluates_with_variables() {
        let args = Args::new().with("context", json!({"k": 1})).with("color", json!("red"));
        let out = eval(r#"{ c: $context.k, x: (.n + 1), col: $color }"#, &json!({"n": 2}), &args).unwrap();
        assert_eq!(out, json!({"c": 1, "x": 3, "col": "red"}));
    }

    #[test]
    fn empty_is_null_and_errors_are_reported() {
        assert_eq!(eval("empty", &json!(1), &Args::new()).unwrap(), Value::Null);
        assert!(eval(".a.b", &json!({"a": 1}), &Args::new()).is_err());
        assert!(check("$missing", &Args::new()).is_err());
        assert!(check("$here", &Args::new().with("here", Value::Null)).is_ok());
        assert!(check(".[", &Args::new()).is_err());
    }

    #[test]
    fn host_environment_clock_and_halt_are_withheld() {
        assert!(eval("halt", &json!(1), &Args::new()).unwrap_err().contains("halt"));
        assert!(eval("halt_error", &json!(1), &Args::new()).is_err());
        assert!(check("env", &Args::new()).is_err());
        assert!(check("now", &Args::new()).is_err());
        assert!(check("$ENV", &Args::new()).is_err());
    }

    #[test]
    fn std_library_is_available() {
        let out =
            eval(r#"[.[] | select(. > 1)] | map(tostring) | join(",")"#, &json!([1, 2, 3]), &Args::new()).unwrap();
        assert_eq!(out, json!("2,3"));
        assert_eq!(eval("null + [1]", &Value::Null, &Args::new()).unwrap(), json!([1]));
        assert_eq!(eval(r#""a\(.)b""#, &json!(5), &Args::new()).unwrap(), json!("a5b"));
    }

    #[test]
    fn templates_render_only_definition_expressions() {
        let template = json!({"plain": "x", "e": "${ .v }", "nested": ["${ .v + 1 }", 3]});
        // The produced string looks like an expression but is data: not re-evaluated.
        let input = json!({"v": "${ .secret }"});
        let out = render(&template, &input, &Args::new()).unwrap_err();
        assert!(out.contains("failed"), "string + number fails, proving `.v` was data: {out}");
        let out = render(&json!({"e": "${ .v }"}), &input, &Args::new()).unwrap();
        assert_eq!(out, json!({"e": "${ .secret }"}));
    }
}
