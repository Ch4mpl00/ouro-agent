// Random but valid workflows: 1. the program (a typed tree the tests and the
// reference model read); 2. its expressions, a grammar small enough that
// every result is predictable; 3. the generator (proptest, shrinkable) and
// the fix-up that makes any generated tree valid; 4. rendering to a
// definition `owf::model::parse` accepts; 5. rewrites the metamorphic tests
// apply.
//
// The data every task sees is an object with an integer counter, `{ n: 3 }`.
// Flow targets are indices into the task's own list, resolved to names only
// when rendered, so renaming a task cannot break a `then`.
use proptest::prelude::*;
use serde_json::{Map, Value, json};

// ── 1. the program ─────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct Program {
    pub input_from: Option<Data>,
    pub output_as: Option<Data>,
    /// `use.functions`: each is a task; its `name` is the function's name.
    pub functions: Vec<Task>,
    pub tasks: Vec<Task>,
    /// Expression-typed fields written bare (`.n > 1`) instead of `${ … }`.
    pub bare: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Task {
    pub name: String,
    pub when: Option<Cond>,
    pub input_from: Option<Data>,
    pub output_as: Option<Data>,
    pub export_as: Option<Data>,
    pub timeout_ms: Option<u64>,
    pub then: Option<Flow>,
    pub body: Body,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Flow {
    Continue,
    Exit,
    End,
    /// The index of a task in the same list.
    Goto(usize),
    /// A literal target that may name nothing (the sloppy profile only).
    Named(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Body {
    Set(Template),
    /// A host function or a `use.functions` entry, with `{ n: <num> }`.
    Call(String, Num),
    Do(Vec<Task>),
    For {
        each: Option<String>,
        at: Option<String>,
        items: Items,
        cond: Option<Cond>,
        body: Vec<Task>,
    },
    Switch(Vec<Case>),
    Try {
        body: Vec<Task>,
        catch: Box<Catch>,
    },
    Raise(u16),
    Fork {
        branches: Vec<Task>,
        compete: bool,
    },
    Wait(u64),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Items {
    Inline(Vec<i64>),
    /// `[range(.n % m)]`.
    Range(i64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Case {
    pub name: String,
    pub when: Option<Cond>,
    pub then: Flow,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Catch {
    /// `errors.with: { status }`.
    pub status: Option<u16>,
    pub as_: Option<String>,
    pub when: Option<Cond>,
    pub retry: Option<Retry>,
    pub handler: Option<Vec<Task>>,
    pub then: Option<Flow>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Retry {
    pub count: Option<u32>,
    pub delay_ms: u64,
    pub when: Option<Cond>,
}

/// The variable names a catch binds when `as` is absent, and a loop when
/// `each`/`at` are.
pub const DEFAULT_ERROR: &str = "error";
pub const DEFAULT_EACH: &str = "item";
pub const DEFAULT_AT: &str = "index";

/// The raised error's type (any URI; the catch filters match on status).
pub const RAISED: &str = "https://example.com/errors/raised";

pub fn task(name: &str, body: Body) -> Task {
    Task {
        name: name.into(),
        when: None,
        input_from: None,
        output_as: None,
        export_as: None,
        timeout_ms: None,
        then: None,
        body,
    }
}

// ── 2. expressions ─────────────────────────────────────────────────────────

/// A number computed from the data.
#[derive(Clone, Debug, PartialEq)]
pub enum Num {
    N,
    Add(i64),
    Double,
    Ctx,
    /// A loop variable (item or index): the generator's pick, then its name.
    Var(usize, String),
    /// The caught error's status; the variable's name once fixed.
    ErrStatus(String),
    Lit(i64),
    /// Indexes a number: always an expression error.
    Bad,
}

/// A value computed from the data: itself, or `{ n: <num> }`.
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    Same,
    Obj(Num),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Cond {
    True,
    False,
    Gt(i64),
    Even,
    VarGt(usize, String, i64),
    ErrIs(String, u16),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Template {
    /// `set: '${ <data> }'`.
    Expr(Data),
    /// `set: { n: '${ <num> }' }` (a literal for `Lit`).
    Obj(Num),
}

impl Num {
    pub fn code(&self) -> String {
        match self {
            Num::N => ".n".into(),
            Num::Add(k) => format!(".n + {k}"),
            Num::Double => ".n * 2".into(),
            Num::Ctx => ".n + $context.n".into(),
            Num::Var(_, v) => format!(".n + ${v}"),
            Num::ErrStatus(e) => format!(".n + ${e}.status"),
            Num::Lit(k) => k.to_string(),
            Num::Bad => ".n.x".into(),
        }
    }
}

impl Data {
    pub fn code(&self) -> String {
        match self {
            Data::Same => ".".into(),
            Data::Obj(n) => format!("{{ n: ({}) }}", n.code()),
        }
    }
}

impl Cond {
    pub fn code(&self) -> String {
        match self {
            Cond::True => "true".into(),
            Cond::False => "false".into(),
            Cond::Gt(k) => format!(".n > {k}"),
            Cond::Even => ".n % 2 == 0".into(),
            Cond::VarGt(_, v, k) => format!("${v} > {k}"),
            Cond::ErrIs(e, s) => format!("${e}.status == {s}"),
        }
    }
}

impl Items {
    pub fn code(&self) -> Option<String> {
        match self {
            Items::Inline(_) => None,
            Items::Range(m) => Some(format!("[range(.n % {m})]")),
        }
    }
}

// ── 3. the generator ───────────────────────────────────────────────────────

/// Which constructs a program may contain. The fix-up rewrites what a
/// profile excludes into something it allows, so one generator serves all.
#[derive(Clone, Copy, Debug)]
pub struct Profile {
    pub fork: bool,
    pub compete: bool,
    pub wait: bool,
    pub timeouts: bool,
    pub functions: bool,
    /// Calls to unknown functions and `then`s that may name nothing.
    pub sloppy: bool,
}

impl Profile {
    /// What the reference model interprets.
    pub const CORE: Profile =
        Profile { fork: false, compete: false, wait: false, timeouts: false, functions: true, sloppy: false };
    pub const FULL: Profile =
        Profile { fork: true, compete: true, wait: true, timeouts: true, functions: true, sloppy: false };
    /// Everything but racing branches, whose winner depends on timing.
    pub const DETERMINISTIC: Profile = Profile { compete: false, ..Profile::FULL };
    pub const SLOPPY: Profile = Profile { sloppy: true, ..Profile::DETERMINISTIC };
}

/// The host functions (support/host.rs) and two reusable ones.
pub const HOST: &[&str] = &["inc", "send", "down", "reject", "flaky"];
pub const REUSABLE: &[&str] = &["helper", "notify"];
pub const GHOST: &str = "ghost";

fn num() -> BoxedStrategy<Num> {
    prop_oneof![
        3 => Just(Num::N),
        4 => (-2i64..4).prop_map(Num::Add),
        1 => Just(Num::Double),
        2 => Just(Num::Ctx),
        2 => (0usize..4).prop_map(|p| Num::Var(p, String::new())),
        1 => Just(Num::ErrStatus(String::new())),
        2 => (0i64..6).prop_map(Num::Lit),
        1 => Just(Num::Bad),
    ]
    .boxed()
}

fn data() -> BoxedStrategy<Data> {
    prop_oneof![1 => Just(Data::Same), 3 => num().prop_map(Data::Obj)].boxed()
}

fn cond() -> BoxedStrategy<Cond> {
    prop_oneof![
        1 => Just(Cond::True),
        1 => Just(Cond::False),
        4 => (-1i64..6).prop_map(Cond::Gt),
        2 => Just(Cond::Even),
        2 => ((0usize..4), (-1i64..3)).prop_map(|(p, k)| Cond::VarGt(p, String::new(), k)),
        2 => prop::sample::select(vec![400u16, 500, 503]).prop_map(|s| Cond::ErrIs(String::new(), s)),
    ]
    .boxed()
}

fn flow() -> BoxedStrategy<Flow> {
    prop_oneof![
        2 => Just(Flow::Continue),
        3 => Just(Flow::Exit),
        2 => Just(Flow::End),
        5 => (0usize..8).prop_map(Flow::Goto),
        1 => prop::sample::select(vec!["t0", "t1", "t7", "nowhere"]).prop_map(|n| Flow::Named(n.into())),
    ]
    .boxed()
}

fn template() -> BoxedStrategy<Template> {
    prop_oneof![data().prop_map(Template::Expr), num().prop_map(Template::Obj)].boxed()
}

fn function() -> BoxedStrategy<String> {
    let names: Vec<&str> = HOST.iter().chain(REUSABLE).copied().chain([GHOST]).collect();
    prop::sample::select(names).prop_map(String::from).boxed()
}

fn status() -> BoxedStrategy<u16> {
    prop::sample::select(vec![400u16, 404, 500, 503]).boxed()
}

fn leaf() -> BoxedStrategy<Body> {
    prop_oneof![
        4 => template().prop_map(Body::Set),
        4 => (function(), num()).prop_map(|(f, n)| Body::Call(f, n)),
        1 => status().prop_map(Body::Raise),
        1 => prop::sample::select(vec![0u64, 4, 50, 2_000]).prop_map(Body::Wait),
    ]
    .boxed()
}

fn decorate(body: impl Strategy<Value = Body> + 'static) -> BoxedStrategy<Task> {
    (
        body,
        prop::option::weighted(0.15, cond()),
        prop::option::weighted(0.15, data()),
        prop::option::weighted(0.15, data()),
        prop::option::weighted(0.1, data()),
        prop::option::weighted(0.05, prop::sample::select(vec![2u64, 10, 1_000])),
        prop::option::weighted(0.25, flow()),
    )
        .prop_map(|(body, when, input_from, output_as, export_as, timeout_ms, then)| Task {
            name: String::new(),
            when,
            input_from,
            output_as,
            export_as,
            timeout_ms,
            then,
            body,
        })
        .boxed()
}

fn catch(list: impl Strategy<Value = Vec<Task>> + 'static) -> BoxedStrategy<Catch> {
    let retry = (
        prop::option::weighted(0.8, 1u32..5),
        prop::sample::select(vec![0u64, 10]),
        prop::option::weighted(0.3, cond()),
    )
        .prop_map(|(count, delay_ms, when)| Retry { count, delay_ms, when });
    (
        prop::option::weighted(0.4, status()),
        prop::option::of(prop::sample::select(vec!["err", "e"]).prop_map(String::from)),
        prop::option::weighted(0.25, cond()),
        prop::option::weighted(0.35, retry),
        prop::option::weighted(0.6, list),
        prop::option::weighted(0.25, flow()),
    )
        .prop_map(|(status, as_, when, retry, handler, then)| Catch { status, as_, when, retry, handler, then })
        .boxed()
}

fn tree() -> BoxedStrategy<Task> {
    decorate(leaf())
        .prop_recursive(4, 40, 4, |inner| {
            let list = || prop::collection::vec(inner.clone(), 0..4);
            let items = prop_oneof![
                prop::collection::vec(0i64..4, 0..4).prop_map(Items::Inline),
                (1i64..4).prop_map(Items::Range),
            ];
            let names = |pool: Vec<&'static str>| prop::option::of(prop::sample::select(pool).prop_map(String::from));
            decorate(prop_oneof![
                2 => leaf(),
                2 => list().prop_map(Body::Do),
                2 => (names(vec!["x", "y"]), names(vec!["i", "j"]), items, prop::option::weighted(0.3, cond()), list())
                    .prop_map(|(each, at, items, cond, body)| Body::For { each, at, items, cond, body }),
                2 => prop::collection::vec(
                        (prop::option::weighted(0.8, cond()), flow())
                            .prop_map(|(when, then)| Case { name: String::new(), when, then }),
                        1..4,
                    )
                    .prop_map(Body::Switch),
                3 => (list(), catch(list())).prop_map(|(body, c)| Body::Try { body, catch: Box::new(c) }),
                1 => (prop::collection::vec(inner.clone(), 1..4), any::<bool>())
                    .prop_map(|(branches, compete)| Body::Fork { branches, compete }),
            ])
        })
        .boxed()
}

/// A random program; every one parses and validates (the sloppy profile's
/// may fail validation, and only there).
pub fn program(profile: Profile) -> impl Strategy<Value = Program> {
    (
        prop::collection::vec(tree(), 1..5),
        prop::collection::vec(decorate(leaf()), 2),
        prop::option::weighted(0.2, data()),
        prop::option::weighted(0.2, data()),
        any::<bool>(),
    )
        .prop_map(move |(tasks, functions, input_from, output_as, bare)| {
            let mut p = Program { input_from, output_as, functions, tasks, bare };
            fix(&mut p, profile);
            p
        })
}

/// A workflow input: `{ n }` with a small counter.
pub fn input() -> impl Strategy<Value = Value> {
    (0i64..6).prop_map(|n| json!({ "n": n }))
}

/// What is in scope where an expression is evaluated.
#[derive(Clone, Default)]
struct Scope {
    vars: Vec<String>,
    error: Option<String>,
    /// No context: the workflow's `input.from`.
    bare: bool,
}

/// Makes any generated tree valid for `profile`: names, flow targets in
/// range, variables in scope, excluded constructs rewritten.
pub fn fix(p: &mut Program, profile: Profile) {
    let top = Scope { bare: true, ..Scope::default() };
    if let Some(d) = &mut p.input_from {
        fix_data(d, &top);
    }
    if let Some(d) = &mut p.output_as {
        fix_data(d, &Scope::default());
    }
    if profile.functions {
        for (t, name) in p.functions.iter_mut().zip(REUSABLE) {
            t.name = (*name).into();
            // A function is no list: it has no sibling to jump to.
            if matches!(t.then, Some(Flow::Goto(_) | Flow::Named(_))) {
                t.then = None;
            }
            fix_task(t, 1, &Scope::default(), profile, false);
        }
    } else {
        p.functions.clear();
    }
    fix_list(&mut p.tasks, &Scope::default(), profile, true);
}

fn fix_list(tasks: &mut [Task], scope: &Scope, profile: Profile, calls_functions: bool) {
    let len = tasks.len();
    for (i, t) in tasks.iter_mut().enumerate() {
        t.name = format!("t{i}");
        fix_task(t, len, scope, profile, calls_functions);
    }
}

fn fix_flow(f: &mut Flow, len: usize, profile: Profile) {
    match f {
        Flow::Goto(_) if len == 0 => *f = Flow::Continue,
        Flow::Goto(k) => *k %= len,
        Flow::Named(_) if !profile.sloppy => *f = Flow::Exit,
        _ => {}
    }
}

fn fix_task(t: &mut Task, len: usize, scope: &Scope, profile: Profile, calls_functions: bool) {
    if let Some(c) = &mut t.when {
        fix_cond(c, scope);
    }
    for d in [&mut t.input_from, &mut t.output_as, &mut t.export_as].into_iter().flatten() {
        fix_data(d, scope);
    }
    if !profile.timeouts {
        t.timeout_ms = None;
    }
    if let Some(f) = &mut t.then {
        fix_flow(f, len, profile);
    }
    let body = std::mem::replace(&mut t.body, Body::Raise(500));
    t.body = match body {
        Body::Fork { branches, .. } if !profile.fork => Body::Do(branches),
        Body::Wait(_) if !profile.wait => Body::Set(Template::Expr(Data::Same)),
        other => other,
    };
    match &mut t.body {
        Body::Set(Template::Expr(d)) => fix_data(d, scope),
        Body::Set(Template::Obj(n)) => fix_num(n, scope),
        Body::Call(f, n) => {
            let reusable = REUSABLE.contains(&f.as_str());
            if (reusable && !(profile.functions && calls_functions)) || (f == GHOST && !profile.sloppy) {
                *f = "inc".into();
            }
            fix_num(n, scope);
        }
        Body::Do(tasks) => fix_list(tasks, scope, profile, calls_functions),
        Body::For { each, at, items: _, cond, body } => {
            let mut inner = scope.clone();
            inner.vars.push(each.clone().unwrap_or_else(|| DEFAULT_EACH.into()));
            inner.vars.push(at.clone().unwrap_or_else(|| DEFAULT_AT.into()));
            if let Some(c) = cond {
                fix_cond(c, &inner);
            }
            fix_list(body, &inner, profile, calls_functions);
        }
        Body::Switch(cases) => {
            // Only one case may go without `when`.
            let mut defaulted = false;
            for (i, c) in cases.iter_mut().enumerate() {
                c.name = format!("c{i}");
                match &mut c.when {
                    None if defaulted => c.when = Some(Cond::True),
                    None => defaulted = true,
                    Some(w) => fix_cond(w, scope),
                }
                fix_flow(&mut c.then, len, profile);
            }
        }
        Body::Try { body, catch } => {
            fix_list(body, scope, profile, calls_functions);
            let mut caught = scope.clone();
            caught.error = Some(catch.as_.clone().unwrap_or_else(|| DEFAULT_ERROR.into()));
            if let Some(w) = &mut catch.when {
                fix_cond(w, &caught);
            }
            if let Some(w) = catch.retry.as_mut().and_then(|r| r.when.as_mut()) {
                fix_cond(w, &caught);
            }
            if let Some(h) = &mut catch.handler {
                fix_list(h, &caught, profile, calls_functions);
            }
            if let Some(f) = &mut catch.then {
                fix_flow(f, len, profile);
            }
        }
        Body::Fork { branches, compete } => {
            *compete &= profile.compete;
            fix_list(branches, scope, profile, calls_functions);
            // A branch is alone in its scope.
            for b in branches.iter_mut() {
                if let Some(f) = &mut b.then {
                    fix_flow(f, 0, profile);
                }
                if let Body::Switch(cases) = &mut b.body {
                    cases.iter_mut().for_each(|c| fix_flow(&mut c.then, 0, profile));
                }
                if let Body::Try { catch, .. } = &mut b.body
                    && let Some(f) = &mut catch.then
                {
                    fix_flow(f, 0, profile);
                }
            }
        }
        Body::Raise(_) | Body::Wait(_) => {}
    }
}

fn fix_num(n: &mut Num, scope: &Scope) {
    match n {
        Num::Var(pick, name) => match scope.vars.len() {
            0 => *n = Num::Add(1),
            len => *name = scope.vars[*pick % len].clone(),
        },
        Num::ErrStatus(name) => match &scope.error {
            Some(e) => *name = e.clone(),
            None => *n = Num::Add(2),
        },
        Num::Ctx if scope.bare => *n = Num::Add(3),
        _ => {}
    }
}

fn fix_data(d: &mut Data, scope: &Scope) {
    if let Data::Obj(n) = d {
        fix_num(n, scope);
    }
}

fn fix_cond(c: &mut Cond, scope: &Scope) {
    match c {
        Cond::VarGt(pick, name, _) => match scope.vars.len() {
            0 => *c = Cond::Even,
            len => *name = scope.vars[*pick % len].clone(),
        },
        Cond::ErrIs(name, _) => match &scope.error {
            Some(e) => *name = e.clone(),
            None => *c = Cond::Gt(2),
        },
        _ => {}
    }
}

// ── 4. rendering ───────────────────────────────────────────────────────────

/// The definition, as `owf::model::parse` takes it.
pub fn definition(p: &Program) -> Value {
    let r = Render { bare: p.bare };
    let mut root = Map::new();
    root.insert("document".into(), json!({ "dsl": "1.0.0", "namespace": "test", "name": "prop", "version": "0.1.0" }));
    if let Some(d) = &p.input_from {
        root.insert("input".into(), json!({ "from": r.expr(&d.code()) }));
    }
    if let Some(d) = &p.output_as {
        root.insert("output".into(), json!({ "as": r.expr(&d.code()) }));
    }
    if !p.functions.is_empty() {
        let functions: Map<String, Value> = p.functions.iter().map(|t| (t.name.clone(), r.task(t, &[]))).collect();
        root.insert("use".into(), json!({ "functions": functions }));
    }
    root.insert("do".into(), r.list(&p.tasks));
    Value::Object(root)
}

struct Render {
    bare: bool,
}

fn wrapped(code: &str) -> Value {
    Value::String(format!("${{ {code} }}"))
}

impl Render {
    /// An expression-typed field: bare or wrapped, as the program says.
    fn expr(&self, code: &str) -> Value {
        if self.bare { Value::String(code.into()) } else { wrapped(code) }
    }

    fn list(&self, tasks: &[Task]) -> Value {
        let names: Vec<&str> = tasks.iter().map(|t| t.name.as_str()).collect();
        Value::Array(tasks.iter().map(|t| json!({ t.name.clone(): self.task(t, &names) })).collect())
    }

    fn flow(&self, f: &Flow, siblings: &[&str]) -> Value {
        match f {
            Flow::Continue => json!("continue"),
            Flow::Exit => json!("exit"),
            Flow::End => json!("end"),
            Flow::Goto(i) => json!(siblings[*i]),
            Flow::Named(n) => json!(n),
        }
    }

    fn num_value(&self, n: &Num) -> Value {
        match n {
            Num::Lit(k) => json!(k),
            other => wrapped(&other.code()),
        }
    }

    fn task(&self, t: &Task, siblings: &[&str]) -> Value {
        let mut m = Map::new();
        if let Some(c) = &t.when {
            m.insert("if".into(), self.expr(&c.code()));
        }
        if let Some(d) = &t.input_from {
            m.insert("input".into(), json!({ "from": self.expr(&d.code()) }));
        }
        if let Some(d) = &t.output_as {
            m.insert("output".into(), json!({ "as": self.expr(&d.code()) }));
        }
        if let Some(d) = &t.export_as {
            m.insert("export".into(), json!({ "as": self.expr(&d.code()) }));
        }
        if let Some(ms) = t.timeout_ms {
            m.insert("timeout".into(), json!({ "after": { "milliseconds": ms } }));
        }
        if let Some(f) = &t.then {
            m.insert("then".into(), self.flow(f, siblings));
        }
        match &t.body {
            Body::Set(Template::Expr(d)) => {
                m.insert("set".into(), wrapped(&d.code()));
            }
            Body::Set(Template::Obj(n)) => {
                m.insert("set".into(), json!({ "n": self.num_value(n) }));
            }
            Body::Call(f, n) => {
                m.insert("call".into(), json!(f));
                m.insert("with".into(), json!({ "n": self.num_value(n) }));
            }
            Body::Do(tasks) => {
                m.insert("do".into(), self.list(tasks));
            }
            Body::For { each, at, items, cond, body } => {
                let mut f = Map::new();
                if let Some(e) = each {
                    f.insert("each".into(), json!(e));
                }
                if let Some(a) = at {
                    f.insert("at".into(), json!(a));
                }
                let items = match items {
                    Items::Inline(xs) => json!(xs),
                    range => self.expr(&range.code().unwrap_or_default()),
                };
                f.insert("in".into(), items);
                m.insert("for".into(), Value::Object(f));
                if let Some(c) = cond {
                    m.insert("while".into(), self.expr(&c.code()));
                }
                m.insert("do".into(), self.list(body));
            }
            Body::Switch(cases) => {
                let cases: Vec<Value> = cases
                    .iter()
                    .map(|c| {
                        let mut case = Map::new();
                        if let Some(w) = &c.when {
                            case.insert("when".into(), self.expr(&w.code()));
                        }
                        case.insert("then".into(), self.flow(&c.then, siblings));
                        json!({ c.name.clone(): case })
                    })
                    .collect();
                m.insert("switch".into(), json!(cases));
            }
            Body::Try { body, catch } => {
                m.insert("try".into(), self.list(body));
                let mut c = Map::new();
                if let Some(s) = catch.status {
                    c.insert("errors".into(), json!({ "with": { "status": s } }));
                }
                if let Some(a) = &catch.as_ {
                    c.insert("as".into(), json!(a));
                }
                if let Some(w) = &catch.when {
                    c.insert("when".into(), self.expr(&w.code()));
                }
                if let Some(r) = &catch.retry {
                    let mut retry = Map::new();
                    if r.delay_ms > 0 {
                        retry.insert("delay".into(), json!({ "milliseconds": r.delay_ms }));
                    }
                    if let Some(count) = r.count {
                        retry.insert("limit".into(), json!({ "attempt": { "count": count } }));
                    }
                    if let Some(w) = &r.when {
                        retry.insert("when".into(), self.expr(&w.code()));
                    }
                    c.insert("retry".into(), Value::Object(retry));
                }
                if let Some(h) = &catch.handler {
                    c.insert("do".into(), self.list(h));
                }
                if let Some(f) = &catch.then {
                    c.insert("then".into(), self.flow(f, siblings));
                }
                m.insert("catch".into(), Value::Object(c));
            }
            Body::Raise(status) => {
                m.insert("raise".into(), json!({ "error": { "type": RAISED, "status": status, "title": "Raised" } }));
            }
            Body::Fork { branches, compete } => {
                let mut f = Map::new();
                f.insert("branches".into(), self.list(branches));
                if *compete {
                    f.insert("compete".into(), json!(true));
                }
                m.insert("fork".into(), Value::Object(f));
            }
            Body::Wait(ms) => {
                m.insert("wait".into(), json!({ "milliseconds": ms }));
            }
        }
        Value::Object(m)
    }
}

// ── 5. rewrites ────────────────────────────────────────────────────────────

/// Every task list in the program that is a sequence (not fork branches),
/// as a path of child steps from the top list.
pub fn lists(p: &Program) -> Vec<Vec<usize>> {
    fn walk(tasks: &[Task], path: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
        out.push(path.clone());
        for (i, t) in tasks.iter().enumerate() {
            for (slot, inner) in children(&t.body).into_iter().enumerate() {
                path.push(i * 4 + slot);
                walk(inner, path, out);
                path.pop();
            }
        }
    }
    let mut out = Vec::new();
    walk(&p.tasks, &mut Vec::new(), &mut out);
    out
}

/// The sequences directly inside a task: `do`, a loop body, a try body and
/// its handler. Fork branches are not a sequence.
fn children(b: &Body) -> Vec<&Vec<Task>> {
    match b {
        Body::Do(t) | Body::For { body: t, .. } => vec![t],
        Body::Try { body, catch } => {
            let mut v = vec![body];
            v.extend(catch.handler.as_ref());
            v
        }
        _ => Vec::new(),
    }
}

fn children_mut(b: &mut Body) -> Vec<&mut Vec<Task>> {
    match b {
        Body::Do(t) | Body::For { body: t, .. } => vec![t],
        Body::Try { body, catch } => {
            let mut v = vec![body];
            v.extend(catch.handler.as_mut());
            v
        }
        _ => Vec::new(),
    }
}

/// The list at `path` (see `lists`).
pub fn list_at<'p>(p: &'p mut Program, path: &[usize]) -> &'p mut Vec<Task> {
    let mut list = &mut p.tasks;
    for step in path {
        let (i, slot) = (step / 4, step % 4);
        list = children_mut(&mut list[i].body).into_iter().nth(slot).expect("the path names a list");
    }
    list
}

/// Visits every task, functions included.
pub fn each_task(p: &mut Program, f: &mut impl FnMut(&mut Task)) {
    fn walk(tasks: &mut [Task], f: &mut impl FnMut(&mut Task)) {
        for t in tasks {
            f(t);
            match &mut t.body {
                Body::Do(ts) | Body::For { body: ts, .. } | Body::Fork { branches: ts, .. } => walk(ts, f),
                Body::Try { body, catch } => {
                    walk(body, f);
                    if let Some(h) = &mut catch.handler {
                        walk(h, f);
                    }
                }
                _ => {}
            }
        }
    }
    for t in &mut p.functions {
        f(t);
    }
    walk(&mut p.tasks, f);
}
