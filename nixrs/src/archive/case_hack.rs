use std::{
    collections::HashMap,
    fmt, io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use bstr::ByteSlice as _;
use bytes::Bytes;
use futures::Stream;
use pin_project_lite::pin_project;
use tracing::debug;

use super::NarEvent;

pub const CASE_HACK_SUFFIX: &str = "~nix~case~hack~";

struct CIString(Bytes, String);
impl PartialEq for CIString {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(&other.1)
    }
}
impl fmt::Display for CIString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bstr = bstr::BStr::new(&self.0);
        write!(f, "{}", bstr)
    }
}
impl Eq for CIString {}
impl std::hash::Hash for CIString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.1.hash(state)
    }
}

#[derive(Default)]
struct Entries(HashMap<CIString, u32>);
impl Entries {
    fn hack_name(&mut self, name: Bytes) -> Bytes {
        use std::collections::hash_map::Entry;
        use std::io::Write;

        let lower = String::from_utf8_lossy(&name).to_lowercase();
        let ci_str = CIString(name.clone(), lower);
        match self.0.entry(ci_str) {
            Entry::Occupied(mut o) => {
                let b_name = bstr::BStr::new(&name);
                debug!("case collision between '{}' and '{}'", o.key(), b_name);
                let idx = o.get() + 1;
                let mut new_name = name.to_vec();
                write!(new_name, "{}{}", CASE_HACK_SUFFIX, idx).unwrap();
                o.insert(idx);
                Bytes::from(new_name)
            }
            Entry::Vacant(v) => {
                v.insert(0);
                name
            }
        }
    }
}

pin_project! {
    pub struct CaseHackStream<S> {
        #[pin]
        stream: S,
        entries: Entries,
        dir_stack: Vec<Entries>,
    }
}

impl<Err, S, R> CaseHackStream<S>
where
    S: Stream<Item = Result<NarEvent<R>, Err>>,
{
    pub fn new(stream: S) -> CaseHackStream<S> {
        CaseHackStream {
            stream,
            entries: Default::default(),
            dir_stack: Default::default(),
        }
    }
}

impl<Err, S, R> Stream for CaseHackStream<S>
where
    S: Stream<Item = Result<NarEvent<R>, Err>>,
{
    type Item = Result<NarEvent<R>, Err>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if let Some(res) = ready!(this.stream.poll_next(cx)) {
            let item = res?;
            let changed = match item {
                re @ NarEvent::EndDirectory => {
                    *this.entries = this.dir_stack.pop().unwrap();
                    re
                }
                NarEvent::StartDirectory { name } => {
                    let name = this.entries.hack_name(name);

                    #[allow(clippy::mutable_key_type)]
                    let entries = std::mem::take(this.entries);
                    this.dir_stack.push(entries);
                    NarEvent::StartDirectory { name }
                }
                NarEvent::Symlink { name, target } => {
                    let name = this.entries.hack_name(name);
                    NarEvent::Symlink { name, target }
                }
                NarEvent::File {
                    name,
                    executable,
                    size,
                    reader,
                } => {
                    let name = this.entries.hack_name(name);
                    NarEvent::File {
                        name,
                        executable,
                        size,
                        reader,
                    }
                }
            };

            Poll::Ready(Some(Ok(changed)))
        } else {
            Poll::Ready(None)
        }
    }
}

pin_project! {
    pub struct UncaseHackStream<S> {
        #[pin]
        stream: S,
    }
}

impl<Err, S, R> UncaseHackStream<S>
where
    S: Stream<Item = Result<NarEvent<R>, Err>>,
{
    pub fn new(stream: S) -> Self {
        Self { stream }
    }
}

fn remove_case_hack(name: &mut Bytes) {
    if let Some(pos) = name.rfind(CASE_HACK_SUFFIX) {
        debug!("removing case hack suffix from '{:?}'", name);
        name.truncate(pos);
    }
}

impl<S, R> Stream for UncaseHackStream<S>
where
    S: Stream<Item = io::Result<NarEvent<R>>>,
{
    type Item = io::Result<NarEvent<R>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if let Some(res) = ready!(self.project().stream.poll_next(cx)) {
            let mut event = res?;
            match &mut event {
                NarEvent::File {
                    name,
                    executable: _,
                    size: _,
                    reader: _,
                } => {
                    remove_case_hack(name);
                }
                NarEvent::Symlink { name, target: _ } => {
                    remove_case_hack(name);
                }
                NarEvent::StartDirectory { name } => {
                    remove_case_hack(name);
                }
                _ => {}
            };
            Poll::Ready(Some(Ok(event)))
        } else {
            Poll::Ready(None)
        }
    }
}
