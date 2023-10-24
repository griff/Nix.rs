use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::io::Write;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use bytes::Bytes;
use futures::Stream;
use tracing::debug;
use pin_project_lite::pin_project;

use crate::archive::CASE_HACK_SUFFIX;

use super::NAREvent;

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
impl Hash for CIString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.1.hash(state)
    }
}

pin_project! {
    pub struct CaseHackStream<S> {
        #[pin]
        stream: S,
        entries: HashMap<CIString, u32>,
        dir_stack: Vec<HashMap<CIString, u32>>,
    }
}

impl<Err, S: Stream<Item = Result<NAREvent, Err>>> CaseHackStream<S> {
    pub fn new(stream: S) -> CaseHackStream<S> {
        CaseHackStream {
            stream,
            entries: HashMap::new(),
            dir_stack: Vec::new(),
        }
    }
}

impl<Err, S: Stream<Item = Result<NAREvent, Err>>> Stream for CaseHackStream<S> {
    type Item = Result<NAREvent, Err>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if let Some(res) = ready!(this.stream.poll_next(cx)) {
            let item = res?;
            let changed = match item {
                re @ NAREvent::Directory => {
                    let entries = std::mem::replace(this.entries, HashMap::new());
                    this.dir_stack.push(entries);
                    re
                }
                re @ NAREvent::EndDirectory => {
                    *this.entries = this.dir_stack.pop().unwrap();
                    re
                }
                NAREvent::DirectoryEntry { name } => {
                    let lower = String::from_utf8_lossy(&name).to_lowercase();
                    let ci_str = CIString(name.clone(), lower);
                    match this.entries.entry(ci_str) {
                        Entry::Occupied(mut o) => {
                            let b_name = bstr::BStr::new(&name);
                            debug!("case collision between '{}' and '{}'", o.key(), b_name);
                            let idx = o.get() + 1;
                            let mut new_name = name.to_vec();
                            write!(new_name, "{}{}", CASE_HACK_SUFFIX, idx).unwrap();
                            o.insert(idx);
                            NAREvent::DirectoryEntry {
                                name: Bytes::from(new_name),
                            }
                        }
                        Entry::Vacant(v) => {
                            v.insert(0);
                            NAREvent::DirectoryEntry { name }
                        }
                    }
                }
                re => re,
            };

            Poll::Ready(Some(Ok(changed)))
        } else {
            Poll::Ready(None)
        }
    }
}
