use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use derive_more::Display;
use futures::Stream;
use log::debug;
use pin_project_lite::pin_project;

use crate::ready;
use crate::archive::CASE_HACK_SUFFIX;

use super::NAREvent;


#[derive(Display)]
#[display(fmt="{}", _0)]
struct CIString(Arc<String>, String);
impl PartialEq for CIString {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(&other.1)
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

impl<Err, S:Stream<Item=Result<NAREvent, Err>>> CaseHackStream<S> {
    pub fn new(stream: S) -> CaseHackStream<S> {
        CaseHackStream {
            stream,
            entries: HashMap::new(),
            dir_stack: Vec::new(),
        }
    }
}

impl<Err, S:Stream<Item=Result<NAREvent, Err>>> Stream for CaseHackStream<S> {
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
                },
                re @ NAREvent::EndDirectory => {
                    *this.entries = this.dir_stack.pop().unwrap();
                    re
                },
                NAREvent::DirectoryEntry { name } => {
                    let lower = name.to_lowercase();
                    let ci_str = CIString(name.clone(), lower);
                    match this.entries.entry(ci_str){
                        Entry::Occupied(mut o) => {
                            debug!("case collision between '{}' and '{}'", o.key(), name);
                            let idx = o.get() + 1;
                            let new_name = format!("{}{}{}", name, CASE_HACK_SUFFIX, idx);
                            o.insert(idx);
                            NAREvent::DirectoryEntry { name: Arc::new(new_name) }
                        },
                        Entry::Vacant(v) => {
                            v.insert(0);
                            NAREvent::DirectoryEntry { name }
                        }
                    }
                },
                re => re
            };

            Poll::Ready(Some(Ok(changed)))
        } else {
            Poll::Ready(None)
        }
    }
}
