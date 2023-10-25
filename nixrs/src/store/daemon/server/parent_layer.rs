use tracing::{
    dispatcher::{get_default, with_default},
    span, Dispatch, Event, Subscriber,
};
use tracing_subscriber::{layer, registry::LookupSpan, Layer};

struct ParentId(span::Id);

pub struct ParentLayer {
    parent: Dispatch,
    log: bool,
}
impl ParentLayer {
    pub fn new() -> ParentLayer {
        let parent = get_default(|d| d.clone());
        ParentLayer { parent, log: false }
    }
    pub fn new_debug() -> ParentLayer {
        let parent = get_default(|d| d.clone());
        eprintln!("Parent {:?}", parent);
        ParentLayer { parent, log: true }
    }
}

impl<S> Layer<S> for ParentLayer
where
    for<'lookup> S: Subscriber + LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
        if let Some(meta) = ctx.metadata(id) {
            if self.parent.enabled(meta) {
                if let Some(span) = ctx.span(id) {
                    let parent_id = self.parent.new_span(attrs);
                    if self.log {
                        eprintln!("Parent new span {:?} {}", parent_id, meta.name());
                    }
                    span.extensions_mut().insert(ParentId(parent_id));
                }
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: layer::Context<'_, S>) {
        let meta = event.metadata();
        if self.parent.enabled(meta) {
            if self.log {
                eprintln!("Parent event {} {}", meta.target(), meta.name());
            }
            self.parent.event(event);
        }
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: layer::Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            if let Some(parent_id) = span.extensions().get::<ParentId>() {
                if self.log {
                    eprintln!("Parent record {:?} {}", parent_id.0, span.name());
                }
                self.parent.record(&parent_id.0, values);
            }
        }
    }

    fn on_follows_from(&self, id: &span::Id, follows: &span::Id, ctx: layer::Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            if let Some(parent_id) = span.extensions().get::<ParentId>() {
                if self.log {
                    eprintln!("Parent follows {:?} {}", parent_id.0, span.name());
                }
                self.parent.record_follows_from(&parent_id.0, follows);
            }
        }
    }

    fn on_id_change(&self, old: &span::Id, new: &span::Id, ctx: layer::Context<'_, S>) {
        if self.log {
            eprintln!("Id change from {:?} to {:?}", old, new);
        }
        if let Some(span) = ctx.span(&old) {
            if let Some(parent_id) = span.extensions().get::<ParentId>() {
                if self.log {
                    eprintln!("Parent cloning {:?} {}", parent_id.0, span.name());
                }
                if let Some(new_span) = ctx.span(&new) {
                    let new_parent_id = self.parent.clone_span(&parent_id.0);
                    new_span.extensions_mut().insert(ParentId(new_parent_id));
                }
            }
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: layer::Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            if let Some(parent_id) = span.extensions().get::<ParentId>() {
                if self.log {
                    eprintln!("Parent enter {:?} {}", parent_id.0, span.name());
                }
                self.parent.enter(&parent_id.0);
            }
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: layer::Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            if let Some(parent_id) = span.extensions().get::<ParentId>() {
                if self.log {
                    eprintln!("Parent exit {:?} {:?} {}", id, parent_id.0, span.name());
                }
                with_default(&self.parent, || {
                    if self.log {
                        eprintln!(
                            "Actual Parent exit {:?} {:?} {}",
                            id,
                            parent_id.0,
                            span.name()
                        );
                    }
                    self.parent.exit(&parent_id.0);
                });
                if self.log {
                    eprintln!("Parent after exit {:?}", parent_id.0);
                }
            }
        }
    }

    fn on_close(&self, id: span::Id, ctx: layer::Context<'_, S>) {
        if self.log {
            eprintln!("Span close {:?}", id);
        }
        if let Some(span) = ctx.span(&id) {
            if self.log {
                eprintln!("Found Span close {:?} {}", id, span.name());
            }
            if let Some(parent_id) = span.extensions().get::<ParentId>() {
                if self.log {
                    eprintln!("Parent close");
                }
                self.parent.try_close(parent_id.0.clone());
            }
        }
        if self.log {
            eprintln!("Span after close {:?}", id);
        }
    }
}
