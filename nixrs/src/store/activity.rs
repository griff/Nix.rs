use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use derive_more::{LowerHex, UpperHex};
use tracing::event;
use tracing::field::{Field, Visit};
use tracing::span;
use tracing::span::{Attributes, Id};
use tracing::trace;
use tracing::{Event, Level, Span};

use crate::num_enum::num_enum;

use super::error::Verbosity;

pub type ActivityId = u64;

pub struct Activity {
    pub span: Span,
}

#[macro_export]
macro_rules! activity {
    ($level:expr, $act_type:expr, $msg:expr, $($fields:tt)*) => {{
        let level : u64 = $level.into();
        let activity_type : u64 = $act_type.into();
        let span = tracing::span!($level.to_tracing(), $crate::store::activity::ACTIVITY_NAME, level, activity_type, message=$msg, $($fields)*);
        $crate::store::activity::Activity { span }
    }}
}

#[derive(Debug, Clone)]
pub enum LoggerField {
    Int(u64),
    String(String),
}

impl LoggerField {
    fn as_value(&self) -> Box<dyn tracing::Value> {
        match self {
            LoggerField::Int(i) => Box::new(*i),
            LoggerField::String(s) => Box::new(s.clone()),
        }
    }
}

num_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub enum LoggerFieldType {
        Invalid(u64),
        Int = 0,
        String = 1,
    }
}

num_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, UpperHex, LowerHex)]
    pub enum ActivityType {
        Invalid(u64),
        Unknown = 0,
        CopyPath = 100,
        FileTransfer = 101,
        Realise = 102,
        CopyPaths = 103,
        Builds = 104,
        Build = 105,
        OptimiseStore = 106,
        VerifyPaths = 107,
        Substitute = 108,
        QueryPathInfo = 109,
        PostBuildHook = 110,
        BuildWaiting = 111,
    }
}

num_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, UpperHex, LowerHex)]
    pub enum ResultType {
        Invalid(u64),
        FileLinked = 100,
        BuildLogLine = 101,
        UntrustedPath = 102,
        CorruptedPath = 103,
        SetPhase = 104,
        Progress = 105,
        SetExpected = 106,
        PostBuildLogLine = 107,
    }
}

pub const ACTIVITY_TARGET: &str = "nix::activity";
pub const ACTIVITY_NAME: &str = "nix.activity";
pub const RESULT_TARGET: &str = "nix::activity::result";

#[derive(Debug)]
pub struct StartActivity {
    pub act: ActivityId,
    pub level: Verbosity,
    pub activity_type: ActivityType,
    pub text: String,
    pub fields: Vec<LoggerField>,
    pub parent: ActivityId,
}

impl<'a> TryFrom<&'a Attributes<'a>> for StartActivity {
    type Error = ();

    fn try_from(value: &'a Attributes<'a>) -> Result<Self, Self::Error> {
        let mut visitor = StartActivityVisitor::default();
        value.record(&mut visitor);
        eprintln!("Activity {:?}", visitor);
        if let Some(result) = visitor.into_activity() {
            Ok(result)
        } else {
            Err(())
        }
    }
}

#[derive(Debug, Default)]
struct StartActivityVisitor {
    parent: Option<ActivityId>,
    act: Option<ActivityId>,
    level: Option<Verbosity>,
    activity_type: Option<ActivityType>,
    text: Option<String>,
    pub fields: Vec<Option<LoggerField>>,
}

impl StartActivityVisitor {
    pub fn into_activity(self) -> Option<StartActivity> {
        let parent = if let Some(parent_id) = self.parent {
            parent_id
        } else {
            0
        };
        let act = self.act?;
        let level = self.level?;
        let activity_type = self.activity_type?;
        let text = self.text?;
        let mut fields = Vec::with_capacity(self.fields.len());
        for (idx, f) in self.fields.into_iter().enumerate() {
            if let Some(field) = f {
                fields.push(field);
            } else {
                eprintln!("Missing field {}", idx);
            }
        }
        Some(StartActivity {
            parent,
            act,
            level,
            activity_type,
            text,
            fields,
        })
    }
}

impl Visit for StartActivityVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "parent" => {
                self.parent = Some(value);
            }
            "act" => {
                self.act = Some(value);
            }
            "level" => {
                self.level = Some(value.into());
            }
            "activity_type" => {
                self.activity_type = Some(value.into());
            }
            field_name if field_name.starts_with("field") => {
                if let Ok(idx) = field_name.strip_prefix("field").unwrap().parse::<usize>() {
                    eprintln!("Insert field {}>={}", idx, self.fields.len());
                    if idx >= self.fields.len() {
                        eprintln!("Extend {:?}", (self.fields.len()..=idx).map(|_| 0));
                        self.fields.extend((self.fields.len()..=idx).map(|_| None));
                    }
                    self.fields[idx] = Some(LoggerField::Int(value));
                }
            }
            _ => (),
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        let field_name = field.name();
        if let Some(idx_s) = field_name.strip_prefix("field") {
            if let Ok(idx) = idx_s.parse::<usize>() {
                eprintln!("Insert field {}>={}", idx, self.fields.len());
                if idx >= self.fields.len() {
                    eprintln!("Extend {:?}", (self.fields.len()..=idx).map(|_| 0));
                    self.fields.extend((self.fields.len()..=idx).map(|_| None));
                }
                self.fields[idx] = Some(LoggerField::String(value.to_string()));
            }
        } else if field_name == "text" || field_name == "message" {
            self.text = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if let Some(idx_s) = field.name().strip_prefix("field") {
            if let Ok(idx) = idx_s.parse::<usize>() {
                eprintln!("Insert field {}>={}", idx, self.fields.len());
                if idx >= self.fields.len() {
                    eprintln!("Extend {:?}", (self.fields.len()..=idx).map(|_| 0));
                    self.fields.extend((self.fields.len()..=idx).map(|_| None));
                }
                self.fields[idx] = Some(LoggerField::String(format!("{:?}", value)));
            }
        }
    }
}

#[derive(Debug)]
pub struct ActivityResult {
    pub act: ActivityId,
    pub result_type: ResultType,
    pub fields: Vec<LoggerField>,
}

impl ActivityResult {
    pub fn from_event(event: &Event<'_>, parent: Id) -> Result<Self, ()> {
        let mut visitor = ActivityResultVisitor::default();
        event.record(&mut visitor);
        if let Some(result) = visitor.into_result(parent) {
            Ok(result)
        } else {
            Err(())
        }
    }
}

#[derive(Default)]
pub struct ActivityResultVisitor {
    act: Option<ActivityId>,
    result_type: Option<ResultType>,
    fields: Vec<Option<LoggerField>>,
}

impl ActivityResultVisitor {
    pub fn into_result(self, parent: Id) -> Option<ActivityResult> {
        let act = self.act.unwrap_or(parent.into_u64());
        let result_type = self.result_type?;
        let mut fields = Vec::with_capacity(self.fields.len());
        for (idx, f) in self.fields.into_iter().enumerate() {
            if let Some(field) = f {
                fields.push(field);
            } else {
                eprintln!("Missing field {}", idx);
            }
        }
        Some(ActivityResult {
            act,
            result_type,
            fields,
        })
    }
}

impl Visit for ActivityResultVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "parent" => {
                self.act = Some(value);
            }
            "result_type" => {
                self.result_type = Some(value.into());
            }
            field_name if field_name.starts_with("field") => {
                if let Ok(idx) = field_name.strip_prefix("field").unwrap().parse::<usize>() {
                    eprintln!("Insert field {}>={}", idx, self.fields.len());
                    if idx >= self.fields.len() {
                        eprintln!("Extend {:?}", (self.fields.len()..=idx).map(|_| 0));
                        self.fields.extend((self.fields.len()..=idx).map(|_| None));
                    }
                    self.fields[idx] = Some(LoggerField::Int(value));
                }
            }
            _ => (),
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if let Some(idx_s) = field.name().strip_prefix("field") {
            if let Ok(idx) = idx_s.parse::<usize>() {
                eprintln!("Insert field {}>={}", idx, self.fields.len());
                if idx >= self.fields.len() {
                    eprintln!("Extend {:?}", (self.fields.len()..=idx).map(|_| 0));
                    self.fields.extend((self.fields.len()..=idx).map(|_| None));
                }
                self.fields[idx] = Some(LoggerField::String(value.to_string()));
            }
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if let Some(idx_s) = field.name().strip_prefix("field") {
            if let Ok(idx) = idx_s.parse::<usize>() {
                eprintln!("Insert field {}>={}", idx, self.fields.len());
                if idx >= self.fields.len() {
                    eprintln!("Extend {:?}", (self.fields.len()..=idx).map(|_| 0));
                    self.fields.extend((self.fields.len()..=idx).map(|_| None));
                }
                self.fields[idx] = Some(LoggerField::String(format!("{:?}", value)));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActivityLogger {
    map: Arc<Mutex<BTreeMap<ActivityId, Span>>>,
}

impl ActivityLogger {
    pub fn new() -> ActivityLogger {
        ActivityLogger {
            map: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    pub fn start_activity(
        &mut self,
        act: ActivityId,
        lvl: Verbosity,
        act_type: ActivityType,
        message: String,
        fields: Vec<LoggerField>,
        parent: ActivityId,
    ) {
        let activity = StartActivity {
            act,
            level: lvl,
            activity_type: act_type,
            text: message.clone(),
            fields: fields.clone(),
            parent,
        };
        trace!(?activity, "Start activity");

        let mut map = self.map.lock().unwrap();
        let level: u64 = lvl.into();
        let activity_type: u64 = act_type.into();
        let span = if let Some(parent_span) = map.get(&parent) {
            match lvl.into() {
                Level::ERROR => {
                    remote!(parent: parent_span, Level::ERROR, act, level, activity_type, message, parent, fields)
                }
                Level::WARN => {
                    remote!(parent: parent_span, Level::WARN, act, level, activity_type, message, parent, fields)
                }
                Level::INFO => {
                    remote!(parent: parent_span, Level::INFO, act, level, activity_type, message, parent, fields)
                }
                Level::DEBUG => {
                    remote!(parent: parent_span, Level::DEBUG, act, level, activity_type, message, parent, fields)
                }
                Level::TRACE => {
                    remote!(parent: parent_span, Level::TRACE, act, level, activity_type, message, parent, fields)
                }
            }
        } else {
            match lvl.into() {
                Level::ERROR => {
                    remote!(Level::ERROR, act, level, activity_type, message, fields)
                }
                Level::WARN => {
                    remote!(Level::WARN, act, level, activity_type, message, fields)
                }
                Level::INFO => {
                    remote!(Level::INFO, act, level, activity_type, message, fields)
                }
                Level::DEBUG => {
                    remote!(Level::DEBUG, act, level, activity_type, message, fields)
                }
                Level::TRACE => {
                    remote!(Level::TRACE, act, level, activity_type, message, fields)
                }
            }
        };
        map.insert(act, span);
    }

    pub fn stop_activity(&mut self, act: ActivityId) {
        let mut map = self.map.lock().unwrap();
        map.remove(&act);
    }

    pub fn result(&mut self, parent: ActivityId, res_type: ResultType, fields: Vec<LoggerField>) {
        let result = ActivityResult {
            act: parent,
            result_type: res_type,
            fields: fields.clone(),
        };
        trace!(?result, "Activity result");
        let map = self.map.lock().unwrap();
        if let Some(span) = map.get(&parent) {
            let result_type: u64 = res_type.into();
            expand_fields!( event, @ { target: RESULT_TARGET, parent: span, Level::ERROR, parent, result_type }, fields)
        }
    }
}

macro_rules! remote {
    (parent: $parent_span:expr, $lvl:expr, $act:ident, $level:ident, $activity_type:ident, $text:ident, $parent:ident, $fields:ident) => {
        expand_fields!( span, @ { target: ACTIVITY_TARGET, parent: $parent_span, $lvl, ACTIVITY_NAME, $act, $level, $activity_type, $text, $parent }, $fields)
    };
    ($lvl:expr, $act:ident, $level:ident, $activity_type:ident, $text:ident, $fields:ident) => {
        expand_fields!( span, @ { target: ACTIVITY_TARGET, $lvl, ACTIVITY_NAME, $act, $level, $activity_type, $text }, $fields)
    };
}
pub(crate) use remote;

macro_rules! expand_fields {
    ($mac:ident, @ { $(,)* $($out:tt)* }, $fields:ident) => {
        match $fields.len() {
            0 => expand_fields!( $mac, @ { $($out)* } ),
            1 => expand_fields!( $mac, @ { $($out)* }, 1, $fields ),
            2 => expand_fields!( $mac, @ { $($out)* }, 2, $fields ),
            3 => expand_fields!( $mac, @ { $($out)* }, 3, $fields ),
            4 => expand_fields!( $mac, @ { $($out)* }, 4, $fields ),
            5 => expand_fields!( $mac, @ { $($out)* }, 5, $fields ),
            6 => expand_fields!( $mac, @ { $($out)* }, 6, $fields ),
            7 => expand_fields!( $mac, @ { $($out)* }, 7, $fields ),
            8 => expand_fields!( $mac, @ { $($out)* }, 8, $fields ),
            9 => expand_fields!( $mac, @ { $($out)* }, 9, $fields ),
            10 => expand_fields!( $mac, @ { $($out)* }, 10, $fields ),
            11 => expand_fields!( $mac, @ { $($out)* }, 11, $fields ),
            12 => expand_fields!( $mac, @ { $($out)* }, 12, $fields ),
            13 => expand_fields!( $mac, @ { $($out)* }, 13, $fields ),
            14 => expand_fields!( $mac, @ { $($out)* }, 14, $fields ),
            15 => expand_fields!( $mac, @ { $($out)* }, 15, $fields ),
            16 => expand_fields!( $mac, @ { $($out)* }, 16, $fields ),
            17 => expand_fields!( $mac, @ { $($out)* }, 17, $fields ),
            18 => expand_fields!( $mac, @ { $($out)* }, 18, $fields ),
            19 => expand_fields!( $mac, @ { $($out)* }, 19, $fields ),
            20 => expand_fields!( $mac, @ { $($out)* }, 20, $fields ),
            21 => expand_fields!( $mac, @ { $($out)* }, 21, $fields ),
            22 => expand_fields!( $mac, @ { $($out)* }, 22, $fields ),
            23 => expand_fields!( $mac, @ { $($out)* }, 23, $fields ),
            24 => expand_fields!( $mac, @ { $($out)* }, 24, $fields ),
            25 => expand_fields!( $mac, @ { $($out)* }, 25, $fields ),
            s => {
                event!(Level::WARN, "Remote activity has too many fields {} > 25", s);
                expand_fields!( $mac, @ { $($out)* }, 25, $fields )
            }
        }
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 25, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field24 = $fields[24].as_value() },
            24, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 24, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field23 = $fields[23].as_value() },
            23, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 23, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field22 = $fields[22].as_value() },
            22, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 22, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field21 = $fields[21].as_value() },
            21, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 21, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field20 = $fields[20].as_value() },
            20, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 20, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field19 = $fields[19].as_value() },
            19, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 19, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field18 = $fields[18].as_value() },
            18, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 18, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field17 = $fields[17].as_value() },
            17, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 17, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field16 = $fields[16].as_value() },
            16, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 16, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field15 = $fields[15].as_value() },
            15, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 15, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field14 = $fields[14].as_value() },
            14, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 14, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field13 = $fields[13].as_value() },
            13, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 13, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field12 = $fields[12].as_value() },
            12, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 12, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field11 = $fields[11].as_value() },
            11, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 11, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field10 = $fields[10].as_value() },
            10, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 10, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field9 = $fields[9].as_value() },
            9, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 9, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field8 = $fields[8].as_value() },
            8, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 8, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field7 = $fields[7].as_value() },
            7, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 7, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field6 = $fields[6].as_value() },
            6, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 6, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field5 = $fields[5].as_value() },
            5, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 5, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field4 = $fields[4].as_value() },
            4, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 4, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field3 = $fields[3].as_value() },
            3, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 3, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field2 = $fields[2].as_value() },
            2, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 2, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field1 = $fields[1].as_value() },
            1, $fields
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }, 1, $fields:ident) => {
        expand_fields!(
            $mac,
            @ { $($out)*, field0 = $fields[0].as_value() }
        )
    };
    ($mac:ident, @ { $(,)* $($out:tt)* }) => {
        $mac!($($out)*)
    };
}
pub(crate) use expand_fields;
