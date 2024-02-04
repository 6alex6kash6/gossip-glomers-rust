use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use log::info;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Send {
        msg: u64,
        key: String,
    },
    Poll {
        offsets: HashMap<String, u64>,
    },
    CommitOffsets {
        offsets: HashMap<String, u64>,
    },
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    Init {
        node_ids: Vec<String>,
        node_id: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    SendOk {
        offset: u64,
    },
    PollOk {
        msgs: HashMap<String, Vec<Vec<u64>>>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: HashMap<String, u64>,
    },
}

#[derive(Clone)]
struct Handler {
    state: Arc<Mutex<NodeState>>,
}

#[derive(Clone, Default, Debug)]
struct Log {
    offset: u64,
    message: u64,
}

#[derive(Clone, Default)]
struct NodeState {
    logs: HashMap<String, Vec<Log>>,
    latest_offsets: HashMap<String, u64>,
    commited_offsets: HashMap<String, u64>,
}

impl NodeState {
    fn get_logs_from_offset(&self, key: &str, offset: u64) -> Option<Vec<Log>> {
        if let Some(logs) = self.logs.get(key) {
            if let Some(start_index) = logs.iter().position(|log| log.offset == offset) {
                let end_index = start_index + 3;
                let end_index = if end_index > logs.len() {
                    logs.len()
                } else {
                    end_index
                };
                let selected_logs = logs[start_index..end_index].to_vec();

                return Some(selected_logs);
            }
        }
        None
    }
}

impl Handler {
    fn from_init() -> Self {
        Self {
            state: Arc::new(Mutex::new(NodeState::default())),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        let msg: Result<Request> = request.body.as_obj();
        let mut state = self.state.lock().await;
        match msg {
            Ok(Request::Send { msg, key }) => {
                let mut offset: u64 = 0;
                if let Some(&key_offset) = state.latest_offsets.get(&key) {
                    offset = key_offset + 1;
                    let log = Log {
                        offset,
                        message: msg,
                    };

                    if let Some(key_logs) = state.logs.get_mut(&key) {
                        key_logs.push(log)
                    } else {
                        state.logs.insert(key.clone(), vec![log]);
                    };
                } else {
                    state.logs.insert(
                        key.clone(),
                        vec![Log {
                            offset,
                            message: msg,
                        }],
                    );
                }
                state.latest_offsets.insert(key, offset);
                runtime.reply(request, Response::SendOk { offset }).await
            }
            Ok(Request::Poll { offsets }) => {
                let mut key_logs = HashMap::new();
                let mut result_map = HashMap::new();
                for (key, off) in offsets {
                    if let Some(logs) = state.get_logs_from_offset(&key, off) {
                        key_logs.insert(key.clone(), logs);
                    };
                }

                for (key, logs) in key_logs {
                    let mapped_logs: Vec<Vec<u64>> = logs
                        .into_iter()
                        .map(|log| vec![log.offset, log.message])
                        .collect();
                    result_map.insert(key, mapped_logs);
                }
                runtime
                    .reply(request, Response::PollOk { msgs: result_map })
                    .await
            }
            Ok(Request::CommitOffsets { offsets }) => {
                state.commited_offsets = offsets;
                runtime.reply(request, Response::CommitOffsetsOk {}).await
            }
            Ok(Request::ListCommittedOffsets { keys }) => {
                let mut keys_offsets = HashMap::new();
                let commited_offsets = state.commited_offsets.clone();
                let keys = keys.clone();
                for key in keys {
                    if let Some(val) = commited_offsets.get(&key) {
                        keys_offsets.insert(key, *val);
                    }
                }
                runtime
                    .reply(
                        request,
                        Response::ListCommittedOffsetsOk {
                            offsets: commited_offsets,
                        },
                    )
                    .await
            }
            _ => done(runtime, request),
        }
    }
}

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::from_init());
    Runtime::new().with_handler(handler).run().await
}
