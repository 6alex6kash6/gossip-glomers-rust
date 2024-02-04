use maelstrom::kv::{lin_kv, seq_kv, Storage, KV};
use std::collections::HashMap;
use std::sync::Arc;

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
        offset: i32,
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
    kv: Storage,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
struct Log {
    offset: u64,
    message: u64,
}

impl Handler {
    fn from_init(runtime: Runtime) -> Self {
        Self {
            kv: lin_kv(runtime),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        let msg: Result<Request> = request.body.as_obj();
        let logs_prefix = String::from("logs");
        let latest_offsets_prefix = String::from("latest_offsets");
        let commited_offsets_prefix = String::from("commited_offsets");
        let (_, mut handler) = tokio_context::context::Context::new();
        match msg {
            Ok(Request::Send { msg, key }) => {
                let mut offset = self
                    .kv
                    .get::<i32>(
                        handler.spawn_ctx(),
                        format!("{latest_offsets_prefix}_{key}"),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        info!("error while getting value");
                        0
                    });

                while let Err(_) = self
                    .kv
                    .cas(
                        handler.spawn_ctx(),
                        format!("{latest_offsets_prefix}_{key}"),
                        offset - 1,
                        offset,
                        true,
                    )
                    .await
                {
                    offset += 1
                }

                self.kv
                    .put(
                        handler.spawn_ctx(),
                        format!("{logs_prefix}_{key}_{offset}"),
                        msg,
                    )
                    .await
                    .unwrap_or_else(|_| info!("error while writing value"));
                runtime.reply(request, Response::SendOk { offset }).await
            }
            Ok(Request::Poll { offsets }) => {
                //                let mut key_logs = HashMap::new();
                let mut result_map: HashMap<String, Vec<Vec<u64>>> = HashMap::new();
                for (key, mut off) in offsets {
                    let cloned_key = key.clone();
                    while let Ok(val) = self
                        .kv
                        .get::<u64>(
                            handler.spawn_ctx(),
                            format!("{logs_prefix}_{cloned_key}_{off}"),
                        )
                        .await
                    {
                        if let Some(logs) = result_map.get_mut(&key) {
                            logs.push(vec![off, val]);
                        } else {
                            result_map.insert(key.clone(), vec![vec![off, val]]);
                        };
                        off += 1;
                    }
                }
                runtime
                    .reply(request, Response::PollOk { msgs: result_map })
                    .await
            }
            Ok(Request::CommitOffsets { offsets }) => {
                for (key, offset) in offsets {
                    self.kv
                        .put(
                            handler.spawn_ctx(),
                            format!("{commited_offsets_prefix}_{key}"),
                            offset,
                        )
                        .await
                        .unwrap_or_else(|_| info!("error while writing value"));
                }
                runtime.reply(request, Response::CommitOffsetsOk {}).await
            }
            Ok(Request::ListCommittedOffsets { keys }) => {
                let mut keys_offsets = HashMap::new();
                let keys = keys.clone();
                for key in keys {
                    if let Ok(commited_offset) = self
                        .kv
                        .get::<u64>(
                            handler.spawn_ctx(),
                            format!("{commited_offsets_prefix}_{key}"),
                        )
                        .await
                    {
                        keys_offsets.insert(key, commited_offset);
                    };
                }
                runtime
                    .reply(
                        request,
                        Response::ListCommittedOffsetsOk {
                            offsets: keys_offsets,
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
    let r = Runtime::new();
    let handler = Arc::new(Handler::from_init(r.clone()));
    r.with_handler(handler).run().await
}
