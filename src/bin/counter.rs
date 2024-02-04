use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use log::info;
use maelstrom::kv::{seq_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Add {
        delta: u64,
    },
    Read {},
    ReadBucket {},
    Init {
        node_ids: Vec<String>,
        node_id: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    AddOk {},
    InitOk {},
    ReadOk { value: u64 },
    ReadBucketOk { value: u64 },
}

#[derive(Clone)]
struct Handler {
    state: Arc<Mutex<NodeState>>,
    kv: Storage,
}

#[derive(Clone)]
struct NodeState {
    nodes: Vec<String>,
}

impl Handler {
    fn from_init(runtime: Runtime) -> Self {
        Self {
            state: Arc::new(Mutex::new(NodeState { nodes: Vec::new() })),
            kv: seq_kv(runtime),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        let msg: Result<Request> = request.body.as_obj();
        let node_id = request.dest.clone();
        let (_, mut handler) = tokio_context::context::Context::new();
        match msg {
            Ok(Request::Init { node_ids, node_id }) => {
                self.kv
                    .put(handler.spawn_ctx(), node_id, 0)
                    .await
                    .unwrap_or_else(|_| info!("error while writing value"));
                self.state.lock().unwrap().nodes = node_ids;
                Ok(())
            }
            Ok(Request::ReadBucket {}) => {
                let counter = self
                    .kv
                    .get::<u64>(handler.spawn_ctx(), node_id.to_string())
                    .await
                    .unwrap_or_else(|_| {
                        info!("error while getting value");
                        0
                    });
                runtime
                    .reply(request, Response::ReadBucketOk { value: counter })
                    .await
            }
            Ok(Request::Add { delta }) => {
                let ctx = handler.spawn_ctx();
                let counter = self
                    .kv
                    .get::<u64>(ctx, node_id.to_string())
                    .await
                    .unwrap_or_else(|_| {
                        info!("error while getting value");
                        0
                    });
                let updated_val = counter + delta;
                self.kv
                    .cas(
                        handler.spawn_ctx(),
                        node_id.to_string(),
                        counter,
                        updated_val,
                        true,
                    )
                    .await
                    .unwrap_or_else(|_| info!("error while writing value"));
                runtime.reply_ok(request).await
            }
            Ok(Request::Read {}) => {
                let mut sum: u64 = 0;
                let nodes = self.state.lock().unwrap().nodes.clone();
                for node in nodes {
                    if node == node_id {
                        let ctx = handler.spawn_ctx();
                        let counter =
                            self.kv
                                .get::<u64>(ctx, node.clone())
                                .await
                                .unwrap_or_else(|_| {
                                    info!("error while getting value");
                                    0
                                });
                        sum = sum + counter;
                    } else {
                        let mut res = runtime.rpc(node, Request::ReadBucket {}).await.unwrap();
                        let msg = res.done_with(handler.spawn_ctx()).await.unwrap();
                        let data = msg.body.as_obj::<Response>()?;
                        match data {
                            Response::ReadBucketOk { value } => sum = sum + value,
                            _ => sum = sum + 0,
                        }
                    }
                }

                runtime
                    .reply(request, Response::ReadOk { value: sum })
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
