use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum TxnOperations {
    W,
    R,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    TxnOk {
        txn: Vec<(TxnOperations, u64, Option<u64>)>,
    },
    SyncOk {},
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Txn {
        txn: Vec<(TxnOperations, u64, Option<u64>)>,
    },
    Sync {
        changes: HashMap<u64, u64>,
    },
    Init {
        node_ids: Vec<String>,
        node_id: String,
    },
}

#[derive(Clone, Default)]
struct NodeState {
    kv: HashMap<u64, u64>,
    nodes: Vec<String>,
    node_id: String,
}
#[derive(Clone, Default)]
struct Handler {
    state: Arc<Mutex<NodeState>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        let msg: Result<Request> = request.body.as_obj();
        let mut state = self.state.lock().await;
        match msg {
            Ok(Request::Init { node_ids, node_id }) => {
                state.nodes = node_ids;
                state.node_id = node_id;
                Ok(())
            }
            Ok(Request::Sync { changes }) => {
                for (key, val) in changes {
                    state.kv.insert(key, val);
                }
                runtime.reply(request, Response::SyncOk {}).await
            }
            Ok(Request::Txn { txn }) => {
                let mut response: Vec<(TxnOperations, u64, Option<u64>)> = Vec::new();
                let mut changes: HashMap<u64, u64> = HashMap::new();
                for operation in txn {
                    match operation.0 {
                        TxnOperations::W => {
                            let key = operation.1;
                            let val = operation.2.unwrap();
                            state.kv.insert(key, val);
                            changes.insert(key, val);
                            response.push(operation);
                        }
                        TxnOperations::R => {
                            if let Some(&val) = state.kv.get(&operation.1) {
                                response.push((TxnOperations::R, operation.1, Some(val)));
                            } else {
                                response.push((TxnOperations::R, operation.1, None));
                            }
                        }
                    }
                }

                for node in state.nodes.clone() {
                    if node != state.node_id {
                        let ch = changes.clone();
                        let rt = runtime.clone();
                        tokio::spawn(async move {
                            let (ctx, _) = tokio_context::context::Context::new();
                            let mut res = rt
                                .rpc(
                                    node,
                                    Request::Sync {
                                        changes: ch,
                                    },
                                )
                                .await
                                .unwrap();
                            let msg = res.done_with(ctx).await.unwrap();
                            let data = msg.body.as_obj::<Response>().unwrap();
                            //                            while let Err(_) = runtime.call_async(node, Request::Sync { changes }) {
                            //                                tokio::time::sleep(Duration::from_millis(300)).await;
                            //                                let n = s.state.lock().unwrap().neighbors.clone();
                            //                            }
                        });
                    }
                }

                runtime
                    .reply(request, Response::TxnOk { txn: response })
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
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}
