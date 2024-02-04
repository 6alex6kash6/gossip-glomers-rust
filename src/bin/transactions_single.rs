use std::collections::HashMap;
use std::sync::Arc;

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
enum Messages {
    Txn {
        txn: Vec<(TxnOperations, u64, Option<u64>)>,
    },
    TxnOk {
        txn: Vec<(TxnOperations, u64, Option<u64>)>,
    },
}

#[derive(Clone, Default)]
struct Handler {
    kv: Arc<Mutex<HashMap<u64, u64>>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        let msg: Result<Messages> = request.body.as_obj();
        let mut kv = self.kv.lock().await;
        match msg {
            Ok(Messages::Txn { txn }) => {
                let mut response: Vec<(TxnOperations, u64, Option<u64>)> = Vec::new();
                for operation in txn {
                    match operation.0 {
                        TxnOperations::W => {
                            kv.insert(operation.1, operation.2.unwrap());
                            response.push(operation);
                        }
                        TxnOperations::R => {
                            if let Some(&val) = kv.get(&operation.1) {
                                response.push((TxnOperations::R, operation.1, Some(val)));
                            } else {
                                response.push((TxnOperations::R, operation.1, None));
                            }
                        }
                    }
                }
                runtime
                    .reply(request, Messages::TxnOk { txn: response })
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
