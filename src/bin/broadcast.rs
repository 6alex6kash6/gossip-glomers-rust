use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use log::info;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
struct Handler {
    state: Arc<Mutex<NodeState>>,
}

#[derive(Clone, Default)]
struct NodeState {
    messages: HashSet<u64>,
    neighbors: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {},
    Read {},
    Gossip {
        seen: HashSet<u64>,
    },
    BroadcastOk {},
    ReadOk {
        messages: HashSet<u64>,
    },
    Broadcast {
        message: u64,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        let msg: Result<Request> = request.body.as_obj();
        let rt = runtime.clone();
        let s = self.clone();
        match msg {
            Ok(Request::Broadcast { message: element }) => {
                self.try_add_msg(element);
                runtime.reply_ok(request).await
            }
            Ok(Request::Gossip { seen }) => {
                self.state.lock().unwrap().messages.extend(seen);
                Ok(())
            }
            Ok(Request::Read {}) => {
                let msgs = self.get_messages();
                let res = Request::ReadOk { messages: msgs };
                runtime.reply(request, res).await
            }
            Ok(Request::Topology { topology }) => {
                let neighbours = topology.get(runtime.node_id()).unwrap();
                self.state.lock().unwrap().neighbors = neighbours.clone();
                info!("My neighbors are {:?}", neighbours);
                tokio::spawn(async move {
                    // generate gossip events
                    loop {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        let n = s.state.lock().unwrap().neighbors.clone();
                        for node in n {
                            rt.call_async(
                                node,
                                Request::Gossip {
                                    seen: s.get_messages(),
                                },
                            )
                        }
                    }
                });
                runtime.reply_ok(request).await
            }
            _ => done(runtime, request),
        }
    }
}

impl Handler {
    fn try_add_msg(&self, msg: u64) -> bool {
        let mut s = self.state.lock().unwrap();
        if !s.messages.contains(&msg) {
            s.messages.insert(msg);
            return true;
        }
        false
    }

    fn get_messages(&self) -> HashSet<u64> {
        self.state
            .lock()
            .unwrap()
            .messages
            .iter()
            .copied()
            .collect()
    }
}

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}
