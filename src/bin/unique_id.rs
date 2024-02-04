use std::sync::Arc;

use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
struct Handler {}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
struct UniqueId {
    #[serde(rename = "type")]
    typ: String,
    id: String,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        if request.get_type() == "generate" {
            let mut res = UniqueId::default();
            res.typ = String::from("generate_ok");
            res.id = generate_id(&runtime);
            return runtime.reply(request, res).await;
        }
        done(runtime, request)
    }
}

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

fn generate_id(runtime: &Runtime) -> String {
    let id = format!(
        "{node_id}{msg_id}",
        node_id = runtime.node_id(),
        msg_id = runtime.next_msg_id()
    );
    id
}
