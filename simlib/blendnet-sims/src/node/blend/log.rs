use serde::{Deserialize, Serialize};

#[macro_export]
macro_rules! log {
    ($topic:expr, $msg:expr) => {
        println!(
            "{}",
            serde_json::to_string(&$crate::node::blend::log::TopicLog {
                topic: $topic.to_string(),
                message: $msg
            })
            .unwrap()
        );
    };
}

#[derive(Serialize, Deserialize)]
pub struct TopicLog<M>
where
    M: 'static,
{
    pub topic: String,
    pub message: M,
}
