use serde::Serialize;

#[macro_export]
macro_rules! log {
    ($topic:expr, $msg:expr) => {
        tracing::info!(
            "{}",
            serde_json::to_string(&$crate::node::blend::log::TopicLog {
                topic: $topic.to_string(),
                message: $msg
            })
            .unwrap()
        );
    };
}

#[derive(Serialize)]
pub struct TopicLog<M: Serialize> {
    pub topic: String,
    pub message: M,
}
