use actix::prelude::*;

use crate::kv::*;

/// This is the sled executor actor. We are going to run 3 of them in parallel.
/// sled::Tree appears to be using mutexes internally, so going to assume for this
/// example that it is thread-safe.
pub struct SledExecutor {
    pub db: std::sync::Arc<sled::Tree>,
}

impl SledExecutor {
    pub fn new(db: std::sync::Arc<sled::Tree>) -> SledExecutor {
        SledExecutor { db }
    }
}

impl Actor for SledExecutor {
    type Context = SyncContext<Self>;
}

type GetResult = std::result::Result<GetResponse, tower_grpc::Status>;
impl Message for GetRequest {
    type Result = GetResult;
}

impl Handler<GetRequest> for SledExecutor {
    type Result = GetResult;

    fn handle(&mut self, msg: GetRequest, _: &mut Self::Context) -> Self::Result {
        // get returns a Result<Option<PinnedValue>, ()>
        // So here we're going to map the error Result to a tower_rpc::Status, then put
        // an error into the gRPC message if the Option is None.
        let v = self
            .db
            .get(&msg.key)
            .map_err(|_| tower_grpc::Status::with_code(tower_grpc::Code::Internal))?;
        match v {
            Some(value) => Ok(GetResponse {
                error: None,
                value: value.to_vec(),
            }),
            None => Ok(GetResponse {
                error: Some(NotFound {
                    key: msg.key.clone(),
                }),
                value: vec![],
            }),
        }
    }
}

type SetResult = std::result::Result<SetResponse, tower_grpc::Status>;
impl Message for SetRequest {
    type Result = SetResult;
}

impl Handler<SetRequest> for SledExecutor {
    type Result = SetResult;

    fn handle(&mut self, msg: SetRequest, _: &mut Self::Context) -> Self::Result {
        self.db
            .set(msg.key, msg.value)
            .map_err(|_| tower_grpc::Status::with_code(tower_grpc::Code::Internal))?;
        Ok(SetResponse {})
    }
}

type DeleteResult = std::result::Result<DeleteResponse, tower_grpc::Status>;
impl Message for DeleteRequest {
    type Result = DeleteResult;
}

impl Handler<DeleteRequest> for SledExecutor {
    type Result = DeleteResult;

    fn handle(&mut self, msg: DeleteRequest, _: &mut Self::Context) -> Self::Result {
        self.db
            .del(&msg.key)
            .map_err(|_| tower_grpc::Status::with_code(tower_grpc::Code::Internal))?;
        Ok(DeleteResponse {})
    }
}

type ScanResult = std::result::Result<ScanResponse, tower_grpc::Status>;
impl Message for ScanRequest {
    type Result = ScanResult;
}

impl Handler<ScanRequest> for SledExecutor {
    type Result = ScanResult;

    fn handle(&mut self, msg: ScanRequest, _: &mut Self::Context) -> Self::Result {
        let r = self.db.range(msg.start_key..msg.end_key);
        // range returns an Iterator of Result. So here I'm mapping the Result to an Option, then
        // filtering out None to build a Vec<Pair>.
        let pairs: Vec<Pair> = r
            .filter_map(|maybe| {
                maybe.ok().and_then(|(k, v)| {
                    Some(Pair {
                        key: k,
                        value: v.to_vec(),
                    })
                })
            })
            .collect();
        Ok(ScanResponse { pairs })
    }
}
