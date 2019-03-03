use futures;
#[macro_use]
extern crate log;
#[macro_use]
extern crate prost_derive;
use tokio;
use tower_grpc;
use tower_h2;

use actix::prelude::*;

pub mod kv {
    include!(concat!(env!("OUT_DIR"), "/kv.rs"));
}

mod db_actor;

use crate::kv::{
    server, DeleteRequest, DeleteResponse, GetRequest, GetResponse, ScanRequest, ScanResponse,
    SetRequest, SetResponse,
};

use futures::{Future, Stream};
use std::error::Error;
use tokio::executor::DefaultExecutor;
use tokio::net::TcpListener;
use tower_grpc::{Request, Response};
use tower_h2::Server;

#[derive(Clone)]
struct KV {
    pub addr: actix::Addr<db_actor::SledExecutor>,
}

impl server::Kv for KV {
    // The tower-grpc generated code needs a GetFuture type defined.
    type GetFuture = Box<
        dyn Future<Item = tower_grpc::Response<GetResponse>, Error = tower_grpc::Status> + Send,
    >;
    fn get(&mut self, request: Request<GetRequest>) -> Self::GetFuture {
        Box::new(
            self.addr
                .send(request.into_inner())
                // ignore actix::address::MailboxError
                .map_err(|_| tower_grpc::Status::with_code(tower_grpc::Code::Internal))
                .and_then(|res| res.map(Response::new)),
        )
    }

    type SetFuture = Box<
        dyn Future<Item = tower_grpc::Response<SetResponse>, Error = tower_grpc::Status> + Send,
    >;
    fn set(&mut self, request: Request<SetRequest>) -> Self::SetFuture {
        Box::new(
            self.addr
                .send(request.into_inner())
                .map_err(|_| tower_grpc::Status::with_code(tower_grpc::Code::Internal))
                .and_then(|res| res.map(Response::new)),
        )
    }

    type DeleteFuture = Box<
        dyn Future<Item = tower_grpc::Response<DeleteResponse>, Error = tower_grpc::Status> + Send,
    >;
    fn delete(&mut self, request: Request<DeleteRequest>) -> Self::DeleteFuture {
        Box::new(
            self.addr
                .send(request.into_inner())
                .map_err(|_| tower_grpc::Status::with_code(tower_grpc::Code::Internal))
                .and_then(|res| res.map(Response::new)),
        )
    }

    type ScanFuture = Box<
        dyn Future<Item = tower_grpc::Response<ScanResponse>, Error = tower_grpc::Status> + Send,
    >;
    fn scan(&mut self, request: Request<ScanRequest>) -> Self::ScanFuture {
        Box::new(
            self.addr
                .send(request.into_inner())
                .map_err(|_| tower_grpc::Status::with_code(tower_grpc::Code::Internal))
                .and_then(|res| res.map(Response::new)),
        )
    }
}

pub fn main() -> Result<(), Box<Error>> {
    ::env_logger::init();
    let tree = sled::Db::start_default("test_db")?.open_tree(b"test".to_owned().to_vec())?;

    actix::System::run(move || {
        let addr = SyncArbiter::start(3, move || db_actor::SledExecutor::new(tree.clone()));

        let new_service = server::KvServer::new(KV { addr });

        let h2_settings = Default::default();
        let mut h2 = Server::new(new_service, h2_settings, DefaultExecutor::current());

        let addr = "127.0.0.1:50051".parse().unwrap();
        let bind = TcpListener::bind(&addr).expect("bind");

        let serve = bind
            .incoming()
            .for_each(move |sock| {
                if let Err(e) = sock.set_nodelay(true) {
                    return Err(e);
                }

                let serve = h2.serve(sock);
                tokio::spawn(serve.map_err(|e| error!("h2 error: {:?}", e)));

                Ok(())
            })
            .map_err(|e| eprintln!("accept error: {}", e));

        // start the gRPC server
        tokio::spawn(serve);
    });
    Ok(())
}
