use futures;
use http;
#[macro_use]
extern crate prost_derive;
use tokio;
use tower_grpc;
use tower_h2;
use tower_http;
use tower_service;
use tower_util;

use futures::{Future, Poll};
use tokio::executor::DefaultExecutor;
use tokio::net::tcp::{ConnectFuture, TcpStream};
use tower_grpc::Request;
use tower_h2::client;
use tower_service::Service;
use tower_util::MakeService;

pub mod kv {
    include!(concat!(env!("OUT_DIR"), "/kv.rs"));
}

pub fn main() {
    let _ = ::env_logger::init();

    let set = |key: Vec<u8>, value: Vec<u8>| {
        let mut make_client =
            client::Connect::new(Dst, Default::default(), DefaultExecutor::current());

        let service = make_client.make_service(()).map(move |conn| {
            let uri: http::Uri = format!("http://localhost:50051").parse().unwrap();
            let conn = tower_http::add_origin::Builder::new()
                .uri(uri)
                .build(conn)
                .unwrap();
            kv::client::Kv::new(conn)
        });
        service
            .and_then(|mut client| {
                client
                    .set(Request::new(kv::SetRequest { key, value }))
                    .map_err(|e| panic!("gRPC request failed; err={:?}", e))
            })
            .and_then(|response| {
                println!("RESPONSE = {:?}", response);
                Ok(())
            })
            .map_err(|e| {
                println!("ERR = {:?}", e);
            })
    };

    let get = |key: Vec<u8>| {
        let mut make_client =
            client::Connect::new(Dst, Default::default(), DefaultExecutor::current());

        let service = make_client.make_service(()).map(move |conn| {
            let uri: http::Uri = format!("http://localhost:50051").parse().unwrap();
            let conn = tower_http::add_origin::Builder::new()
                .uri(uri)
                .build(conn)
                .unwrap();
            kv::client::Kv::new(conn)
        });
        service
            .and_then(|mut client| {
                client
                    .get(Request::new(kv::GetRequest { key }))
                    .map_err(|e| panic!("gRPC request failed; err={:?}", e))
            })
            .and_then(|response| {
                println!("RESPONSE = {:?}", response);
                Ok(())
            })
            .map_err(|e| {
                println!("ERR = {:?}", e);
            })
    };

    let del = |key: Vec<u8>| {
        let mut make_client =
            client::Connect::new(Dst, Default::default(), DefaultExecutor::current());

        let service = make_client.make_service(()).map(move |conn| {
            let uri: http::Uri = format!("http://localhost:50051").parse().unwrap();
            let conn = tower_http::add_origin::Builder::new()
                .uri(uri)
                .build(conn)
                .unwrap();
            kv::client::Kv::new(conn)
        });
        service
            .and_then(|mut client| {
                client
                    .delete(Request::new(kv::DeleteRequest { key }))
                    .map_err(|e| panic!("gRPC request failed; err={:?}", e))
            })
            .and_then(|response| {
                println!("RESPONSE = {:?}", response);
                Ok(())
            })
            .map_err(|e| {
                println!("ERR = {:?}", e);
            })
    };

    let scan = |start_key: Vec<u8>, end_key: Vec<u8>| {
        let mut make_client =
            client::Connect::new(Dst, Default::default(), DefaultExecutor::current());

        let service = make_client.make_service(()).map(move |conn| {
            let uri: http::Uri = format!("http://localhost:50051").parse().unwrap();
            let conn = tower_http::add_origin::Builder::new()
                .uri(uri)
                .build(conn)
                .unwrap();
            kv::client::Kv::new(conn)
        });
        service
            .and_then(|mut client| {
                client
                    .scan(Request::new(kv::ScanRequest { start_key, end_key }))
                    .map_err(|e| panic!("gRPC request failed; err={:?}", e))
            })
            .and_then(|response| {
                println!("RESPONSE = {:?}", response);
                Ok(())
            })
            .map_err(|e| {
                println!("ERR = {:?}", e);
            })
    };

    tokio::run(
        set(b"asdf".to_vec(), b"junk".to_vec())
            .and_then(move |()| set(b"foo".to_vec(), b"12345".to_vec()))
            .and_then(move |()| set(b"bar".to_vec(), b"67890".to_vec()))
            .and_then(move |()| del(b"asdf".to_vec()))
            .and_then(move |()| get(b"foo".to_vec()))
            .and_then(move |()| scan(b"aaa".to_vec(), b"zzz".to_vec())),
    );
}

struct Dst;

impl Service<()> for Dst {
    type Response = TcpStream;
    type Error = ::std::io::Error;
    type Future = ConnectFuture;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(().into())
    }

    fn call(&mut self, _: ()) -> Self::Future {
        TcpStream::connect(&([127, 0, 0, 1], 50051).into())
    }
}
