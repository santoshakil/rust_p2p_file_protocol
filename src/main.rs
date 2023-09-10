use std::io::Read;

use futures::StreamExt;
use libp2p::{
    core::upgrade,
    identity::Keypair,
    mdns, noise,
    swarm::NetworkBehaviour,
    swarm::{SwarmBuilder, SwarmEvent},
    tcp, yamux, PeerId, Swarm, Transport,
};

use libp2p_request_response::ResponseChannel;
use mdns::Event::Discovered;
use tokio::{sync::mpsc::UnboundedSender, task};

type ReqResEvent = libp2p_request_response::Event<ReqRes, ReqRes>;
type ReqResChannel = UnboundedSender<(ReqRes, ResponseChannel<ReqRes>)>;

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    reqres: codec::Behaviour<ReqRes, ReqRes>,
    mdns: mdns::async_io::Behaviour,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct ReqRes {
    data: Vec<u8>,
}

#[tokio::main]
async fn main() {
    let (mut swarm, _key_pair, _my_peer) = build_swarp();
    let (s, mut r) = tokio::sync::mpsc::unbounded_channel::<(ReqRes, ResponseChannel<ReqRes>)>();
    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening on {:?}", address);
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(Discovered(peers_datum))) => {
                    for peer_data in peers_datum {
                        let peer = peer_data.0.clone();
                        let _addr = peer_data.1.clone();
                        println!("Found peer {:?}", peer);
                        if !swarm.is_connected(&peer) {
                            swarm.dial(peer).unwrap();
                            _ = swarm.behaviour_mut().reqres.send_request(
                                &peer,
                                ReqRes {
                                    data: "hello".as_bytes().to_vec(),
                                },
                            );
                        }
                    }
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Reqres(reqres)) => {
                    task::spawn(handle_reqres(reqres, s.clone()));
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    println!("Connection established with {}", peer_id);
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    let is_closed = !swarm.is_connected(&peer_id);
                    println!("Connection with {} closed: {}", peer_id, is_closed);
                }
                SwarmEvent::IncomingConnection { .. } => {}
                _ => {
                    println!("Got an event: {:?}", event);
                },
            },
            r = r.recv() => {
                if let Some((res, chanel)) = r {
                    _ = swarm.behaviour_mut().reqres.send_response(chanel, res.clone());
                }
            }
        }
    }
}

async fn handle_reqres(reqres: ReqResEvent, s: ReqResChannel) {
    match reqres {
        libp2p_request_response::Event::Message { peer: _, message } => match message {
            libp2p_request_response::Message::Request {
                request, channel, ..
            } => {
                println!(
                    "\nReqRes(Message::Request): request: {:?},\nchannel: {:?}\n",
                    request, channel
                );
                let mut file_buf = Vec::new();
                let mut file = std::fs::File::open("demo/22mb.jpg").unwrap();
                let _ = file.read_to_end(&mut file_buf).unwrap();
                let res = ReqRes { data: file_buf };
                _ = s.send((res, channel));
            }
            libp2p_request_response::Message::Response { response, .. } => {
                println!("\nReqRes(Message::Response): response: {:?}\n", response);
            }
        },
        _ => {
            println!("\nReqRes event: {:?}\n", reqres);
        }
    }
}

fn build_swarp() -> (Swarm<MyBehaviour>, Keypair, PeerId) {
    let id_keys = Keypair::generate_ed25519();
    let peer = PeerId::from(id_keys.public());
    println!("Local peer id: {peer}");
    let mdns = mdns::async_io::Behaviour::new(mdns::Config::default(), peer).unwrap();
    let tcp_config = tcp::Config::default().nodelay(true);
    let yc = yamux::Config::default();
    // yc.set_max_buffer_size(10 * 1024 * 1024);
    // yc.set_receive_window_size(1024 * 1024);
    // yc.set_window_update_mode(yamux::WindowUpdateMode::on_receive());
    let transport = tcp::tokio::Transport::new(tcp_config)
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&id_keys).expect("signing libp2p-noise static keypair"))
        .multiplex(yc);
    let mut reqres_config = libp2p::request_response::Config::default();
    reqres_config.set_request_timeout(std::time::Duration::from_secs(60));
    reqres_config.set_connection_keep_alive(std::time::Duration::from_secs(60));
    let behaviour = MyBehaviour {
        mdns,
        reqres: codec::Behaviour::<ReqRes, ReqRes>::with_codec(
            codec::Codec::default(),
            [(
                libp2p::StreamProtocol::new("/my-cbor-protocol"),
                libp2p_request_response::ProtocolSupport::Full,
            )],
            reqres_config,
        ),
    };
    let mut swarm = SwarmBuilder::with_tokio_executor(transport.boxed(), behaviour, peer).build();
    swarm
        .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
        .unwrap();
    (swarm, id_keys, peer)
}

mod codec {
    use async_trait::async_trait;
    use cbor4ii::core::error::DecodeError;
    use futures::prelude::*;
    use futures::{AsyncRead, AsyncWrite};
    use libp2p_swarm::StreamProtocol;
    use serde::{de::DeserializeOwned, Serialize};
    use std::{collections::TryReserveError, convert::Infallible, io, marker::PhantomData};

    /// Max request size in bytes
    const REQUEST_SIZE_MAXIMUM: u64 = 10 * 1024 * 1024;
    /// Max response size in bytes
    const RESPONSE_SIZE_MAXIMUM: u64 = 100 * 1024 * 1024;

    pub type Behaviour<Req, Resp> = libp2p_request_response::Behaviour<Codec<Req, Resp>>;

    pub struct Codec<Req, Resp> {
        phantom: PhantomData<(Req, Resp)>,
    }

    impl<Req, Resp> Default for Codec<Req, Resp> {
        fn default() -> Self {
            Codec {
                phantom: PhantomData,
            }
        }
    }

    impl<Req, Resp> Clone for Codec<Req, Resp> {
        fn clone(&self) -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl<Req, Resp> libp2p_request_response::Codec for Codec<Req, Resp>
    where
        Req: Send + Serialize + DeserializeOwned,
        Resp: Send + Serialize + DeserializeOwned,
    {
        type Protocol = StreamProtocol;
        type Request = Req;
        type Response = Resp;

        async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Req>
        where
            T: AsyncRead + Unpin + Send,
        {
            let mut vec = Vec::new();

            io.take(REQUEST_SIZE_MAXIMUM).read_to_end(&mut vec).await?;

            cbor4ii::serde::from_slice(vec.as_slice()).map_err(decode_into_io_error)
        }

        async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Resp>
        where
            T: AsyncRead + Unpin + Send,
        {
            let mut vec = Vec::new();

            io.take(RESPONSE_SIZE_MAXIMUM).read_to_end(&mut vec).await?;

            cbor4ii::serde::from_slice(vec.as_slice()).map_err(decode_into_io_error)
        }

        async fn write_request<T>(
            &mut self,
            _: &Self::Protocol,
            io: &mut T,
            req: Self::Request,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            let data: Vec<u8> =
                cbor4ii::serde::to_vec(Vec::new(), &req).map_err(encode_into_io_error)?;

            io.write_all(data.as_ref()).await?;

            Ok(())
        }

        async fn write_response<T>(
            &mut self,
            _: &Self::Protocol,
            io: &mut T,
            resp: Self::Response,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            let data: Vec<u8> =
                cbor4ii::serde::to_vec(Vec::new(), &resp).map_err(encode_into_io_error)?;

            io.write_all(data.as_ref()).await?;

            Ok(())
        }
    }

    fn decode_into_io_error(err: cbor4ii::serde::DecodeError<Infallible>) -> io::Error {
        match err {
            cbor4ii::serde::DecodeError::Core(DecodeError::Read(e)) => {
                io::Error::new(io::ErrorKind::Other, e)
            }
            cbor4ii::serde::DecodeError::Core(e @ DecodeError::Unsupported { .. }) => {
                io::Error::new(io::ErrorKind::Unsupported, e)
            }
            cbor4ii::serde::DecodeError::Core(e @ DecodeError::Eof { .. }) => {
                io::Error::new(io::ErrorKind::UnexpectedEof, e)
            }
            cbor4ii::serde::DecodeError::Core(e) => io::Error::new(io::ErrorKind::InvalidData, e),
            cbor4ii::serde::DecodeError::Custom(e) => {
                io::Error::new(io::ErrorKind::Other, e.to_string())
            }
        }
    }

    fn encode_into_io_error(err: cbor4ii::serde::EncodeError<TryReserveError>) -> io::Error {
        io::Error::new(io::ErrorKind::Other, err)
    }
}
