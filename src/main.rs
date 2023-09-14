use std::{collections::HashMap, io::Read, path::PathBuf, str::FromStr};

use futures::StreamExt;
use libp2p::{
    core::upgrade,
    identity::Keypair,
    mdns, noise,
    request_response::{self, ResponseChannel},
    swarm::NetworkBehaviour,
    swarm::{SwarmBuilder, SwarmEvent},
    tcp, yamux, PeerId, Swarm, Transport,
};

use mdns::Event::Discovered;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task,
};
use uuid::Uuid;

type StreamsMap = HashMap<String, UnboundedSender<ReqRes>>;

#[tokio::main]
async fn main() {
    let (mut swarm, _key_pair, _my_peer) = build_swarp();

    let (success_s, mut success_r) = unbounded_channel::<(ReqRes, ResponseChannel<ReqRes>)>();
    let (s, mut r) = unbounded_channel::<(ReqRes, PeerId)>();

    let mut streams: StreamsMap = HashMap::new();

    let mut line = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    loop {
        tokio::select! {
            line = line.next_line() => {
                if let Ok(Some(line)) = line {
                    let id = Uuid::new_v4().to_string();
                    let (res_s, res_r) = unbounded_channel::<ReqRes>();
                    streams.insert(id.clone(), res_s);
                    let peer = PeerId::from_str(&line).unwrap();
                    let req_s = s.clone();
                    task::spawn(async move {
                        _ = send_file(id, peer, "demo/22mb.jpg".to_string(), res_r, req_s).await;
                    });
                }
            },
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Reqres(reqres)) => {
                    match reqres {
                        ReqResEvent::Message { peer: _, message } => match message {
                            request_response::Message::Request {
                                request: req, channel: c, ..
                            } => {
                                println!(
                                    "Got request with len: {:?}",
                                    req.clone().data.unwrap_or([].to_vec()).len()
                                );
                                task::spawn(handle_req(req, c, success_s.clone()));
                            }
                            request_response::Message::Response { response, .. } => {
                                println!("Got response: {:?}", response);
                                if let Some(res_s) = streams.get(&response.id) {
                                    _ = res_s.send(response);
                                }
                            }
                        },
                        _ => {}
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening on {:?}", address);
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(Discovered(peers_addr))) => {
                    for (peer, _addr) in peers_addr {
                        println!("Found peer {:?}", peer);
                        if !swarm.is_connected(&peer) {
                            swarm.dial(peer).unwrap();
                        }
                    }
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
            rr = r.recv() => {
                if let Some((req, peer)) = rr {
                    println!("Sending request to peer: {}", peer);
                    _ = swarm.behaviour_mut().reqres.send_request(&peer, req);
                }
            },
            sr = success_r.recv() => {
                if let Some((success, chanel)) = sr {
                    _ = swarm.behaviour_mut().reqres.send_response(chanel, success);
                }
            }
        }
    }
}

async fn handle_req(
    req: ReqRes,
    c: ResponseChannel<ReqRes>,
    success_s: UnboundedSender<(ReqRes, ResponseChannel<ReqRes>)>,
) {
    if let Some(file_name) = req.file_name {
        let mut path = PathBuf::from(req.path.unwrap());
        let new_file_name = format!("{}-{}", req.id, file_name);
        path.set_file_name(new_file_name);
        if !path.exists() {
            _ = tokio::fs::File::create(&path).await.unwrap();
        } else if req.sent.unwrap() == 0 {
            _ = tokio::fs::remove_file(&path).await.unwrap();
            _ = tokio::fs::File::create(&path).await.unwrap();
        }
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&path)
            .await
            .unwrap();
        _ = file.write_all(&req.data.unwrap()).await;
    }
    let id = req.id.clone();
    let success = ReqRes::success_data(id.clone());
    success_s.send((success, c)).unwrap();
}

const BUF: usize = 100 * 1024 * 1024;
async fn send_file(
    id: String,
    peer: PeerId,
    file_name: String,
    mut res_r: UnboundedReceiver<ReqRes>,
    req_s: UnboundedSender<(ReqRes, PeerId)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = std::fs::File::open(&file_name)?;
    let file_len = file.metadata()?.len() as usize;
    println!(
        "Sending file: {} with len {} to peer {}",
        &file_name, file_len, &peer
    );
    let mut buf = vec![0; BUF];
    let mut sent: usize = 0;
    'outer: loop {
        let read = file.read(&mut buf)?;
        println!("Read {} bytes", read);
        sent += read;
        if sent > file_len {
            eprintln!("Sent more than file len. Something went wrong!");
            break 'outer;
        }
        let last = sent == file_len;
        let req = ReqRes::file_block(
            id.clone(),
            buf[..read].to_vec(),
            sent,
            last,
            Some(file_name.clone()),
        );
        req_s.send((req.clone(), peer.clone()))?;
        println!("Total sent {} blocks", sent);
        loop {
            if let Some(res) = res_r.recv().await {
                if res.id == id {
                    let mut tried: i8 = 1;
                    if res.success.unwrap_or(false) {
                        if last {
                            println!("File {} sent to peer: {}", &file_name, &peer);
                            break 'outer;
                        }
                        break;
                    } else {
                        if tried == 3 {
                            println!("Failed to send file: {}", &file_name);
                            break 'outer;
                        }
                        tried += 1;
                        req_s.send((req.clone(), peer.clone()))?;
                        println!("Resending({}) block from: {}", tried, &file_name);
                    }
                }
            }
        }
    }
    Ok(())
}

fn build_swarp() -> (Swarm<MyBehaviour>, Keypair, PeerId) {
    let id_keys = Keypair::generate_ed25519();
    let peer = PeerId::from(id_keys.public());
    println!("Local peer id: {peer}");
    let mdns = mdns::async_io::Behaviour::new(mdns::Config::default(), peer).unwrap();
    let tcp_config = tcp::Config::default().nodelay(true);
    let mut yc = yamux::Config::default();
    yc.set_max_buffer_size(100 * 1024 * 1024);
    yc.set_receive_window_size(100 * 1024 * 1024);
    yc.set_window_update_mode(yamux::WindowUpdateMode::on_receive());
    let transport = tcp::tokio::Transport::new(tcp_config)
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&id_keys).expect("signing libp2p-noise static keypair"))
        .multiplex(yc);
    let mut reqres_config = libp2p::request_response::Config::default();
    reqres_config.set_request_timeout(std::time::Duration::from_secs(60));
    reqres_config.set_connection_keep_alive(std::time::Duration::from_secs(60));
    let behaviour = MyBehaviour {
        mdns,
        keep_alive: libp2p::swarm::keep_alive::Behaviour::default(),
        reqres: codec::Behaviour::<ReqRes, ReqRes>::with_codec(
            codec::Codec::default(),
            [(
                libp2p::StreamProtocol::new("/my-cbor-protocol"),
                request_response::ProtocolSupport::Full,
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

type ReqResEvent = request_response::Event<ReqRes, ReqRes>;
// type ReqResChannel = UnboundedSender<(ReqRes, ResponseChannel<ReqRes>)>;

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    keep_alive: libp2p::swarm::keep_alive::Behaviour,
    reqres: codec::Behaviour<ReqRes, ReqRes>,
    mdns: mdns::async_io::Behaviour,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct ReqRes {
    id: String,
    success: Option<bool>,
    message: Option<String>,
    file_name: Option<String>,
    path: Option<String>,
    data: Option<Vec<u8>>,
    sent: Option<usize>,
    last: Option<bool>,
}

impl ReqRes {
    pub fn _message_data(message: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            success: None,
            message: Some(message),
            file_name: None,
            path: None,
            data: None,
            sent: None,
            last: None,
        }
    }

    pub fn _file_data(data: Vec<u8>, path: String) -> Self {
        let file = std::path::Path::new(&path);
        let file_name = file.file_name().unwrap().to_str().unwrap().to_string();
        Self {
            id: Uuid::new_v4().to_string(),
            last: Some(true),
            path: Some(path),
            file_name: Some(file_name),
            data: Some(data),
            message: None,
            success: None,
            sent: None,
        }
    }

    pub fn file_block(
        id: String,
        data: Vec<u8>,
        sent: usize,
        last: bool,
        path: Option<String>,
    ) -> Self {
        Self {
            id,
            last: Some(last),
            data: Some(data),
            sent: Some(sent),
            message: None,
            success: None,
            path: path.clone(),
            file_name: {
                if let Some(path) = path {
                    let file = std::path::Path::new(&path);
                    Some(file.file_name().unwrap().to_str().unwrap().to_string())
                } else {
                    None
                }
            },
        }
    }

    pub fn success_data(id: String) -> Self {
        Self {
            id,
            success: Some(true),
            message: None,
            file_name: None,
            path: None,
            data: None,
            sent: None,
            last: None,
        }
    }
}

mod codec {
    use async_trait::async_trait;
    use cbor4ii::core::error::DecodeError;
    use futures::prelude::*;
    use futures::{AsyncRead, AsyncWrite};
    use libp2p::swarm::StreamProtocol;
    use serde::{de::DeserializeOwned, Serialize};
    use std::{collections::TryReserveError, convert::Infallible, io, marker::PhantomData};

    /// Max request size in bytes
    const REQUEST_SIZE_MAXIMUM: u64 = 100 * 1024 * 1024;
    /// Max response size in bytes
    const RESPONSE_SIZE_MAXIMUM: u64 = 100 * 1024 * 1024;

    pub type Behaviour<Req, Resp> = libp2p::request_response::Behaviour<Codec<Req, Resp>>;

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
    impl<Req, Resp> libp2p::request_response::Codec for Codec<Req, Resp>
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
