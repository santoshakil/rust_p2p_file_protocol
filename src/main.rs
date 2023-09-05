use std::io::Read;

use futures::{future::Either, StreamExt};
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::OrTransport, upgrade},
    identity, mdns, noise,
    swarm::NetworkBehaviour,
    swarm::{SwarmBuilder, SwarmEvent},
    tcp, yamux, PeerId, Swarm, Transport,
};
use libp2p_quic as quic;

type ReqResType = libp2p_request_response::Event<FileRequest, FileResponse>;

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    mdns: mdns::async_io::Behaviour,
    reqres: libp2p::request_response::cbor::Behaviour<FileRequest, FileResponse>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FileRequest {
    name: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FileResponse {
    hash: String,
    name: Option<String>,
    content: FileContent,
    block: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum FileContent {
    Start(Vec<u8>),
    Continue(Vec<u8>),
    End(Vec<u8>),
}

#[tokio::main]
async fn main() {
    let mut swarm = build_swarp();

    let (s, mut r) = tokio::sync::mpsc::unbounded_channel();

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(mut list))) => {
                    list.sort_by(|a, b| a.0.cmp(&b.0));
                    list.dedup_by(|a, b| a.0 == b.0);
                    for (peer_id, _multiaddr) in list {
                        println!("\nmDNS discovered a new peer: {peer_id}");
                        let req = FileRequest { name: "demo/picture.jpg".to_string() };
                        swarm.behaviour_mut().reqres.send_request(&peer_id, req);
                    }
                },
                SwarmEvent::Behaviour(MyBehaviourEvent::Reqres(reqres)) => {
                    match reqres {
                        ReqResType::Message {peer, message } => {
                            match message {
                                libp2p_request_response::Message::Request { request_id, request, channel } => {
                                    println!("Received request from {peer} with id {request_id}: {request:?}");
                                    if let Ok(mut file) = std::fs::File::open(request.name.clone()) {
                                        let name = request.name.clone();
                                        let mut buffer = [0; 1024];
                                        let mut block = 0;
                                        loop {
                                            let size = file.read(&mut buffer).unwrap();
                                            if size == 0 {
                                                break;
                                            }
                                            let content = if block == 0 {
                                                FileContent::Start(buffer[..size].to_vec())
                                            } else {
                                                FileContent::Continue(buffer[..size].to_vec())
                                            };
                                            let response = FileResponse { hash: "hash".to_string(), name: Some(name.clone()), content, block };
                                            swarm.behaviour_mut().reqres.send_response(channel, response);
                                            block += 1;
                                        }
                                    }
                                },
                                libp2p_request_response::Message::Response { request_id, response } => {
                                    println!("Received response from {peer} with id {request_id}: {response:?}");
                                    _ = s.send(response);
                                },
                            }
                        },
                        _ => {}
                    }
                },
                _ => {},
            },
            response = r.recv() => {
                if let Some(response) = response {
                    let FileResponse { hash, name, content, block } = response;
                    match content {
                        FileContent::Start(data) => {
                            println!("Received file {name} with hash {hash} and block {block} with size {size}", name = name.unwrap_or("".to_string()), hash = hash, block = block, size = data.len());
                        },
                        FileContent::Continue(data) => {
                            println!("Received file {name} with hash {hash} and block {block} with size {size}", name = name.unwrap_or("".to_string()), hash = hash, block = block, size = data.len());
                        },
                        FileContent::End(data) => {
                            println!("Received file {name} with hash {hash} and block {block} with size {size}", name = name.unwrap_or("".to_string()), hash = hash, block = block, size = data.len());
                        },
                    }
                }
            }
        }
    }
}

fn build_swarp() -> Swarm<MyBehaviour> {
    let id_keys = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(id_keys.public());
    println!("Local peer id: {local_peer_id}");

    let tcp_transport = tcp::async_io::Transport::new(tcp::Config::default().nodelay(true))
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise::Config::new(&id_keys).expect("signing libp2p-noise static keypair"))
        .multiplex(yamux::Config::default())
        .timeout(std::time::Duration::from_secs(10))
        .boxed();
    let quic_transport = quic::async_std::Transport::new(quic::Config::new(&id_keys));
    let transport = OrTransport::new(quic_transport, tcp_transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    let mut swarm = {
        let mdns = mdns::async_io::Behaviour::new(mdns::Config::default(), local_peer_id).unwrap();
        let behaviour = MyBehaviour {
            mdns,
            reqres: libp2p::request_response::cbor::Behaviour::<FileRequest, FileResponse>::new(
                [(
                    libp2p::StreamProtocol::new("/my-cbor-protocol"),
                    libp2p_request_response::ProtocolSupport::Full,
                )],
                libp2p::request_response::Config::default(),
            ),
        };
        SwarmBuilder::with_async_std_executor(transport, behaviour, local_peer_id).build()
    };

    swarm
        .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap())
        .unwrap();
    swarm
        .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
        .unwrap();

    swarm
}
