use std::io::{Read, Seek, SeekFrom};

use futures::{future::Either, StreamExt};
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::OrTransport, upgrade},
    identity, mdns, noise,
    swarm::NetworkBehaviour,
    swarm::{SwarmBuilder, SwarmEvent},
    tcp, yamux, PeerId, Swarm, Transport,
};
use libp2p_quic as quic;
use tokio::{fs::File, io::AsyncWriteExt, task};

const RESPONSE_SIZE: u64 = 9 * 1024 * 1024;
type ReqResType = libp2p_request_response::Event<FileRequest, FileResponse>;

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    mdns: mdns::async_io::Behaviour,
    reqres: libp2p::request_response::cbor::Behaviour<FileRequest, FileResponse>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FileRequest {
    name: String,
    written: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FileResponse {
    path: String,
    hash: String,
    buf: Vec<u8>,
    written: u64,
    start: bool,
    end: bool,
}

#[tokio::main]
async fn main() {
    let mut swarm = build_swarp();

    let (s, mut r) = tokio::sync::mpsc::unbounded_channel::<(PeerId, FileResponse)>();
    let (s2, mut r2) = tokio::sync::mpsc::unbounded_channel::<(PeerId, FileRequest)>();
    let (cs, cr) = crossbeam_channel::unbounded::<FileResponse>();

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(mut list))) => {
                    list.sort_by(|a, b| a.0.cmp(&b.0));
                    list.dedup_by(|a, b| a.0 == b.0);
                    for (peer_id, _multiaddr) in list {
                        println!("\nmDNS discovered a new peer: {peer_id}");
                        let req = FileRequest { name: "demo/picture.jpg".to_string(), written: 0};
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
                                        _ = file.seek(SeekFrom::Start(request.written));
                                        let mut buffer = [0u8; RESPONSE_SIZE as usize];
                                        let bytes_read = file.read(&mut buffer);
                                        let is_end = bytes_read.unwrap() < buffer.len();
                                        let response = FileResponse {
                                            path: request.name.clone(),
                                            hash: "hash".to_string(),
                                            buf: buffer.to_vec(),
                                            written: request.written,
                                            start: false,
                                            end: is_end,
                                        };
                                        _ = swarm.behaviour_mut().reqres.send_response(channel, response);
                                    }
                                },
                                libp2p_request_response::Message::Response { request_id, response } => {
                                    println!("Received response from {peer} with id {request_id}: {response:?}");
                                    _ = s.send((peer, response));
                                },
                            }
                        },
                        _ => {}
                    }
                },
                _ => {},
            },
            r = r.recv() => {
                if let Some((peer, response)) = r {
                    if response.start {
                        task::spawn(handle_response(peer, response, cr.clone(), s2.clone()));
                    } else {
                        cs.send(response).unwrap();
                    }
                }
            }
            r2 = r2.recv() => {
                if let Some((peer, req)) = r2 {
                    swarm.behaviour_mut().reqres.send_request(&peer, req);
                }
            }
        }
    }
}

type CR = crossbeam_channel::Receiver<FileResponse>;
type TS = tokio::sync::mpsc::UnboundedSender<(PeerId, FileRequest)>;

async fn handle_response(peer: PeerId, response: FileResponse, cr: CR, s: TS) {
    let path = format!("tmp/{}", response.path);
    let mut file = File::create(path).await.unwrap();
    file.write_all(&response.buf).await.unwrap();
    loop {
        if let Ok(r) = cr.recv() {
            if r.path == response.path {
                file.write_all(&r.buf).await.unwrap();
                if r.end {
                    break;
                }
                let written = r.written + r.buf.len() as u64;
                let request = FileRequest {
                    name: response.path.clone(),
                    written,
                };
                s.send((peer, request)).unwrap();
            }
        } else {
            break;
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
