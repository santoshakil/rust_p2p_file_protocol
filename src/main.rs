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
    reqres: libp2p::request_response::cbor::Behaviour<ReqRes, ReqRes>,
    mdns: mdns::async_io::Behaviour,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
                if let Some((event, chanel)) = r {
                    _ = swarm.behaviour_mut().reqres.send_response(chanel, event);
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
                let mut file = std::fs::File::open("demo/4_7mb.jpg").unwrap();
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
        reqres: libp2p::request_response::cbor::Behaviour::<ReqRes, ReqRes>::new(
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
