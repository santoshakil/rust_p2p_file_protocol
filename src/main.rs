use std::io::Read;

use futures::StreamExt;
use libp2p::{
    core::upgrade,
    identity::Keypair,
    kad::{
        self,
        store::{MemoryStore, MemoryStoreConfig},
        Kademlia, KademliaEvent, RecordKey,
    },
    mdns, noise,
    swarm::NetworkBehaviour,
    swarm::{SwarmBuilder, SwarmEvent},
    tcp, yamux, PeerId, Swarm, Transport,
};

use mdns::Event::Discovered;
use tokio::task;

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    reqres: libp2p::request_response::cbor::Behaviour<FileRequest, FileResponse>,
    mdns: mdns::async_io::Behaviour,
    kad: Kademlia<MemoryStore>,
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
    let (mut swarm, _key_pair, _my_peer) = build_swarp();
    let mut file = std::fs::File::open("demo/picture.jpg").unwrap();
    loop {
        let event = swarm.select_next_some().await;
        match event {
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
                        let key = RecordKey::new::<String>(&"files".to_string());
                        let mut file_buf = Vec::new();
                        let _ = file.read_to_end(&mut file_buf).unwrap();
                        _ = swarm.behaviour_mut().kad.put_record(
                            kad::Record::new::<RecordKey>(key, file_buf),
                            kad::Quorum::All,
                        );
                    }
                }
            }
            SwarmEvent::Behaviour(MyBehaviourEvent::Kad(kad_event)) => {
                task::spawn(handle_kad_event(kad_event));
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
            }
        }
    }
}

async fn handle_kad_event(event: KademliaEvent) {
    match event {
        KademliaEvent::OutboundQueryProgressed {
            id,
            result,
            stats,
            step,
        } => {
            println!(
                "\nOutboundQueryProgressed: id: {:?},\nresult: {:?},\nstats: {:?},\nstep: {:?}\n",
                id, result, stats, step
            );
        }
        _ => {
            println!("UnHandled kad event: {:?}", event);
        }
    }
}

fn build_swarp() -> (Swarm<MyBehaviour>, Keypair, PeerId) {
    let id_keys = Keypair::generate_ed25519();
    let peer = PeerId::from(id_keys.public());
    println!("Local peer id: {peer}");
    let mdns = mdns::async_io::Behaviour::new(mdns::Config::default(), peer).unwrap();
    let tcp_config = tcp::Config::default().nodelay(true);
    let mut yc = yamux::Config::default();
    yc.set_max_buffer_size(1024 * 1024 * 1024);
    yc.set_receive_window_size(1024 * 1024 * 1024);
    let transport = tcp::tokio::Transport::new(tcp_config)
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&id_keys).expect("signing libp2p-noise static keypair"))
        .multiplex(yc);
    let mut mc = MemoryStoreConfig::default();
    mc.max_value_bytes = 1024 * 1024 * 1024;
    let behaviour = MyBehaviour {
        mdns,
        kad: Kademlia::new(peer.clone(), MemoryStore::with_config(peer.clone(), mc)),
        reqres: libp2p::request_response::cbor::Behaviour::<FileRequest, FileResponse>::new(
            [(
                libp2p::StreamProtocol::new("/my-cbor-protocol"),
                libp2p_request_response::ProtocolSupport::Full,
            )],
            libp2p::request_response::Config::default(),
        ),
    };
    let mut swarm = SwarmBuilder::with_tokio_executor(transport.boxed(), behaviour, peer).build();
    swarm
        .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
        .unwrap();
    (swarm, id_keys, peer)
}
