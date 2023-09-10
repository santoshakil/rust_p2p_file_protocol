use futures::prelude::*;
use libp2p::core::upgrade::Version;
use libp2p::{
    identity, mdns, noise,
    swarm::{keep_alive, NetworkBehaviour, SwarmBuilder, SwarmEvent},
    tcp, yamux, PeerId, Transport,
};
use libp2p_stream_protocol as libp2p_stream;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("Local peer id: {local_peer_id:?}");

    let transport = tcp::async_io::Transport::default()
        .upgrade(Version::V1Lazy)
        .authenticate(noise::Config::new(&local_key)?)
        .multiplex(yamux::Config::default())
        .boxed();

    let mut mdns_config = mdns::Config::default();
    mdns_config.query_interval = std::time::Duration::from_secs(10);

    let behaviour = Behaviour {
        mdns: mdns::tokio::Behaviour::new(mdns_config, local_peer_id).unwrap(),
        keep_alive: keep_alive::Behaviour::default(),
        stream: libp2p_stream::Behaviour::default(),
    };

    let mut swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    loop {
        let event = swarm.select_next_some().await;
        match event {
            SwarmEvent::NewListenAddr { address, .. } => println!("Listening on {address:?}"),
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(data))) => {
                for (peer, ..) in data {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    if !swarm.is_connected(&peer) {
                        println!("Connecting {peer:?}");
                        _ = swarm.dial(peer);
                    }
                }
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                println!("Connection established: {peer_id:?}");
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                println!("Connection closed: {peer_id:?}");
            }
            _ => println!("{event:?}"),
        }
    }
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    keep_alive: keep_alive::Behaviour,
    stream: libp2p_stream::Behaviour,
    mdns: mdns::tokio::Behaviour,
}
