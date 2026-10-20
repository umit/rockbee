#![allow(clippy::unwrap_used)]
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use rockbee_raft_protocol::{ClusterId, GroupId, NodeId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, watch},
    task::JoinSet,
    time::{Instant, sleep, timeout},
};

use super::PeerRelays;
use crate::rpc::{
    PeerIdentity, PeerLimits, PeerResources, PeerServer,
    message::{
        PEER_HEADER_BYTES, PeerHeader, PeerKind, PeerRequest, PeerRequestBody, encode_request,
    },
};

#[tokio::test]
async fn group_cut_drains_only_its_native_owners_and_heal_restores_admission() {
    let cluster = ClusterId::new([109; 16]).unwrap();
    let source = NodeId::new(1).unwrap();
    let target = NodeId::new(2).unwrap();
    let blocked = GroupId::new(1);
    let sibling = GroupId::new(2);
    let metadata = GroupId::new(0);
    let mut relays = PeerRelays::start(cluster, vec![], PeerLimits::default());
    let tracker = relays.dispatch_tracker();
    let header = |group| PeerHeader::request(cluster, source, target, group, PeerKind::Vote);
    let original = tracker.try_admit(&header(blocked)).unwrap();
    let installed = relays.install_group_cut(blocked);
    assert!(tracker.try_admit(&header(blocked)).is_err());
    // The same peer edge can carry another group on its pooled connection.
    let sibling_owner = tracker.try_admit(&header(sibling)).unwrap();
    let metadata_owner = tracker.try_admit(&header(metadata)).unwrap();
    let first = {
        let mut drain = std::pin::pin!(relays.drain_group_cut(&installed));
        std::future::poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx))).await
    };
    assert!(
        first.is_pending(),
        "a seal cannot release an invoked native owner"
    );
    let counts = tracker.group_edge_counts(blocked, source, target);
    assert_eq!(counts.active, 1);
    assert_eq!(counts.admitted, 1);
    assert_eq!(counts.rejected, 1);
    assert_eq!(tracker.active_for(target), 3);
    drop(original);
    let ack = timeout(Duration::from_secs(2), relays.drain_group_cut(&installed))
        .await
        .unwrap();
    assert_eq!(ack.generation, installed.generation);
    assert_eq!(ack.group_id, blocked);
    assert_eq!(ack.earlier_dispatches, 0);
    assert_eq!(tracker.group_counts(blocked).active, 0);
    assert_eq!(tracker.group_counts(sibling).active, 1);
    assert_eq!(tracker.group_counts(metadata).active, 1);
    assert!(tracker.try_admit(&header(blocked)).is_err());
    relays.heal();
    let healed = tracker.try_admit(&header(blocked)).unwrap();
    let counts = tracker.group_counts(blocked);
    assert_eq!(counts.active, 1);
    assert_eq!(counts.admitted, 2);
    assert_eq!(counts.rejected, 2);
    drop(healed);
    drop(sibling_owner);
    drop(metadata_owner);
    assert_eq!(tracker.active_for(target), 0);
    assert_eq!(tracker.admitted_for(target), 4);
    relays.close().await;
}

#[test]
fn group_dispatch_counts_distinguish_group_and_directed_edge() {
    let tracker = super::DispatchTracker::new(Arc::new(super::relay::Shared::default()));
    let cluster = ClusterId::new([110; 16]).unwrap();
    let a = NodeId::new(1).unwrap();
    let b = NodeId::new(2).unwrap();
    let first = GroupId::new(1);
    let second = GroupId::new(2);
    let admit = |group, source, target| {
        tracker
            .try_admit(&PeerHeader::request(
                cluster,
                source,
                target,
                group,
                PeerKind::Vote,
            ))
            .unwrap()
    };
    let forward = admit(first, a, b);
    let reverse = admit(first, b, a);
    let sibling = admit(second, a, b);
    assert_eq!(tracker.group_counts(first).active, 2);
    assert_eq!(tracker.group_counts(second).active, 1);
    assert_eq!(tracker.group_edge_counts(first, a, b).admitted, 1);
    assert_eq!(tracker.group_edge_counts(first, b, a).admitted, 1);
    assert_eq!(tracker.group_edge_counts(second, b, a).admitted, 0);
    drop(forward);
    assert_eq!(tracker.group_edge_counts(first, a, b).active, 0);
    assert_eq!(tracker.group_edge_counts(first, b, a).active, 1);
    assert_eq!(tracker.group_edge_counts(second, a, b).active, 1);
    drop(reverse);
    drop(sibling);
    assert_eq!(tracker.group_counts(first).active, 0);
    assert_eq!(tracker.group_counts(first).admitted, 2);
    assert_eq!(tracker.group_counts(second).active, 0);
    assert_eq!(tracker.admitted_for(a), 3);
}

#[tokio::test]
async fn atomic_edge_cut_preserves_internal_streams_and_drains_cross_dispatch() {
    let cluster = ClusterId::new([108; 16]).unwrap();
    let ids: Vec<_> = (1..=4).map(|id| NodeId::new(id).unwrap()).collect();
    let edges: BTreeSet<_> = [ids[1], ids[2]]
        .into_iter()
        .flat_map(|left| {
            [ids[0], ids[3]]
                .into_iter()
                .flat_map(move |right| [(left, right), (right, left)])
        })
        .collect();
    let (stop, stopped) = watch::channel(false);
    let mut servers = Vec::new();
    let mut bindings = Vec::new();
    let mut addresses = BTreeMap::new();
    for target in &ids {
        let private = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let advertised = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.insert(*target, advertised.local_addr().unwrap());
        bindings.push((*target, advertised, private.local_addr().unwrap()));
        let mut stopped = stopped.clone();
        servers.push(tokio::spawn(async move {
            let mut streams = JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopped.changed() => break,
                    accepted = private.accept() => {
                        let (mut stream, _) = accepted.unwrap();
                        streams.spawn(async move {
                            let mut bytes = [0; 256];
                            while let Ok(length) = stream.read(&mut bytes).await {
                                if length == 0 || stream.write_all(&bytes[..length]).await.is_err() { break; }
                            }
                        });
                    }
                    Some(_) = streams.join_next(), if !streams.is_empty() => {}
                }
            }
            streams.abort_all();
            while streams.join_next().await.is_some() {}
        }));
    }
    let mut relays = PeerRelays::start(cluster, bindings, PeerLimits::default());
    // Existing bidirectional traffic includes every edge, including BC and AD.
    let mut streams = BTreeMap::new();
    for source in &ids {
        for target in &ids {
            if source == target {
                continue;
            }
            let mut stream = TcpStream::connect(addresses[target]).await.unwrap();
            let bytes = edge_prefix(cluster, *source, *target);
            stream.write_all(&bytes).await.unwrap();
            let mut echoed = vec![0; bytes.len()];
            timeout(Duration::from_secs(2), stream.read_exact(&mut echoed))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(echoed, bytes);
            streams.insert((*source, *target), stream);
        }
    }
    let mut partial = TcpStream::connect(addresses[&ids[0]]).await.unwrap();
    partial.write_all(&[0, 0, 0, 64, 1]).await.unwrap();
    timeout(Duration::from_secs(2), relays.wait_connections(13))
        .await
        .unwrap();
    let tracker = relays.dispatch_tracker();
    let cross_header =
        PeerHeader::request(cluster, ids[1], ids[0], GroupId::new(1), PeerKind::Vote);
    let native_owner = tracker.try_admit(&cross_header).unwrap();
    let installed = relays.install_edge_cut(edges.clone());
    let cross_admitted = tracker.admitted_for_edges(&edges);
    // Synchronous install seals all eight receiver edges in one generation.
    let sealed = edges.iter().all(|(source, target)| {
        tracker
            .try_admit(&PeerHeader::request(
                cluster,
                *source,
                *target,
                GroupId::new(1),
                PeerKind::Vote,
            ))
            .is_err()
    });
    let internal_admitted = [
        (ids[1], ids[2]),
        (ids[2], ids[1]),
        (ids[0], ids[3]),
        (ids[3], ids[0]),
    ]
    .into_iter()
    .all(|(source, target)| {
        tracker
            .try_admit(&PeerHeader::request(
                cluster,
                source,
                target,
                GroupId::new(1),
                PeerKind::Vote,
            ))
            .is_ok()
    });
    let first = {
        let mut drain = std::pin::pin!(relays.drain_edge_cut(&installed));
        std::future::poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx))).await
    };
    let still_owned = tracker.active_for(ids[1]);
    drop(native_owner);
    let ack = timeout(Duration::from_secs(2), relays.drain_edge_cut(&installed))
        .await
        .unwrap();
    let mut closed = true;
    let mut preserved = true;
    for (edge, stream) in &mut streams {
        if edges.contains(edge) {
            closed &= matches!(
                timeout(Duration::from_millis(100), stream.read_u8()).await,
                Ok(Err(_))
            );
        } else {
            let write = stream.write_all(b"still-live").await;
            let mut echoed = [0; 10];
            preserved &= write.is_ok()
                && matches!(
                    timeout(Duration::from_secs(2), stream.read_exact(&mut echoed)).await,
                    Ok(Ok(_))
                )
                && &echoed == b"still-live";
        }
    }
    let partial_closed = matches!(
        timeout(Duration::from_millis(100), partial.read_u8()).await,
        Ok(Err(_))
    );
    let mut reconnect = TcpStream::connect(addresses[&ids[0]]).await.unwrap();
    let write = reconnect
        .write_all(&edge_prefix(cluster, ids[1], ids[0]))
        .await;
    let reconnect_closed = write.is_err()
        || matches!(
            timeout(Duration::from_millis(100), reconnect.read_u8()).await,
            Ok(Err(_))
        );
    let cross_admitted_after = tracker.admitted_for_edges(&edges);
    drop(streams);
    drop(partial);
    drop(reconnect);
    relays.heal();
    relays.close().await;
    let _ = stop.send(true);
    for server in servers {
        server.await.unwrap();
    }
    assert!(
        sealed,
        "atomic edge-set installation must seal all eight cross dispatches"
    );
    assert_eq!(cross_admitted, 1);
    assert_eq!(
        cross_admitted_after, cross_admitted,
        "sealed cross reconnects cannot enter native dispatch"
    );
    assert!(
        internal_admitted && preserved,
        "BC and AD internal traffic must remain live"
    );
    assert!(
        first.is_pending(),
        "full ACK must wait for the retained native owner"
    );
    assert_eq!(
        still_owned, 1,
        "installation cannot cancel an invoked native future"
    );
    assert!(
        closed && partial_closed && reconnect_closed,
        "cut must join cross/unknown streams and reject cross reconnects"
    );
    assert_eq!(ack.generation, installed.generation);
    assert_eq!(ack.blocked_edges, edges);
    assert_eq!(ack.surviving_connections, 0);
    assert_eq!(ack.earlier_dispatches, 0);
}

fn edge_prefix(cluster: ClusterId, source: NodeId, target: NodeId) -> Vec<u8> {
    let mut bytes = vec![0; 4 + PEER_HEADER_BYTES];
    bytes[..4].copy_from_slice(&u32::try_from(PEER_HEADER_BYTES + 1).unwrap().to_be_bytes());
    let mut header = [0; PEER_HEADER_BYTES];
    PeerHeader::request(cluster, source, target, GroupId::new(1), PeerKind::Vote)
        .encode_fixed(&mut header);
    bytes[4..].copy_from_slice(&header);
    bytes.push(42);
    bytes
}

#[tokio::test]
async fn acknowledged_cut_closes_active_stream_and_blocks_reconnect() {
    let cluster = ClusterId::new([101; 16]).unwrap();
    let source = NodeId::new(1).unwrap();
    let target = NodeId::new(3).unwrap();
    let private = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let private_address = private.local_addr().unwrap();
    let advertised = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = advertised.local_addr().unwrap();
    let (stop, mut stopped) = watch::channel(false);
    let forwarded = Arc::new(Notify::new());
    let observed = Arc::clone(&forwarded);
    let release_response = Arc::new(Notify::new());
    let response_release = Arc::clone(&release_response);
    let echo = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                _ = stopped.changed() => break,
                accepted = private.accept() => {
                    let (mut stream, _) = accepted.unwrap();
                    let observed = Arc::clone(&observed);
                    let response_release = Arc::clone(&response_release);
                    tasks.spawn(async move {
                        let mut initial = [0; 4 + PEER_HEADER_BYTES + 16];
                        if stream.read_exact(&mut initial).await.is_err() { return; }
                        if stream.write_all(&initial).await.is_err() { return; }
                        let mut active = [0; 11];
                        if stream.read_exact(&mut active).await.is_err() { return; }
                        if &active != b"active-copy" { return; }
                        // A private receipt proves bytes crossed the relay.
                        // Hold this response so the cut interrupts an actual
                        // in-flight exchange, rather than a completed request.
                        observed.notify_one();
                        response_release.notified().await;
                        let _ = stream.write_all(&active).await;
                    });
                }
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    });
    let mut relays = PeerRelays::start(
        cluster,
        vec![(target, advertised, private_address)],
        PeerLimits::default(),
    );
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut bytes = vec![0; 4 + PEER_HEADER_BYTES];
    bytes[..4].copy_from_slice(&u32::try_from(PEER_HEADER_BYTES + 16).unwrap().to_be_bytes());
    let mut header = [0; PEER_HEADER_BYTES];
    PeerHeader::request(
        cluster,
        source,
        target,
        GroupId::new(1),
        PeerKind::AppendEntries,
    )
    .encode_fixed(&mut header);
    bytes[4..].copy_from_slice(&header);
    bytes.extend_from_slice(b"original-payload");
    stream.write_all(&bytes).await.unwrap();
    let mut echoed = vec![0; bytes.len()];
    timeout(Duration::from_secs(2), stream.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        echoed, bytes,
        "relay preserves the original prefix and payload"
    );
    // Retain an independently opened idle pooled stream through the same cut.
    let mut idle = TcpStream::connect(address).await.unwrap();
    idle.write_all(&bytes).await.unwrap();
    let mut idle_echoed = vec![0; bytes.len()];
    timeout(Duration::from_secs(2), idle.read_exact(&mut idle_echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idle_echoed, bytes);
    // Positively observe private receipt, with the reply still held at cut.
    stream.write_all(b"active-copy").await.unwrap();
    timeout(Duration::from_secs(2), forwarded.notified())
        .await
        .unwrap();
    let ack = relays.cut(target).await;
    release_response.notify_one();
    let after_ack = timeout(Duration::from_millis(100), stream.read_u8()).await;
    let idle_after_ack = timeout(Duration::from_millis(100), idle.read_u8()).await;
    let mut reconnect = TcpStream::connect(address).await.unwrap();
    let reconnect_write = reconnect.write_all(&bytes).await;
    let reconnected = timeout(Duration::from_millis(100), reconnect.read_u8()).await;
    // Complete owned cleanup before reporting a failed behavioral assertion.
    drop(stream);
    drop(idle);
    drop(reconnect);
    relays.heal();
    relays.close().await;
    let _ = stop.send(true);
    echo.await.unwrap();
    assert!(ack.generation > 0);
    assert_eq!(ack.blocked_node, target);
    assert_eq!(ack.surviving_connections, 0);
    assert!(
        matches!(after_ack, Ok(Err(_))),
        "acknowledged cut must close an already-open active stream; got {after_ack:?}"
    );
    assert!(
        matches!(idle_after_ack, Ok(Err(_))),
        "acknowledged cut must close an idle pooled stream; got {idle_after_ack:?}"
    );
    assert!(
        reconnect_write.is_err() || matches!(reconnected, Ok(Err(_))),
        "reconnect must not forward after acknowledged cut; got {reconnected:?}"
    );
}

#[tokio::test]
async fn cancelled_cut_retains_and_joins_unknown_partial_header_task() {
    let cluster = ClusterId::new([102; 16]).unwrap();
    let target = NodeId::new(2).unwrap();
    let cut_node = NodeId::new(3).unwrap();
    let private = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let advertised = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = advertised.local_addr().unwrap();
    let mut relays = PeerRelays::start(
        cluster,
        vec![(target, advertised, private.local_addr().unwrap())],
        PeerLimits::default(),
    );
    let mut partial = TcpStream::connect(address).await.unwrap();
    partial.write_all(&[0, 0, 0, 64, 1]).await.unwrap();
    timeout(Duration::from_secs(2), relays.wait_connections(1))
        .await
        .unwrap();
    {
        let mut cut = std::pin::pin!(relays.cut(cut_node));
        // Poll once on this current-thread runtime: cut installs the seal and
        // aborts the child, but its listener cannot have joined the child yet.
        let first = std::future::poll_fn(|cx| Poll::Ready(cut.as_mut().poll(cx))).await;
        assert!(
            first.is_pending(),
            "cut must await the owned connection join"
        );
    }
    let ack = timeout(Duration::from_secs(2), relays.cut(cut_node))
        .await
        .unwrap();
    let closed = timeout(Duration::from_secs(2), partial.read_u8()).await;
    // A healthy target must still reject the cut node as a newly decoded
    // source. This cannot rely on rejecting a blocked target at accept time.
    let mut reconnect = TcpStream::connect(address).await.unwrap();
    let mut prefix = [0; 4 + PEER_HEADER_BYTES];
    prefix[..4].copy_from_slice(&u32::try_from(PEER_HEADER_BYTES + 1).unwrap().to_be_bytes());
    let mut header = [0; PEER_HEADER_BYTES];
    PeerHeader::request(cluster, cut_node, target, GroupId::new(1), PeerKind::Vote)
        .encode_fixed(&mut header);
    prefix[4..].copy_from_slice(&header);
    let reconnect_write = reconnect.write_all(&prefix).await;
    let reconnect_closed = timeout(Duration::from_secs(2), reconnect.read_u8()).await;
    let reached_private = timeout(Duration::from_millis(30), private.accept()).await;
    drop(partial);
    drop(reconnect);
    relays.close().await;
    // Calling close again never polls an already-completed JoinHandle.
    relays.close().await;
    assert_eq!(ack.surviving_connections, 0);
    assert!(
        matches!(closed, Ok(Err(_))),
        "pre-cut unknown partial header must close: {closed:?}"
    );
    assert!(
        reached_private.is_err(),
        "partial header and blocked-source reconnect must never reach private listener"
    );
    assert!(
        reconnect_write.is_err() || matches!(reconnect_closed, Ok(Err(_))),
        "cut source cannot reconnect to healthy target: {reconnect_closed:?}"
    );
}

#[tokio::test]
async fn cut_seals_buffered_dispatch_and_drains_admitted_native_owner() {
    let cluster = ClusterId::new([103; 16]).unwrap();
    let source = NodeId::new(1).unwrap();
    let target = NodeId::new(3).unwrap();
    let mut relays = PeerRelays::start(cluster, vec![], PeerLimits::default());
    let tracker = relays.dispatch_tracker();
    let buffered = PeerHeader::request(cluster, source, target, GroupId::new(1), PeerKind::Vote);
    // This represents native work invoked before the cut; the fixture must
    // retain ownership until it finishes, rather than cancel it with a socket.
    let native_owner = tracker.try_admit(&buffered).unwrap();
    let first = {
        let mut cut = std::pin::pin!(relays.cut(target));
        std::future::poll_fn(|cx| Poll::Ready(cut.as_mut().poll(cx))).await
    };
    let still_owned = tracker.active_for(target);
    // Already-decoded receiver bytes must consult the installed seal too.
    let buffered_rejected = tracker.try_admit(&buffered).is_err();
    let admitted = tracker.admitted_for(target);
    drop(native_owner);
    let ack = timeout(Duration::from_secs(2), relays.cut(target))
        .await
        .unwrap();
    relays.close().await;
    assert!(
        first.is_pending(),
        "cut must drain the already-admitted native dispatch owner before ACK; got {first:?}"
    );
    assert_eq!(
        still_owned, 1,
        "cut must not cancel an invoked native operation"
    );
    assert!(
        buffered_rejected,
        "buffered request must be rejected by the final admission seal"
    );
    assert_eq!(
        admitted, 1,
        "buffered request cannot acquire a native dispatch guard"
    );
    assert_eq!(tracker.active_for(target), 0);
    assert_eq!(ack.surviving_connections, 0);
    assert_eq!(ack.earlier_dispatches, 0);
}

#[tokio::test]
async fn receiver_rejects_buffered_request_after_cut_seal() {
    let cluster = ClusterId::new([104; 16]).unwrap();
    let source = NodeId::new(1).unwrap();
    let target = NodeId::new(3).unwrap();
    let limits = PeerLimits::default();
    let resources = Arc::new(PeerResources::new(limits).unwrap());
    let mut operations = Vec::new();
    for _ in 0..limits.max_control_operations {
        operations.push(resources.acquire_incoming_control().await.unwrap());
    }
    // Leave exactly one connection lane for the server. Its acquisition
    // positively proves the header was decoded before installing the seal.
    let mut connections = Vec::new();
    for _ in 1..(limits.max_connections - limits.max_snapshot_operations) {
        connections.push(resources.try_acquire_incoming_control_connection().unwrap());
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut relays = PeerRelays::start(cluster, vec![], limits);
    let tracker = relays.dispatch_tracker();
    let mut server = PeerServer::start_with_dispatch_tracker(
        listener,
        PeerIdentity {
            cluster_id: cluster,
            node_id: target,
        },
        BTreeMap::new(),
        Arc::clone(&resources),
        tracker.clone(),
    );
    let mut stream = TcpStream::connect(address).await.unwrap();
    let request = PeerRequest {
        header: PeerHeader::request(cluster, source, target, GroupId::new(1), PeerKind::Vote),
        body: PeerRequestBody::Vote(openraft::raft::VoteRequest::new(
            openraft::Vote::new(1, source.get()),
            None,
        )),
    };
    stream
        .write_all(&encode_request(&request, limits.max_frame_bytes).unwrap())
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match resources.try_acquire_incoming_control_connection() {
            Err(_) => break,
            Ok(permit) => drop(permit),
        }
        assert!(
            Instant::now() < deadline,
            "receiver must decode the buffered request header"
        );
        sleep(Duration::from_millis(1)).await;
    }
    let admitted_before = tracker.admitted_for(target);
    let ack = timeout(Duration::from_secs(2), relays.cut(target))
        .await
        .unwrap();
    // The full body was sent before sealing and the receiver was blocked at
    // its operation lane. It can now decode but cannot enter dispatch.
    drop(operations);
    let rejected = timeout(Duration::from_secs(2), stream.read_u8()).await;
    let admitted_after = tracker.admitted_for(target);
    drop(stream);
    drop(connections);
    server.close().await;
    relays.close().await;
    assert_eq!(admitted_before, 0);
    assert_eq!(
        admitted_after, admitted_before,
        "buffered request must not reach native dispatch"
    );
    assert_eq!(ack.earlier_dispatches, 0);
    assert!(
        matches!(rejected, Ok(Err(_))),
        "sealed buffered request must close rather than dispatch to unknown group: {rejected:?}"
    );
}
