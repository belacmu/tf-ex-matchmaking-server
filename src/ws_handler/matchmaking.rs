use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use axum::{
    extract::{ConnectInfo, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    TypedHeader,
};
use axum_typed_websockets::{Message, WebSocket, WebSocketUpgrade};
use futures::{SinkExt, StreamExt};
use headers::HeaderMap;
use metrics::counter;
use tokio::{
    sync::mpsc,
    time::{interval, Duration},
};
use tracing::{debug, error, info, warn};

use crate::{
    constants::KEEP_ALIVE_INTERVAL,
    extensions::{clients::ClientsExts, str::StrExts},
    msg::{ClientMsg, ServerMsg},
    state::{
        lobby::{Departure, Lobby},
        AppState, QuickPlayOutcome,
    },
    telemetry,
    version::ExVersion,
};

pub async fn matchmaking_handler(
    ws: WebSocketUpgrade<ServerMsg, ClientMsg>,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Response {
    if state.is_draining() {
        return StatusCode::NOT_FOUND.into_response();
    }

    let version = params
        .get("version")
        .and_then(|version| ExVersion::parse(version));

    match check_version(state.min_client_version(), version) {
        VersionCheck::Accepted => {}
        VersionCheck::Unknown(required) => {
            info!(
                "Refusing a client without TF.EX version (requires {})",
                required
            );
            return StatusCode::UPGRADE_REQUIRED.into_response();
        }
        VersionCheck::Outdated(required) => {
            info!(
                "Refusing TF.EX {} (requires {})",
                version.map(|v| v.to_string()).unwrap_or_default(),
                required
            );
            return ws
                .on_upgrade(move |socket| refuse_outdated(socket, required))
                .into_response();
        }
    }

    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };

    let mut addressv4: Option<SocketAddr> = None;
    let mut addressv6: Option<SocketAddr> = None;

    for (key, value) in headers.iter() {
        match key.as_str().to_lowercase().as_str() {
            "x-forwarded-for" => {
                if value.to_str().unwrap().contains(",") {
                    let splitted = value.to_str().unwrap().split(",").collect::<Vec<&str>>();
                    for ip in splitted.iter().filter(|ip| !ip.is_an_ip_and_private()) {
                        if let Ok(ipv6) = Ipv6Addr::from_str(ip) {
                            if addressv6.is_some() {
                                continue;
                            }

                            addressv6 =
                                Some(SocketAddr::new(std::net::IpAddr::V6(ipv6), addr.port()));
                        } else if let Ok(ipv4) = ip.parse::<Ipv4Addr>() {
                            if addressv4.is_some() {
                                continue;
                            }

                            addressv4 =
                                Some(SocketAddr::new(std::net::IpAddr::V4(ipv4), addr.port()));
                        }
                    }
                } else {
                    if let Ok(ipv6) = Ipv6Addr::from_str(value.to_str().unwrap()) {
                        addressv6 = Some(SocketAddr::new(std::net::IpAddr::V6(ipv6), addr.port()));
                    } else if let Ok(ipv4) = value.to_str().unwrap().parse::<Ipv4Addr>() {
                        addressv4 = Some(SocketAddr::new(std::net::IpAddr::V4(ipv4), addr.port()));
                    }
                }
            }

            _ => {}
        }
    }

    if addressv4.is_none() && addressv6.is_none() {
        addressv4 = Some(addr);
    }

    info!("New connection from `{user_agent}`");

    let ips: Vec<SocketAddr> = vec![addressv4, addressv6]
        .into_iter()
        .filter_map(|x| x)
        .collect();

    ws.on_upgrade(move |socket| handle_socket(socket, ips, state.clone()))
        .into_response()
}

#[derive(Debug, PartialEq)]
enum VersionCheck {
    Accepted,
    Unknown(ExVersion),
    Outdated(ExVersion),
}

fn check_version(required: Option<ExVersion>, version: Option<ExVersion>) -> VersionCheck {
    match (required, version) {
        (None, _) => VersionCheck::Accepted,
        (Some(required), None) => VersionCheck::Unknown(required),
        (Some(required), Some(version)) if version < required => VersionCheck::Outdated(required),
        _ => VersionCheck::Accepted,
    }
}

async fn refuse_outdated(mut socket: WebSocket<ServerMsg, ClientMsg>, required: ExVersion) {
    let _ = socket
        .send(Message::Item(ServerMsg::UpdateRequired {
            version: required.to_string(),
        }))
        .await;
    let _ = socket.send(Message::Close(None)).await;
}

/// Actual websocket statemachine (one will be spawned per connection)
pub async fn handle_socket(
    socket: WebSocket<ServerMsg, ClientMsg>,
    ips: Vec<SocketAddr>,
    state: Arc<AppState>,
) {
    let Some(ip) = ips
        .iter()
        .find(|x| x.is_ipv4())
        .or_else(|| ips.first())
        .copied()
    else {
        return;
    };
    let client = state.clone().clients.get_client(ip).await;
    if client.is_some() {
        info!("{} is already connected", state.client_label(ip).await);
        return;
    }

    let res = state.clone().add_new_client(ips.clone()).await;
    if res.is_err() {
        info!("New client could not be added : {}", res.unwrap_err());
        return;
    }

    let (mut sink, mut stream) = socket.split();
    let (sender, mut receiver) = mpsc::channel::<Message<ServerMsg>>(512);

    // spawn a task that forwards messages from the mpsc to the sink
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            if sink.send(message.into()).await.is_err() {
                break;
            }
        }
    });

    match state.clone().add_connexion(ip, sender.clone()).await {
        Ok(()) => {
            counter!(telemetry::CONNECTIONS_TOTAL).increment(1);
        }
        Err(err) => {
            warn!("New client connection could not be registered : {}", err);
            return;
        }
    }

    let cloned_state_recv = state.clone();
    let cloned_state_keep_alive = state.clone();

    let send_task_in_rcv = sender.clone();
    let send_task_in_keep_alive = sender.clone();

    let mut recv_task = tokio::spawn(async move {
        while let Some(msg) = stream.next().await {
            let who = if matches!(msg, Ok(Message::Item(ClientMsg::KeepAlive {}))) {
                String::new()
            } else {
                debug!("Received message: {:?}", msg);
                cloned_state_recv.client_label(ip).await
            };

            match msg {
                Ok(Message::Item(item)) => match item {
                    ClientMsg::KeepAlive {} => {
                        match cloned_state_recv.update_alive_client(ip).await {
                            Ok(()) => {}
                            Err(err) => {
                                error!("Error when trying to make client alive : {}", err);
                            }
                        }

                        cloned_state_recv.record_keep_alive_rtt(ip).await;
                    }
                    ClientMsg::Identify { player_id, name } => {
                        let claimed = format!("{} ({})", name, player_id);

                        if let Err(err) =
                            cloned_state_recv.identify_client(ip, player_id, name).await
                        {
                            warn!("Identify from {} rejected : {}", claimed, err);
                        }
                    }
                    ClientMsg::GetLobbies {} => {
                        debug!("{} requested lobbies", who);

                        let lobbies: Vec<Lobby> = cloned_state_recv.get_lobbies(ip).await;

                        send_task_in_rcv
                            .send(Message::Item(ServerMsg::GetLobbiesResponse { lobbies }))
                            .await
                            .unwrap();
                    }
                    ClientMsg::GetLeaderboard {} => {
                        debug!("{} requested the leaderboard", who);

                        if let Some(view) = cloned_state_recv.leaderboard_for(ip).await {
                            send_task_in_rcv
                                .send(Message::Item(ServerMsg::Leaderboard {
                                    entries: view.entries,
                                    own: view.own,
                                    around: view.around,
                                    total: view.total,
                                }))
                                .await
                                .unwrap();
                        }
                    }
                    ClientMsg::GetPlayerProfile { rank } => {
                        debug!("{} requested the profile at rank {:?}", who, rank);

                        let profile = cloned_state_recv.player_profile_for(ip, rank).await;

                        send_task_in_rcv
                            .send(Message::Item(ServerMsg::PlayerProfile { rank, profile }))
                            .await
                            .unwrap();
                    }
                    ClientMsg::CreateLobby {
                        mut lobby,
                        local_players,
                    } => {
                        info!("{} wants to create a lobby", who);

                        cloned_state_recv.exit_quick_play(ip).await;
                        cloned_state_recv
                            .ranking()
                            .exit_ranked(&cloned_state_recv, ip)
                            .await;
                        cloned_state_recv.leave_stale_lobbies(ip, None).await;

                        match cloned_state_recv
                            .add_lobby(&mut lobby, ip, local_players)
                            .await
                        {
                            Ok(final_lobby) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::CreateLobbyResponse {
                                        success: true,
                                        message: "".to_string(),
                                        lobby: Some(final_lobby),
                                    }))
                                    .await
                                    .unwrap();
                            }
                            Err(err) => {
                                warn!("{} could not create a lobby : {}", who, err);
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::CreateLobbyResponse {
                                        success: false,
                                        message: err,
                                        lobby: None,
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::JoinLobby {
                        room_id,
                        name,
                        is_player,
                        local_players,
                        game_logic_speed,
                    } => {
                        info!("{who} wants to join lobby {room_id}, is a player {is_player}");

                        cloned_state_recv.exit_quick_play(ip).await;
                        cloned_state_recv
                            .ranking()
                            .exit_ranked(&cloned_state_recv, ip)
                            .await;
                        cloned_state_recv
                            .leave_stale_lobbies(ip, Some(&room_id))
                            .await;

                        match cloned_state_recv
                            .join_lobby(
                                room_id.clone(),
                                ip,
                                name,
                                is_player,
                                local_players,
                                game_logic_speed,
                            )
                            .await
                        {
                            Ok((lobby, room_peer_id)) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::JoinLobbyResponse {
                                        success: true,
                                        message: "".to_string(),
                                        room_peer_id: room_peer_id.clone(),
                                        lobby: Some(lobby),
                                    }))
                                    .await
                                    .unwrap();

                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }

                                if !is_player {
                                    cloned_state_recv
                                        .notify_host_of_late_spectator(&room_id, &room_peer_id)
                                        .await;
                                }
                            }
                            Err(err) => {
                                warn!("{} could not join lobby : {}", who, err);

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::JoinLobbyResponse {
                                        success: false,
                                        message: err,
                                        room_peer_id: "".to_string(),
                                        lobby: None,
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::EnterQuickPlay {
                        name,
                        is_wide,
                        local_players,
                        game_logic_speed,
                    } => {
                        info!("{who} entered the quickplay queue (wide: {is_wide}, local players: {local_players}, speed: {game_logic_speed})");

                        cloned_state_recv
                            .ranking()
                            .exit_ranked(&cloned_state_recv, ip)
                            .await;
                        cloned_state_recv.leave_stale_lobbies(ip, None).await;

                        let outcome = cloned_state_recv
                            .enter_quick_play(ip, name, is_wide, local_players, game_logic_speed)
                            .await;

                        let outcome_label = match &outcome {
                            QuickPlayOutcome::Queued { .. } => "queued",
                            QuickPlayOutcome::Seated { .. } => "seated",
                            QuickPlayOutcome::Paired { .. } => "paired",
                            QuickPlayOutcome::Failed { .. } => "failed",
                        };
                        counter!(telemetry::QUICKPLAY_ENTER_TOTAL, "outcome" => outcome_label)
                            .increment(1);

                        match outcome {
                            QuickPlayOutcome::Queued(counts) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::QuickPlayStatus {
                                        queued: true,
                                        searching_standard: counts.standard,
                                        searching_wide: counts.wide,
                                        searching_hz60: counts.hz60,
                                        searching_hz240: counts.hz240,
                                        message: "".to_string(),
                                    }))
                                    .await
                                    .unwrap();
                            }
                            QuickPlayOutcome::Seated {
                                lobby,
                                room_peer_id,
                            } => {
                                info!("{} seated into quickplay lobby {}", who, lobby.room_id());

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::QuickPlayMatchFound {
                                        lobby: lobby.clone(),
                                        room_peer_id,
                                    }))
                                    .await
                                    .unwrap();

                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(lobby.room_id()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            QuickPlayOutcome::Paired {
                                lobby,
                                caller_peer_id,
                                partner_addr,
                                partner_peer_id,
                            } => {
                                let partner = cloned_state_recv.member_label(&partner_addr).await;
                                info!(
                                    "{} paired with {} in quickplay lobby {}",
                                    who,
                                    partner,
                                    lobby.room_id()
                                );

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::QuickPlayMatchFound {
                                        lobby: lobby.clone(),
                                        room_peer_id: caller_peer_id,
                                    }))
                                    .await
                                    .unwrap();

                                cloned_state_recv
                                    .send_msg_to_addrs(
                                        &[partner_addr],
                                        ServerMsg::QuickPlayMatchFound {
                                            lobby,
                                            room_peer_id: partner_peer_id,
                                        },
                                    )
                                    .await;
                            }
                            QuickPlayOutcome::Failed { message } => {
                                warn!("Quickplay enter failed for {} : {}", who, message);

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::QuickPlayStatus {
                                        queued: false,
                                        searching_standard: 0,
                                        searching_wide: 0,
                                        searching_hz60: 0,
                                        searching_hz240: 0,
                                        message,
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::ExitQuickPlay {} => {
                        info!("{} left the quickplay queue", who);

                        let counts = cloned_state_recv.exit_quick_play(ip).await;

                        send_task_in_rcv
                            .send(Message::Item(ServerMsg::QuickPlayStatus {
                                queued: false,
                                searching_standard: counts.standard,
                                searching_wide: counts.wide,
                                searching_hz60: counts.hz60,
                                searching_hz240: counts.hz240,
                                message: "".to_string(),
                            }))
                            .await
                            .unwrap();
                    }
                    ClientMsg::EnterRanked { name, is_wide } => {
                        info!("{who} entered the ranked queue (wide: {is_wide})");

                        cloned_state_recv.leave_stale_lobbies(ip, None).await;

                        match cloned_state_recv
                            .ranking()
                            .enter_ranked(&cloned_state_recv, ip, name, is_wide)
                            .await
                        {
                            Ok(Some(counts)) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::RankedStatus {
                                        queued: true,
                                        searching_standard: counts.standard,
                                        searching_wide: counts.wide,
                                        message: "".to_string(),
                                    }))
                                    .await
                                    .unwrap();
                            }
                            Ok(None) => {}
                            Err(message) => {
                                warn!("Ranked enter failed for {} : {}", who, message);

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::RankedStatus {
                                        queued: false,
                                        searching_standard: 0,
                                        searching_wide: 0,
                                        message,
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::ExitRanked {} => {
                        info!("{} left the ranked queue", who);

                        cloned_state_recv
                            .ranking()
                            .exit_ranked(&cloned_state_recv, ip)
                            .await;

                        send_task_in_rcv
                            .send(Message::Item(ServerMsg::RankedStatus {
                                queued: false,
                                searching_standard: 0,
                                searching_wide: 0,
                                message: "".to_string(),
                            }))
                            .await
                            .unwrap();
                    }
                    ClientMsg::JoinPrivate {
                        code,
                        name,
                        is_player,
                        local_players,
                        game_logic_speed,
                    } => {
                        info!(
                            "{who} wants to join a private lobby by code, is a player {is_player}"
                        );

                        cloned_state_recv.exit_quick_play(ip).await;
                        cloned_state_recv
                            .ranking()
                            .exit_ranked(&cloned_state_recv, ip)
                            .await;
                        cloned_state_recv.leave_stale_lobbies(ip, None).await;

                        match cloned_state_recv
                            .join_private(
                                code,
                                ip,
                                name,
                                is_player,
                                local_players,
                                game_logic_speed,
                            )
                            .await
                        {
                            Ok((lobby, room_peer_id)) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::PrivateJoinResult {
                                        success: true,
                                        message: "".to_string(),
                                        room_peer_id: room_peer_id.clone(),
                                        lobby: Some(lobby.clone()),
                                    }))
                                    .await
                                    .unwrap();

                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(lobby.room_id()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }

                                if !is_player {
                                    cloned_state_recv
                                        .notify_host_of_late_spectator(
                                            &lobby.room_id(),
                                            &room_peer_id,
                                        )
                                        .await;
                                }
                            }
                            Err(err) => {
                                warn!("{} could not join private lobby : {}", who, err);

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::PrivateJoinResult {
                                        success: false,
                                        message: err,
                                        room_peer_id: "".to_string(),
                                        lobby: None,
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::UpdatePlayer { player } => {
                        debug!("{} wants to update player", who);

                        match cloned_state_recv.update_player(ip, player).await {
                            Ok(room_id) => {
                                match cloned_state_recv
                                    .initialize_end_game_choice(room_id.clone())
                                    .await
                                {
                                    Ok(()) => {}
                                    Err(err) => {
                                        error!("Error initializing end game choice : {}", err);
                                    }
                                }

                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }

                                cloned_state_recv.try_start_series_game(room_id).await;
                            }
                            Err(err) => {
                                error!("Error updating player : {}", err);
                            }
                        }
                    }
                    ClientMsg::UpdateLobbySettings {
                        max_players,
                        game_data,
                        mods,
                    } => {
                        debug!("{} wants to update lobby settings", who);

                        match cloned_state_recv
                            .update_lobby_settings(ip, max_players, game_data, mods)
                            .await
                        {
                            Ok(room_id) => {
                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                error!("Error updating lobby settings : {}", err);
                            }
                        }
                    }
                    ClientMsg::LeaveLobby { link_lost } => {
                        let departure = if link_lost {
                            info!("{} wants to leave lobby, game connection lost", who);
                            Departure::LinkLost
                        } else {
                            info!("{} wants to leave lobby", who);
                            Departure::Left
                        };

                        match cloned_state_recv.remove_from_lobby(ip, departure).await {
                            Ok(room_ids) => {
                                for room_id in room_ids {
                                    if let Err(err) =
                                        cloned_state_recv.send_lobby_update(room_id).await
                                    {
                                        warn!(
                                            "Could not send lobby update (lobby likely gone): {}",
                                            err
                                        );
                                    }
                                }

                                cloned_state_recv.purge_lobby_async().await;

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::LeaveLobbyResponse {
                                        success: true,
                                        message: "".to_string(),
                                    }))
                                    .await
                                    .unwrap();
                            }
                            Err(err) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::LeaveLobbyResponse {
                                        success: false,
                                        message: err,
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::StartLobbyChoice {} => {
                        info!("{} wants to start the lobby early", who);

                        if let Err(err) = cloned_state_recv.start_lobby(ip).await {
                            warn!("Error starting lobby : {}", err);
                        }
                    }
                    ClientMsg::MatchEnded {
                        winner_seat,
                        frame,
                        checksum,
                        scores,
                        stats,
                        tower,
                    } => {
                        if let Some(seat) = winner_seat {
                            cloned_state_recv
                                .handle_match_report(
                                    ip, seat, frame, checksum, scores, stats, tower,
                                )
                                .await;
                        }

                        if let Some(room_id) = cloned_state_recv.room_id_for(ip).await {
                            cloned_state_recv.start_end_game_countdown(room_id).await;
                        }
                    }
                    ClientMsg::SeriesContinueChoice {} => {
                        info!("{} is ready for the next series game", who);

                        match cloned_state_recv.series_continue_choice(ip).await {
                            Ok(room_id) => {
                                cloned_state_recv.end_game_action(room_id.clone()).await;
                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                error!("Error recording series continue : {}", err);
                            }
                        }
                    }
                    ClientMsg::SeriesPickMap { map_id } => {
                        info!("{} picks tower {} for the series", who, map_id);

                        match cloned_state_recv.pick_series_map(ip, map_id).await {
                            Ok(room_id) => {
                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }

                                cloned_state_recv.try_start_series_game(room_id).await;
                            }
                            Err(err) => {
                                warn!("Series pick from {} rejected : {}", who, err);
                            }
                        }
                    }
                    ClientMsg::RematchLobbyChoice {} => {
                        info!("{} wants to rematch lobby", who);

                        match cloned_state_recv.rematch_lobby(ip).await {
                            Ok(room_id) => {
                                cloned_state_recv.end_game_action(room_id.clone()).await;
                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                error!("Error rematching lobby : {}", err);
                            }
                        }
                    }
                    ClientMsg::PeerMeasured { room_peer_id } => {
                        cloned_state_recv
                            .record_peer_measured(ip, room_peer_id)
                            .await;
                    }
                    ClientMsg::SkinChunk { chunk } => {
                        if let Err(err) = cloned_state_recv.relay_skin_chunk(ip, chunk).await {
                            warn!("Skin chunk from {} rejected : {}", who, err);
                        }
                    }
                    ClientMsg::ArcherSelectChoice {} => {
                        info!("{} wants to go select archer", who);

                        match cloned_state_recv.archer_select_choice_lobby(ip).await {
                            Ok(room_id) => {
                                cloned_state_recv.end_game_action(room_id.clone()).await;
                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                error!("Error go selecting archer : {}", err);
                            }
                        }
                    }
                },
                Ok(Message::Close(_fame)) => {
                    info!("{} closed connection", who);

                    send_task_in_rcv
                        .send(Message::Close(None))
                        .await
                        .unwrap_or_else(|_| ());

                    break;
                }
                Ok(_) => {}
                Err(err) => {
                    warn!("Error receiving message : {}", err);
                }
            };
        }
    });

    let mut keep_alive_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(KEEP_ALIVE_INTERVAL));

        loop {
            interval.tick().await;
            let mut clients = cloned_state_keep_alive.clients.lock().await;

            let client = clients
                .iter_mut()
                .find(|((ipv4, _), _)| *ipv4 == ip)
                .map(|c| c.1);

            let Some(client) = client else {
                info!("Keep alive stopped, client already removed");
                return "timeout";
            };

            if !client.is_alive() {
                info!("{} is no longer alive, aborting...", client.label());
                return "timeout";
            }

            client.keep_alive_sent();
            client.maybe_not_alive();
            drop(clients);

            if send_task_in_keep_alive
                .send(Message::Item(ServerMsg::KeepAlive {}))
                .await
                .is_err()
            {
                return "clean";
            }
        }
    });

    let disconnect_reason;

    tokio::select! {
        reason = (&mut keep_alive_task) => {
            recv_task.abort();
            disconnect_reason = reason.unwrap_or("timeout");
        },
        _ = (&mut recv_task) => {
            keep_alive_task.abort();
            disconnect_reason = "clean";
        }
    }

    counter!(telemetry::DISCONNECTS_TOTAL, "reason" => disconnect_reason).increment(1);

    let who = state.client_label(ip).await;

    state.exit_quick_play(ip).await;
    state.ranking().exit_ranked(&state, ip).await;

    let mut clients = state.clone().clients.clone();

    let has_room = clients
        .get_client(ip)
        .await
        .is_some_and(|cli| cli.room.is_some());

    let departure = if disconnect_reason == "timeout" {
        state.ranking().note_connection_lost(&state, ip).await;
        Departure::Disconnected
    } else {
        Departure::Left
    };

    match state.remove_from_lobby(ip, departure).await {
        Ok(room_ids) => {
            info!("{} was in a lobby", who);

            for room_id in room_ids {
                if let Err(err) = state.send_lobby_update(room_id).await {
                    warn!("Could not send lobby update (lobby likely gone): {}", err);
                }
            }

            state.purge_lobby_async().await;
        }
        Err(err) if has_room => {
            warn!(
                "Error removing {} from lobby (might already be deleted): {}",
                who, err
            );
        }
        Err(_) => {}
    }

    clients.remove_client(ip).await;

    match state.clone().remove_connexion(ip).await {
        Ok(()) => {}
        Err(err) => {
            error!("Error removing connexion : {}", err);
        }
    }

    info!("Websocket context for {} destroyed", who);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_check_follows_the_minimum() {
        let required = ExVersion::new(0, 22, 0);

        assert_eq!(check_version(None, None), VersionCheck::Accepted);
        assert_eq!(
            check_version(Some(required), None),
            VersionCheck::Unknown(required)
        );
        assert_eq!(
            check_version(Some(required), Some(ExVersion::new(0, 21, 1))),
            VersionCheck::Outdated(required)
        );
        assert_eq!(
            check_version(Some(required), Some(required)),
            VersionCheck::Accepted
        );
        assert_eq!(
            check_version(Some(required), Some(ExVersion::new(0, 23, 0))),
            VersionCheck::Accepted
        );
    }
}
