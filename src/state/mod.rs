pub mod broadcast;
pub mod client;
pub mod codes;
pub mod connectivity;
pub mod countdown;
pub mod drain;
pub mod end_game;
pub mod lobby;
pub mod match_report;
pub mod queue;
pub mod quickplay;
pub mod series;
pub mod signaling_state;

pub use quickplay::{should_auto_start, QuickPlayOutcome, WIDER_SET_MOD_NAME};

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

use axum_typed_websockets::Message;
use metrics::{counter, gauge, histogram};
use tokio::sync::{mpsc::Sender, Mutex};
use tracing::{info, warn};

use crate::{
    constants::{LEADERBOARD_COOLDOWN_SECS, PROFILE_COOLDOWN_MS, SERVER_UPDATED_MESSAGE},
    msg::{LeaderboardView, PlayerProfile, ServerMsg},
    ranking::{NoRanking, Ranking},
    relay::{DropReason, Relay},
    reports::Reports,
    services::geoip::GeoipClient,
    state::lobby::{LobbyBuilder, LobbyKind},
    telemetry,
    version::ExVersion,
};

use self::{
    client::{Client, Room},
    lobby::{is_valid_local_players, Departure, GameData, Lobby, Mod, Player},
    series::SeriesStatus,
    signaling_state::SignalingState,
};

/// The test harness's line for a match start (the same words as the browser repo's local server)
pub fn started_line(lobby: &Lobby) -> String {
    let names: Vec<String> = lobby.players().iter().map(|p| p.name()).collect();

    format!("[lobby] {} started ({})", lobby.room_id(), names.join(" vs "))
}

pub type IpV4V6 = (SocketAddr, Option<SocketAddr>);
pub type Clients = Arc<Mutex<HashMap<IpV4V6, Client>>>;
pub type Lobbies = Arc<Mutex<Vec<Lobby>>>;

#[derive(Debug, Clone)]
pub struct AppState {
    pub clients: Clients,
    geoip_client: GeoipClient,
    pub signaling: Arc<Mutex<SignalingState>>,
    pub ping_measurement_signaling: Arc<Mutex<SignalingState>>,
    pub lobbies: Lobbies,
    connections: Arc<Mutex<HashMap<SocketAddr, Sender<Message<ServerMsg>>>>>,
    pub countdowns: countdown::Countdowns,
    pub queues: queue::Queues,
    match_reports: Arc<Mutex<HashMap<String, match_report::PendingReports>>>,
    relay: Option<Arc<Relay>>,
    reports: Option<Arc<Reports>>,
    draining: Arc<AtomicBool>,
    min_client_version: Option<ExVersion>,
    ranking: Arc<dyn Ranking>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            geoip_client: GeoipClient::new(),
            signaling: Arc::new(Mutex::new(SignalingState::default())),
            ping_measurement_signaling: Arc::new(Mutex::new(SignalingState::default())),
            lobbies: Arc::new(Mutex::new(vec![])),
            connections: Arc::new(Mutex::new(HashMap::new())),
            countdowns: countdown::Countdowns::default(),
            queues: queue::Queues::default(),
            match_reports: Arc::new(Mutex::new(HashMap::new())),
            relay: None,
            reports: None,
            draining: Arc::new(AtomicBool::new(false)),
            min_client_version: None,
            ranking: Arc::new(NoRanking),
        }
    }

    pub fn with_relay(mut self, relay: Option<Arc<Relay>>) -> Self {
        self.relay = relay;
        self
    }

    pub fn relay(&self) -> Option<Arc<Relay>> {
        self.relay.clone()
    }

    pub fn with_ranking(mut self, ranking: Arc<dyn Ranking>) -> Self {
        self.ranking = ranking;
        self
    }

    pub fn ranking(&self) -> Arc<dyn Ranking> {
        self.ranking.clone()
    }

    pub fn with_min_client_version(mut self, version: ExVersion) -> Self {
        self.min_client_version = Some(version);
        self
    }

    pub fn min_client_version(&self) -> Option<ExVersion> {
        self.min_client_version
    }

    pub fn with_reports(mut self, reports: Option<Arc<Reports>>) -> Self {
        self.reports = reports;
        self
    }

    pub fn reports(&self) -> Option<Arc<Reports>> {
        self.reports.clone()
    }

    pub fn lobbies(&self) -> Lobbies {
        self.lobbies.clone()
    }

    pub async fn publish_metrics_gauges(&self) {
        let quick_play_counts = self.queues.quick_play.counts().await;

        let kinds = [
            LobbyKind::Standard,
            LobbyKind::QuickPlay,
            LobbyKind::Private,
            LobbyKind::Ranked,
        ];
        let mut lobby_counts = [0usize; 4];
        let mut game_counts = [0usize; 4];
        let mut player_counts = [0usize; 4];
        let mut spectators = 0usize;

        {
            let lobbies = self.lobbies.lock().await;

            for lobby in lobbies.iter() {
                let slot = kinds.iter().position(|k| *k == lobby.kind()).unwrap();

                lobby_counts[slot] += 1;
                player_counts[slot] += lobby.players().len();
                spectators += lobby.spectators().len();

                if lobby.in_game() {
                    game_counts[slot] += 1;
                }
            }
        }

        let (clients, clients_by_country) = {
            let clients = self.clients.lock().await;
            let mut by_country: HashMap<String, usize> = HashMap::new();

            for client in clients.values() {
                let country = client.location.get_country_code();
                let country = if country.is_empty() {
                    "unknown".to_string()
                } else {
                    country
                };

                *by_country.entry(country).or_default() += 1;
            }

            (clients.len(), by_country)
        };

        let (signaling_rooms, signaling_peers) = {
            let signaling = self.signaling.lock().await;

            (signaling.rooms.len(), signaling.clients.len())
        };

        gauge!(telemetry::QUICKPLAY_QUEUE_DEPTH, "wide" => "true")
            .set(quick_play_counts.wide as f64);
        gauge!(telemetry::QUICKPLAY_QUEUE_DEPTH, "wide" => "false")
            .set(quick_play_counts.standard as f64);

        for (slot, kind) in kinds.iter().enumerate() {
            let label = telemetry::kind_label(*kind);

            gauge!(telemetry::LOBBIES, "kind" => label).set(lobby_counts[slot] as f64);
            gauge!(telemetry::GAMES_ACTIVE, "kind" => label).set(game_counts[slot] as f64);
            gauge!(telemetry::PLAYERS_SEATED, "kind" => label).set(player_counts[slot] as f64);
        }

        gauge!(telemetry::SPECTATORS).set(spectators as f64);
        gauge!(telemetry::CLIENTS_CONNECTED).set(clients as f64);
        telemetry::publish_clients_by_country(clients_by_country);
        gauge!(telemetry::SIGNALING_ROOMS).set(signaling_rooms as f64);
        gauge!(telemetry::SIGNALING_PEERS).set(signaling_peers as f64);
        gauge!(telemetry::DRAINING).set(if self.is_draining() { 1.0 } else { 0.0 });

        if let Some(relay) = &self.relay {
            let stats = relay.stats();

            gauge!(telemetry::RELAY_ROOMS).set(stats.relaying_rooms as f64);
            gauge!(telemetry::RELAY_MEMBERS).set(stats.relaying_members as f64);
            counter!(telemetry::RELAY_DATAGRAMS_TOTAL, "outcome" => "received")
                .absolute(stats.received);
            counter!(telemetry::RELAY_DATAGRAMS_TOTAL, "outcome" => "forwarded")
                .absolute(stats.forwarded);

            for reason in DropReason::ALL {
                counter!(
                    telemetry::RELAY_DATAGRAMS_TOTAL,
                    "outcome" => "dropped",
                    "reason" => reason.label()
                )
                .absolute(stats.dropped(reason));
            }
        }

        let pending_reports = self.match_reports.lock().await.len();
        gauge!(telemetry::MATCH_REPORTS_PENDING).set(pending_reports as f64);

        self.ranking.publish_metrics(self).await;
    }

    pub async fn client_label(&self, addr: SocketAddr) -> String {
        self.clients
            .lock()
            .await
            .iter()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || *ipv6 == Some(addr))
            .map(|(_, client)| client.label())
            .unwrap_or_else(|| "unknown client".to_string())
    }

    pub async fn member_label(&self, addr: &str) -> String {
        match addr.parse::<SocketAddr>() {
            Ok(addr) => self.client_label(addr).await,
            Err(_) => "unknown client".to_string(),
        }
    }

    pub async fn identify_client(
        &self,
        addr: SocketAddr,
        player_id: String,
        name: String,
    ) -> Result<(), String> {
        let valid_prefix = player_id.starts_with("steam:") || player_id.starts_with("local:");

        if !valid_prefix || player_id.len() > 64 {
            return Err(format!("Invalid player id '{}'", player_id));
        }

        {
            let mut clients = self.clients.lock().await;

            let client = clients
                .iter_mut()
                .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
                .map(|(_, client)| client)
                .ok_or("Client not found".to_string())?;

            client.player_id = Some(player_id.clone());
            client.name = name.clone();
        }

        self.ranking.on_identified(self, &player_id, &name).await?;

        Ok(())
    }

    pub async fn leaderboard_for(&self, addr: SocketAddr) -> Option<LeaderboardView> {
        let player_id = self
            .player_id_or_cooled(
                addr,
                |client| &mut client.last_leaderboard_request,
                Duration::from_secs(LEADERBOARD_COOLDOWN_SECS),
            )
            .await?;

        Some(self.ranking.leaderboard(self, player_id).await)
    }

    pub async fn player_profile_for(
        &self,
        addr: SocketAddr,
        rank: Option<u32>,
    ) -> Option<PlayerProfile> {
        let player_id = self
            .player_id_or_cooled(
                addr,
                |client| &mut client.last_profile_request,
                Duration::from_millis(PROFILE_COOLDOWN_MS),
            )
            .await?;

        self.ranking.player_profile(self, player_id, rank).await
    }

    async fn player_id_or_cooled(
        &self,
        addr: SocketAddr,
        last_request: fn(&mut Client) -> &mut Option<Instant>,
        cooldown: Duration,
    ) -> Option<Option<String>> {
        let mut clients = self.clients.lock().await;

        let client = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || *ipv6 == Some(addr))
            .map(|(_, client)| client)?;

        let last = last_request(client);

        if last.is_some_and(|at| at.elapsed() < cooldown) {
            return None;
        }

        *last = Some(Instant::now());

        Some(client.player_id.clone())
    }

    pub async fn update_alive_client(&self, addr: SocketAddr) -> Result<(), String> {
        let mut clients = self.clients.lock().await;

        let client_res = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr);

        if client_res.is_none() {
            return Err("Client not found".to_string());
        }

        let client = client_res.unwrap().1;

        if client.is_alive() {
            client.alive();
        }

        Ok(())
    }

    pub async fn record_keep_alive_rtt(&self, addr: SocketAddr) {
        let mut clients = self.clients.lock().await;

        let rtt = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
            .and_then(|(_, client)| client.take_keep_alive_rtt());

        if let Some(rtt) = rtt {
            histogram!(telemetry::KEEPALIVE_RTT_SECONDS).record(rtt as f64 / 1000.0);
        }
    }

    pub async fn add_connexion(
        &self,
        addr: SocketAddr,
        sender: Sender<Message<ServerMsg>>,
    ) -> Result<(), String> {
        let mut connexions = self.connections.lock().await;

        if connexions.contains_key(&addr) {
            return Err("Connexion already exists".to_string());
        }

        connexions.insert(addr, sender);

        Ok(())
    }

    pub async fn remove_connexion(&self, addr: SocketAddr) -> Result<(), String> {
        let mut connexions = self.connections.lock().await;

        if !connexions.contains_key(&addr) {
            return Err("Connexion not found".to_string());
        }

        connexions.remove(&addr);

        Ok(())
    }

    pub async fn add_new_client(&self, addrs: Vec<SocketAddr>) -> Result<(), String> {
        let ipv4 = addrs[0];

        let location = self.geoip_client.get_location(ipv4.ip()).await;

        if location.is_err() {
            counter!(telemetry::GEOIP_FAILURES_TOTAL).increment(1);
            return Err("Could not get location".to_string());
        }

        let client = Client::new(location.unwrap());

        if addrs.len() == 2 {
            let ipv6 = addrs[1];
            self.clients.lock().await.insert((ipv4, Some(ipv6)), client);
            return Ok(());
        }

        self.clients.lock().await.insert((ipv4, None), client);

        Ok(())
    }

    pub fn get_geoip_client(&self) -> GeoipClient {
        self.geoip_client.clone()
    }

    pub async fn get_lobbies(&self, _addr: SocketAddr) -> Vec<Lobby> {
        let mut lobbies: Vec<Lobby> = self
            .lobbies
            .lock()
            .await
            .iter()
            .filter(|l| l.players().iter().any(|p| p.is_host()))
            .filter(|l| match l.kind() {
                LobbyKind::Standard => !l.is_full() || l.in_game() || l.series_in_progress(),
                LobbyKind::QuickPlay => l.in_game(),
                LobbyKind::Private | LobbyKind::Ranked => false,
            })
            .cloned()
            .collect();

        lobbies.sort_by_key(|l| l.in_game());

        lobbies
    }

    pub async fn add_lobby(
        &self,
        lobby: &mut Lobby,
        addr: SocketAddr,
        local_players: usize,
    ) -> Result<Lobby, String> {
        if self.is_draining() {
            return Err(SERVER_UPDATED_MESSAGE.to_string());
        }

        if lobby.kind() == LobbyKind::Ranked {
            return Err("Ranked lobbies are automated".to_string());
        }

        if !is_valid_local_players(local_players) {
            return Err("Invalid local player count".to_string());
        }

        if local_players >= lobby.max_players() {
            return Err("No room left for another player".to_string());
        }

        let mut lobbies = self.lobbies.lock().await;

        if lobbies.iter().any(|l| l.room_id() == lobby.room_id()) {
            return Err("Lobby with that room id already exists".to_string());
        }

        let mut clients = self.clients.lock().await;
        let client = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
            .unwrap()
            .1;

        client.room = Some(Room::new(lobby.room_id().clone()));

        let lobb = lobby.clone();
        let host_res: Option<Player> = lobb.players().first().cloned();

        if host_res.is_none() {
            return Err("Host not found".to_string());
        }

        let mut host = host_res.unwrap().clone();
        client.name = host.name();

        host.update_addr(addr.to_string());
        host.update_room_peer_id(client.peer_id(lobby.room_id().clone()).unwrap());
        host.set_seat(0);
        let owner = host.clone();

        lobby.update_seed(lobby::new_seed());

        let join_code = match lobby.kind() {
            LobbyKind::Private => {
                let taken: HashSet<String> = lobbies.iter().filter_map(|l| l.join_code()).collect();

                Some(codes::generate_join_code(&taken))
            }
            _ => None,
        };

        let mut final_lobby = LobbyBuilder::new()
            .name(lobby.name())
            .room_id(lobby.room_id())
            .max_players(lobby.max_players())
            .game_data(lobby.game_data())
            .players(vec![host])
            .mods(lobby.mods())
            .kind(lobby.kind())
            .join_code(join_code)
            .is_report_enabled(self.reports.is_some())
            .build();

        final_lobby.add_guests(&owner, local_players);

        lobbies.push(final_lobby.clone());

        info!(
            "[lobby] {} created {:?} lobby {}{}",
            final_lobby.name(),
            final_lobby.kind(),
            final_lobby.room_id(),
            final_lobby
                .join_code()
                .map(|code| format!(" (code {code})"))
                .unwrap_or_default()
        );

        counter!(telemetry::LOBBIES_CREATED_TOTAL, "kind" => telemetry::kind_label(final_lobby.kind()))
            .increment(1);

        Ok(final_lobby)
    }

    pub async fn join_lobby(
        &self,
        room_id: String,
        addr: SocketAddr,
        name: String,
        is_player: bool,
        local_players: usize,
    ) -> Result<(Lobby, String), String> {
        if self.is_draining() {
            return Err(SERVER_UPDATED_MESSAGE.to_string());
        }

        let mut lobbies = self.lobbies.lock().await;

        Self::guard_not_in_lobby(&lobbies, addr)?;

        let lobby = lobbies
            .iter_mut()
            .find(|l| l.room_id() == room_id)
            .ok_or(format!("Lobby {} not found", room_id))?;

        if lobby.kind() == LobbyKind::Ranked {
            return Err("Ranked matches can't be joined".to_string());
        }

        let room_peer_id = self
            .seat_member(lobby, addr, name, is_player, local_players)
            .await?;

        Ok((lobby.clone(), room_peer_id))
    }

    pub async fn join_private(
        &self,
        code: String,
        addr: SocketAddr,
        name: String,
        is_player: bool,
        local_players: usize,
    ) -> Result<(Lobby, String), String> {
        if self.is_draining() {
            return Err(SERVER_UPDATED_MESSAGE.to_string());
        }

        {
            let mut clients = self.clients.lock().await;

            let client = clients
                .iter_mut()
                .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
                .ok_or("Client not found".to_string())?
                .1;

            if let Some(last) = client.last_private_join_attempt {
                if last.elapsed()
                    < Duration::from_secs(crate::constants::PRIVATE_JOIN_COOLDOWN_SECS)
                {
                    counter!(telemetry::PRIVATE_JOIN_FAILURES_TOTAL, "reason" => "rate_limited")
                        .increment(1);
                    return Err("Too many attempts, wait a moment".to_string());
                }
            }

            client.last_private_join_attempt = Some(std::time::Instant::now());
        }

        let code = codes::normalize_code(&code);

        let mut lobbies = self.lobbies.lock().await;

        Self::guard_not_in_lobby(&lobbies, addr)?;

        let lobby = lobbies
            .iter_mut()
            .find(|l| l.join_code().as_deref() == Some(code.as_str()))
            .ok_or_else(|| {
                counter!(telemetry::PRIVATE_JOIN_FAILURES_TOTAL, "reason" => "invalid_code")
                    .increment(1);

                "Invalid or expired code".to_string()
            })?;

        let room_peer_id = self
            .seat_member(lobby, addr, name, is_player, local_players)
            .await?;

        Ok((lobby.clone(), room_peer_id))
    }

    fn guard_not_in_lobby(lobbies: &[Lobby], addr: SocketAddr) -> Result<(), String> {
        let exist = lobbies
            .iter()
            .find(|l| l.players().iter().any(|p| p.addr() == addr.to_string()));

        if exist.is_some() {
            return Err("Player already in a lobby".to_string());
        }

        Ok(())
    }

    pub(super) async fn seat_member(
        &self,
        lobby: &mut Lobby,
        addr: SocketAddr,
        name: String,
        is_player: bool,
        local_players: usize,
    ) -> Result<String, String> {
        let room_id = lobby.room_id();

        if is_player {
            if !is_valid_local_players(local_players) {
                return Err("Invalid local player count".to_string());
            }

            if lobby.free_seats() < local_players {
                return Err(format!("Lobby {} is full", room_id));
            }

            if lobby.in_game() {
                return Err(format!("Lobby {} already started", room_id));
            }

            if lobby.series_in_progress() {
                return Err(format!("Lobby {} has a series running", room_id));
            }
        }

        let mut clients: tokio::sync::MutexGuard<'_, HashMap<IpV4V6, Client>> =
            self.clients.lock().await;

        let client_res = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr);

        if client_res.is_none() {
            return Err("Client not found".to_string());
        }

        let client = client_res.unwrap().1;

        client.room = Some(Room::new(room_id.clone()));
        client.name = name.clone();

        let room_peer_id = client
            .peer_id(room_id.clone())
            .ok_or("Couldn't retrieve Room peer id")?;

        let member = Player::new(
            client.name.clone(),
            addr.to_string(),
            room_peer_id.clone(),
            false,
        );

        info!(
            "[lobby] {} joined {} as {}",
            client.name,
            room_id,
            if is_player { "player" } else { "spectator" }
        );

        if is_player {
            lobby.add_player_device(member, local_players);
        } else {
            lobby.add_spectator(member);
        }

        counter!(telemetry::JOINS_TOTAL, "role" => if is_player { "player" } else { "spectator" })
            .increment(1);

        Ok(room_peer_id)
    }

    pub async fn remove_from_lobby(
        &self,
        addr: SocketAddr,
        departure: Departure,
    ) -> Result<Vec<String>, String> {
        let room_ids = self.remove_from_lobbies(addr, None, departure).await?;

        if room_ids.is_empty() {
            return Err("Lobby not found".to_string());
        }

        Ok(room_ids)
    }

    pub async fn leave_stale_lobbies(&self, addr: SocketAddr, keep_room_id: Option<&str>) {
        let room_ids = match self
            .remove_from_lobbies(addr, keep_room_id, Departure::Left)
            .await
        {
            Ok(room_ids) if !room_ids.is_empty() => room_ids,
            _ => return,
        };

        warn!(
            "{} was still seated in {:?}, removed",
            self.client_label(addr).await,
            room_ids
        );

        for room_id in room_ids {
            if let Err(err) = self.send_lobby_update(room_id).await {
                warn!("Could not send lobby update (lobby likely gone): {}", err);
            }
        }

        self.purge_lobby_async().await;
    }

    async fn remove_from_lobbies(
        &self,
        addr: SocketAddr,
        keep_room_id: Option<&str>,
        departure: Departure,
    ) -> Result<Vec<String>, String> {
        let mut lobbies = self.lobbies.lock().await;
        let mut clients = self.clients.lock().await;

        if !clients
            .keys()
            .any(|(ipv4, ipv6)| *ipv4 == addr || *ipv6 == Some(addr))
        {
            return Err("Client not found".to_string());
        }

        let member = addr.to_string();
        let mut room_ids = vec![];
        let mut departure_events = vec![];

        for lobby in lobbies
            .iter_mut()
            .filter(|l| l.has_member(&member) && Some(l.room_id().as_str()) != keep_room_id)
        {
            let series_running = lobby.series_in_progress();

            departure_events.extend(
                self.ranking
                    .settle_departure(lobby, &member, &clients, departure),
            );

            lobby.remove_player_or_spectator(member.clone());
            lobby.promote_host_if_needed();

            if series_running
                && lobby
                    .series()
                    .is_some_and(|s| s.status == SeriesStatus::Aborted)
            {
                info!("Series in room {} aborted, a player left", lobby.room_id());
                counter!(telemetry::SERIES_OUTCOMES_TOTAL, "outcome" => "aborted").increment(1);
            }

            room_ids.push(lobby.room_id());
        }

        let client: &mut Client = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || *ipv6 == Some(addr))
            .ok_or("Client not found".to_string())?
            .1;

        let is_room_left = client
            .room
            .as_ref()
            .is_some_and(|room| room_ids.contains(&room.id()));

        if keep_room_id.is_none() || is_room_left {
            client.room = None;
        }

        drop(clients);
        drop(lobbies);

        self.ranking
            .after_departure(self, addr, departure, departure_events)
            .await;

        Ok(room_ids)
    }

    pub async fn start_lobby(&self, addr: SocketAddr) -> Result<String, String> {
        if self.is_draining() {
            return Err(SERVER_UPDATED_MESSAGE.to_string());
        }

        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        if lobby.kind() == LobbyKind::Ranked {
            return Err("Ranked matches start automatically".to_string());
        }

        let is_host = lobby
            .players()
            .iter()
            .any(|p| p.addr() == addr.to_string() && p.is_host());

        if !is_host {
            return Err("Only the host can start the lobby".to_string());
        }

        if lobby.in_game() {
            return Err("Lobby already started".to_string());
        }

        if lobby.peer_count() < 2 {
            return Err("Waiting for another player".to_string());
        }

        if lobby.is_checking_connections() {
            return Err("Connections are still being checked".to_string());
        }

        let series_started = if lobby.is_series_lobby() {
            if lobby.series_in_progress() {
                return Err("Series already running".to_string());
            }

            lobby.start_series()?;
            lobby
                .begin_series_game()
                .ok_or("No tower available for the series".to_string())?;
            true
        } else {
            false
        };

        if lobby.end_game_choice().is_none() {
            lobby.initialize_end_game_choice();
        }

        lobby.set_in_game(true);
        info!("{}", started_line(lobby));

        counter!(telemetry::MATCHES_STARTED_TOTAL, "kind" => telemetry::kind_label(lobby.kind()), "trigger" => "manual")
            .increment(1);

        let lobby = lobby.clone();
        drop(lobbies);

        if series_started {
            counter!(telemetry::SERIES_OUTCOMES_TOTAL, "outcome" => "started").increment(1);

            self.send_msg_to_lobby(
                lobby.clone(),
                ServerMsg::LobbyUpdate {
                    lobby: lobby.clone(),
                },
            )
            .await;
        }

        self.send_msg_to_lobby(lobby.clone(), ServerMsg::StartLobby {})
            .await;

        Ok(lobby.room_id())
    }

    pub async fn rematch_lobby(&self, addr: SocketAddr) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        lobby.rematch(addr.to_string());

        Ok(lobby.room_id())
    }

    pub async fn archer_select_choice_lobby(&self, addr: SocketAddr) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        lobby.archer_select_choice(addr.to_string());

        Ok(lobby.room_id())
    }

    pub async fn room_id_for(&self, addr: SocketAddr) -> Option<String> {
        let lobbies = self.lobbies.lock().await;

        lobbies
            .iter()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .map(|lobby| lobby.room_id())
    }

    pub async fn purge_lobby_async(&self) {
        let mut lobbies = self.lobbies.lock().await;

        let count = lobbies.len();
        lobbies.retain(|l| !l.players().is_empty());
        if count != lobbies.len() {
            info!("Purged {} empty lobbies", count - lobbies.len());
        }

        let count = lobbies.len();
        lobbies.retain(|l| l.players().iter().any(|p| p.is_host()));

        if count != lobbies.len() {
            info!("Purged {} lobbies with no host", count - lobbies.len());
        }
    }

    pub async fn update_player(&self, addr: SocketAddr, player: Player) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let addr = addr.to_string();
        let lobby = lobbies
            .iter_mut()
            .find(|l| l.players().iter().any(|p| p.addr() == addr))
            .ok_or("Lobby not found".to_string())?;

        lobby.update_player(&addr, player);

        Ok(lobby.room_id())
    }

    pub async fn update_lobby_settings(
        &self,
        addr: SocketAddr,
        max_players: usize,
        game_data: GameData,
        mods: Vec<Mod>,
    ) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|l| l.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        if lobby.kind() == LobbyKind::Ranked {
            return Err("Ranked settings can't change".to_string());
        }

        if !lobby
            .players()
            .iter()
            .any(|p| p.addr() == addr.to_string() && p.is_host())
        {
            return Err("Only the host can update lobby settings".to_string());
        }

        if lobby.in_game() {
            return Err("Cannot update lobby settings while in game".to_string());
        }

        if lobby.series_in_progress() {
            return Err("Cannot update lobby settings during a series".to_string());
        }

        lobby.update_settings(max_players, game_data, mods);

        Ok(lobby.room_id())
    }

    pub async fn remove_all_empty_signaling_rooms(&self) {
        for signaling in [&self.signaling, &self.ping_measurement_signaling] {
            let mut signaling = signaling.lock().await;

            let mut rooms_to_remove = vec![];

            for (room, peers) in signaling.rooms.iter() {
                if peers.is_empty() {
                    rooms_to_remove.push(room.clone());
                }
            }

            for room in rooms_to_remove.clone() {
                signaling.rooms.remove(&room);
            }

            if rooms_to_remove.len() > 0 {
                info!("Removed {} empty signaling rooms", rooms_to_remove.len());
            }
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use std::net::SocketAddr;

    use crate::services::geoip::Location;

    use super::{
        client::Client,
        lobby::{GameData, Lobby, LobbyBuilder, LobbyKind, Player},
        AppState,
    };

    pub fn test_addr(n: u8) -> SocketAddr {
        format!("127.0.0.{}:4000", n).parse().unwrap()
    }

    pub async fn state_with_clients(addrs: &[SocketAddr]) -> AppState {
        let state = AppState::new();

        for addr in addrs {
            state
                .clients
                .lock()
                .await
                .insert((*addr, None), Client::new(Location::default()));
        }

        state
    }

    pub fn lobby_payload(kind: LobbyKind, room_id: &str) -> Lobby {
        LobbyBuilder::new()
            .name("tester".to_string())
            .room_id(room_id.to_string())
            .game_data(GameData {
                map_id: -1,
                mode: 3,
                match_length: 2,
                variants: vec![],
                seed: 0,
                best_of: 0,
            })
            .players(vec![Player::new(
                "tester".to_string(),
                "".to_string(),
                "".to_string(),
                true,
            )])
            .kind(kind)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn private_lobby_gets_code_and_hides_from_browser() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        let mut payload = lobby_payload(LobbyKind::Private, "room-1");
        let created = state.add_lobby(&mut payload, host, 1).await.unwrap();

        assert_eq!(created.kind(), LobbyKind::Private);
        assert_eq!(created.join_code().unwrap().len(), codes::CODE_LEN);
        assert!(state.get_lobbies(host).await.is_empty());
    }

    #[tokio::test]
    async fn join_private_normalizes_code_and_rejects_bad_one() {
        let host = test_addr(1);
        let joiner = test_addr(2);
        let state = state_with_clients(&[host, joiner]).await;

        let mut payload = lobby_payload(LobbyKind::Private, "room-1");
        let created = state.add_lobby(&mut payload, host, 1).await.unwrap();
        let code = created.join_code().unwrap();

        let err = state
            .join_private("NOPE".to_string(), joiner, "guest".to_string(), true, 1)
            .await
            .unwrap_err();
        assert_eq!(err, "Invalid or expired code");

        let throttled = state
            .join_private(code.clone(), joiner, "guest".to_string(), true, 1)
            .await
            .unwrap_err();
        assert_eq!(throttled, "Too many attempts, wait a moment");

        state
            .clients
            .lock()
            .await
            .iter_mut()
            .for_each(|(_, client)| client.last_private_join_attempt = None);

        let sloppy = format!(" {} ", code.to_lowercase());
        let (lobby, room_peer_id) = state
            .join_private(sloppy, joiner, "guest".to_string(), true, 1)
            .await
            .unwrap();

        assert!(!room_peer_id.is_empty());
        assert_eq!(lobby.players().len(), 2);
    }

    #[tokio::test]
    async fn in_game_lobby_rejects_players_but_accepts_spectators_and_stays_listed() {
        let host = test_addr(1);
        let joiner = test_addr(2);
        let watcher = test_addr(3);
        let qp_host = test_addr(4);
        let state = state_with_clients(&[host, joiner, watcher, qp_host]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        state.add_lobby(&mut payload, host, 1).await.unwrap();
        state.lobbies.lock().await[0].set_in_game(true);

        let err = state
            .join_lobby("room-1".to_string(), joiner, "guest".to_string(), true, 1)
            .await
            .unwrap_err();
        assert_eq!(err, "Lobby room-1 already started");

        let (_, room_peer_id) = state
            .join_lobby(
                "room-1".to_string(),
                watcher,
                "watcher".to_string(),
                false,
                1,
            )
            .await
            .unwrap();
        assert!(!room_peer_id.is_empty());

        let lobbies = state.get_lobbies(host).await;
        assert_eq!(lobbies.len(), 1);
        assert_eq!(lobbies[0].spectators().len(), 1);

        let mut qp_payload = lobby_payload(LobbyKind::QuickPlay, "room-qp");
        state.add_lobby(&mut qp_payload, qp_host, 1).await.unwrap();
        assert_eq!(state.get_lobbies(host).await.len(), 1);

        state
            .lobbies
            .lock()
            .await
            .iter_mut()
            .find(|l| l.room_id() == "room-qp")
            .unwrap()
            .set_in_game(true);
        assert_eq!(state.get_lobbies(host).await.len(), 2);
    }

    #[tokio::test]
    async fn match_players_keep_who_left_until_the_match_ends() {
        let host = test_addr(1);
        let joiner = test_addr(2);
        let state = state_with_clients(&[host, joiner]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        state.add_lobby(&mut payload, host, 1).await.unwrap();
        state
            .join_lobby("room-1".to_string(), joiner, "guest".to_string(), true, 1)
            .await
            .unwrap();

        let mut lobbies = state.lobbies.lock().await;
        let lobby = &mut lobbies[0];

        lobby.set_in_game(true);
        lobby.remove_player_or_spectator(joiner.to_string());

        assert_eq!(lobby.players().len(), 1);
        assert_eq!(lobby.match_players().len(), 2);

        lobby.set_in_game(false);
        assert!(lobby.match_players().is_empty());
    }

    #[tokio::test]
    async fn standard_lobby_stays_listed_without_code() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        let created = state.add_lobby(&mut payload, host, 1).await.unwrap();

        assert!(created.join_code().is_none());
        assert_eq!(state.get_lobbies(host).await.len(), 1);
    }

    #[tokio::test]
    async fn create_lobby_seats_guests_after_the_host() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        let created = state.add_lobby(&mut payload, host, 3).await.unwrap();

        let players = created.players();
        assert_eq!(players.len(), 3);
        assert_eq!(
            players.iter().map(|p| p.seat()).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(players[0].is_host() && !players[0].is_guest());
        assert!(players[1..].iter().all(|p| p.is_guest() && !p.is_host()));
        assert!(players
            .iter()
            .all(|p| p.room_peer_id() == players[0].room_peer_id()));
        assert_eq!(players[1].name(), "tester 2");
        assert_eq!(players[2].name(), "tester 3");
        assert_eq!(created.peer_count(), 1);
    }

    #[tokio::test]
    async fn create_lobby_keeps_room_for_another_device() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        let mut too_many = lobby_payload(LobbyKind::Standard, "room-1");
        assert!(state.add_lobby(&mut too_many, host, 4).await.is_err());

        let base = lobby_payload(LobbyKind::Standard, "room-2");
        let mut duel = LobbyBuilder::new()
            .name("tester".to_string())
            .room_id("room-2".to_string())
            .max_players(2)
            .game_data(base.game_data())
            .players(base.players())
            .build();

        assert!(state.add_lobby(&mut duel, host, 2).await.is_err());
        assert!(state.add_lobby(&mut duel, host, 1).await.is_ok());
    }

    #[tokio::test]
    async fn join_needs_a_free_seat_per_local_player() {
        let host = test_addr(1);
        let duo = test_addr(2);
        let late = test_addr(3);
        let state = state_with_clients(&[host, duo, late]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        state.add_lobby(&mut payload, host, 2).await.unwrap();

        let err = state
            .join_lobby("room-1".to_string(), duo, "duo".to_string(), true, 3)
            .await
            .unwrap_err();
        assert_eq!(err, "Lobby room-1 is full");

        let (lobby, _) = state
            .join_lobby("room-1".to_string(), duo, "duo".to_string(), true, 2)
            .await
            .unwrap();
        assert_eq!(lobby.players().len(), 4);
        assert_eq!(lobby.peer_count(), 2);

        let err = state
            .join_lobby("room-1".to_string(), late, "late".to_string(), true, 1)
            .await
            .unwrap_err();
        assert_eq!(err, "Lobby room-1 is full");

        let (lobby, _) = state
            .join_lobby("room-1".to_string(), late, "late".to_string(), false, 3)
            .await
            .unwrap();
        assert_eq!(lobby.spectators().len(), 1);
        assert_eq!(lobby.players().len(), 4);
    }

    #[tokio::test]
    async fn update_player_targets_the_sent_seat_and_keeps_guest_names() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        state.add_lobby(&mut payload, host, 2).await.unwrap();

        let update: Player = serde_json::from_value(serde_json::json!({
            "Name": "spoofed",
            "ArcherIndex": 3,
            "ArcherAltIndex": 0,
            "Ready": true,
            "RoomPeerId": "",
            "IsHost": true,
            "Seat": 1
        }))
        .unwrap();

        state.update_player(host, update).await.unwrap();

        let lobbies = state.lobbies.lock().await;
        let players = lobbies[0].players();
        assert!(!players[0].ready());
        assert!(players[1].ready());
        assert!(players[1].is_guest() && !players[1].is_host());
        assert_eq!(players[1].name(), "tester 2");
    }

    #[test]
    fn host_promotion_skips_guests() {
        let mut owner = Player::new(
            "owner".to_string(),
            "127.0.0.2:4000".to_string(),
            "peer-2".to_string(),
            false,
        );
        let mut guest = Player::guest_from(&owner, 1);
        guest.set_seat(0);
        owner.set_seat(1);

        let mut lobby = LobbyBuilder::new()
            .name("qp".to_string())
            .room_id("room-qp".to_string())
            .game_data(lobby_payload(LobbyKind::QuickPlay, "room-qp").game_data())
            .players(vec![guest, owner])
            .kind(LobbyKind::QuickPlay)
            .build();

        lobby.promote_host_if_needed();

        let host = lobby.players().into_iter().find(|p| p.is_host()).unwrap();
        assert!(!host.is_guest());
        assert_eq!(host.seat(), 1);
    }

    #[tokio::test]
    async fn start_needs_two_devices() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        state.add_lobby(&mut payload, host, 2).await.unwrap();

        let err = state.start_lobby(host).await.unwrap_err();
        assert_eq!(err, "Waiting for another player");
    }

    #[tokio::test]
    async fn match_report_completes_with_one_report_per_device() {
        let host = test_addr(1);
        let joiner = test_addr(2);
        let state = state_with_clients(&[host, joiner]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        state.add_lobby(&mut payload, host, 2).await.unwrap();
        state
            .join_lobby("room-1".to_string(), joiner, "joiner".to_string(), true, 1)
            .await
            .unwrap();
        state.lobbies.lock().await[0].set_in_game(true);

        state
            .handle_match_report(host, 0, None, None, None, None, None)
            .await;
        assert_eq!(state.match_reports.lock().await.len(), 1);

        state
            .handle_match_report(joiner, 0, None, None, None, None, None)
            .await;
        assert!(state.match_reports.lock().await.is_empty());
    }
}
