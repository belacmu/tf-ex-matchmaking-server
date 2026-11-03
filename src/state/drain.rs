use std::{collections::HashSet, net::SocketAddr, sync::atomic::Ordering};

use axum_typed_websockets::Message;
use tracing::info;

use crate::{constants::SERVER_UPDATED_MESSAGE, msg::ServerMsg};

use super::AppState;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct DrainStatus {
    pub lobbies: usize,
    pub lobbies_in_game: usize,
    pub series_in_progress: usize,
    pub pending_reports: usize,
    pub relay_rooms: usize,
    pub connections: usize,
}

impl DrainStatus {
    pub fn is_drained(&self) -> bool {
        self.lobbies == 0 && self.pending_reports == 0
    }
}

impl AppState {
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }

    pub async fn start_drain(&self) {
        if self.draining.swap(true, Ordering::Relaxed) {
            return;
        }

        let queued: Vec<String> = self
            .queues
            .quick_play
            .take_all()
            .await
            .into_iter()
            .map(|queued| queued.addr.to_string())
            .collect();

        info!("Draining: {} queued player(s) sent away", queued.len());

        self.send_msg_to_addrs(
            &queued,
            ServerMsg::QuickPlayStatus {
                queued: false,
                searching_standard: 0,
                searching_wide: 0,
                searching_hz60: 0,
                searching_hz240: 0,
                message: SERVER_UPDATED_MESSAGE.to_string(),
            },
        )
        .await;

        self.ranking.on_drain_start(self).await;

        self.close_unseated_connections().await;
    }

    /// Kicks lobbies already idle on the previous tick
    pub async fn drain_tick(&self, idle_last_tick: &HashSet<String>) -> HashSet<String> {
        let idle = self.idle_lobby_ids().await;

        for room_id in idle
            .iter()
            .filter(|room_id| idle_last_tick.contains(*room_id))
        {
            self.kick_lobby(room_id).await;
        }

        self.close_unseated_connections().await;

        idle
    }

    pub async fn drain_status(&self) -> DrainStatus {
        let (lobbies, lobbies_in_game, series_in_progress) = {
            let lobbies = self.lobbies.lock().await;

            (
                lobbies.len(),
                lobbies.iter().filter(|l| l.in_game()).count(),
                lobbies.iter().filter(|l| l.series_in_progress()).count(),
            )
        };

        DrainStatus {
            lobbies,
            lobbies_in_game,
            series_in_progress,
            pending_reports: self.match_reports.lock().await.len(),
            relay_rooms: self.relay.as_ref().map_or(0, |relay| relay.stats().rooms),
            connections: self.connections.lock().await.len(),
        }
    }

    pub async fn notify_matches(&self, text: String) {
        let members: Vec<String> = self
            .lobbies
            .lock()
            .await
            .iter()
            .filter(|l| l.in_game() || l.series_in_progress())
            .flat_map(|l| l.players().into_iter().chain(l.spectators()))
            .map(|member| member.addr())
            .collect();

        if members.is_empty() {
            return;
        }

        info!("Notice to {} player(s) in a match: {}", members.len(), text);

        self.send_msg_to_addrs(&members, ServerMsg::Notice { text })
            .await;
    }

    async fn idle_lobby_ids(&self) -> HashSet<String> {
        self.lobbies
            .lock()
            .await
            .iter()
            .filter(|l| !l.in_game() && !l.series_in_progress())
            .map(|l| l.room_id())
            .collect()
    }

    async fn kick_lobby(&self, room_id: &str) {
        let lobby = self
            .lobbies
            .lock()
            .await
            .iter()
            .find(|l| l.room_id() == room_id)
            .cloned();

        let Some(lobby) = lobby else {
            return;
        };

        info!("Draining: lobby {} sent away", room_id);

        self.send_msg_to_lobby(
            lobby.clone(),
            ServerMsg::KickedFromLobby {
                reason: SERVER_UPDATED_MESSAGE.to_string(),
            },
        )
        .await;

        let members: Vec<SocketAddr> = self
            .connections
            .lock()
            .await
            .keys()
            .filter(|addr| lobby.has_member(&addr.to_string()))
            .copied()
            .collect();

        self.close_connections(&members).await;
    }

    async fn close_unseated_connections(&self) {
        let lobbies = self.lobbies.lock().await.clone();

        let unseated: Vec<SocketAddr> = self
            .connections
            .lock()
            .await
            .keys()
            .filter(|addr| {
                let addr = addr.to_string();
                !lobbies.iter().any(|l| l.has_member(&addr))
            })
            .copied()
            .collect();

        self.close_connections(&unseated).await;
    }

    async fn close_connections(&self, addrs: &[SocketAddr]) {
        let senders: Vec<_> = {
            let connections = self.connections.lock().await;

            addrs
                .iter()
                .filter_map(|addr| connections.get(addr).cloned())
                .collect()
        };

        for sender in senders {
            let _ = sender.send(Message::Close(None)).await;
        }
    }
}

pub fn match_ends_notice(secs: u64) -> String {
    format!(
        "Server update: this match ends in {} min",
        secs.div_ceil(60)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tokio::sync::mpsc::{self, Receiver};

    use super::*;
    use crate::state::{lobby::LobbyKind, test_support::*, QuickPlayOutcome};

    async fn connect(state: &AppState, addr: SocketAddr) -> Receiver<Message<ServerMsg>> {
        let (sender, receiver) = mpsc::channel(64);
        state.add_connexion(addr, sender).await.unwrap();
        receiver
    }

    fn received_kinds(receiver: &mut Receiver<Message<ServerMsg>>) -> Vec<&'static str> {
        let mut kinds = vec![];

        while let Ok(message) = receiver.try_recv() {
            match message {
                Message::Item(msg) => kinds.push(msg.kind()),
                Message::Close(_) => kinds.push("Close"),
                _ => {}
            }
        }

        kinds
    }

    async fn lobby_voting_rematch(state: &AppState, host: SocketAddr, joiner: SocketAddr) {
        state
            .add_lobby(&mut lobby_payload(LobbyKind::Standard, "room-1"), host, 1)
            .await
            .unwrap();
        state
            .join_lobby(
                "room-1".to_string(),
                joiner,
                "guest".to_string(),
                true,
                1,
                String::new(),
            )
            .await
            .unwrap();

        {
            let mut lobbies = state.lobbies.lock().await;
            lobbies[0].initialize_end_game_choice();
            lobbies[0].set_in_game(true);
        }

        state.rematch_lobby(host).await.unwrap();
        state.rematch_lobby(joiner).await.unwrap();
    }

    #[tokio::test]
    async fn draining_refuses_new_lobbies_matches_and_quickplay() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        state.start_drain().await;

        let refused = state
            .add_lobby(&mut lobby_payload(LobbyKind::Standard, "room-1"), host, 1)
            .await
            .unwrap_err();

        assert_eq!(refused, SERVER_UPDATED_MESSAGE);
        assert_eq!(
            state.start_lobby(host).await.unwrap_err(),
            SERVER_UPDATED_MESSAGE
        );
        assert!(matches!(
            state.enter_quick_play(host, "tester".to_string(), false, 1, String::new()).await,
            QuickPlayOutcome::Failed { message } if message == SERVER_UPDATED_MESSAGE
        ));
    }

    #[tokio::test]
    async fn drain_sends_the_queue_away_then_closes_it() {
        let player = test_addr(1);
        let state = state_with_clients(&[player]).await;
        let mut receiver = connect(&state, player).await;

        assert!(matches!(
            state
                .enter_quick_play(player, "tester".to_string(), false, 1, String::new())
                .await,
            QuickPlayOutcome::Queued(_)
        ));

        state.start_drain().await;

        assert!(matches!(
            receiver.try_recv(),
            Ok(Message::Item(ServerMsg::QuickPlayStatus { queued: false, message, .. }))
                if message == SERVER_UPDATED_MESSAGE
        ));
        assert!(matches!(receiver.try_recv(), Ok(Message::Close(None))));
    }

    #[tokio::test]
    async fn idle_lobby_is_kicked_and_running_match_is_kept() {
        let idle_host = test_addr(1);
        let playing_host = test_addr(2);
        let state = state_with_clients(&[idle_host, playing_host]).await;
        let mut idle_receiver = connect(&state, idle_host).await;
        let mut playing_receiver = connect(&state, playing_host).await;

        state
            .add_lobby(
                &mut lobby_payload(LobbyKind::Standard, "room-idle"),
                idle_host,
                1,
            )
            .await
            .unwrap();

        state
            .add_lobby(
                &mut lobby_payload(LobbyKind::Standard, "room-game"),
                playing_host,
                1,
            )
            .await
            .unwrap();

        state
            .lobbies
            .lock()
            .await
            .iter_mut()
            .find(|l| l.room_id() == "room-game")
            .unwrap()
            .set_in_game(true);

        state.start_drain().await;
        let first = state.drain_tick(&HashSet::new()).await;

        assert_eq!(first, HashSet::from(["room-idle".to_string()]));
        assert!(received_kinds(&mut idle_receiver).is_empty());

        state.drain_tick(&first).await;

        assert_eq!(
            received_kinds(&mut idle_receiver),
            vec!["KickedFromLobby", "Close"]
        );
        assert!(received_kinds(&mut playing_receiver).is_empty());
    }

    #[tokio::test]
    async fn series_between_games_is_kept() {
        let host = test_addr(1);
        let joiner = test_addr(2);
        let state = state_with_clients(&[host, joiner]).await;
        let mut receiver = connect(&state, host).await;

        state
            .add_lobby(&mut lobby_payload(LobbyKind::Standard, "room-1"), host, 1)
            .await
            .unwrap();
        state
            .join_lobby(
                "room-1".to_string(),
                joiner,
                "guest".to_string(),
                true,
                1,
                String::new(),
            )
            .await
            .unwrap();
        state.lobbies.lock().await[0].start_series().unwrap();

        state.start_drain().await;
        let first = state.drain_tick(&HashSet::new()).await;
        state.drain_tick(&first).await;

        assert!(first.is_empty());
        assert!(!received_kinds(&mut receiver).contains(&"KickedFromLobby"));
    }

    #[tokio::test]
    async fn rematch_vote_rematches_when_not_draining() {
        let host = test_addr(1);
        let joiner = test_addr(2);
        let state = state_with_clients(&[host, joiner]).await;
        let mut receiver = connect(&state, host).await;

        lobby_voting_rematch(&state, host, joiner).await;
        state.end_game_action("room-1".to_string()).await;

        assert!(received_kinds(&mut receiver).contains(&"RematchLobby"));
    }

    #[tokio::test]
    async fn draining_turns_a_rematch_vote_into_archer_select() {
        let host = test_addr(1);
        let joiner = test_addr(2);
        let state = state_with_clients(&[host, joiner]).await;
        let mut receiver = connect(&state, host).await;

        lobby_voting_rematch(&state, host, joiner).await;
        state.start_drain().await;
        state.end_game_action("room-1".to_string()).await;

        let kinds = received_kinds(&mut receiver);
        assert!(kinds.contains(&"ArcherSelectLobby"));
        assert!(!kinds.contains(&"RematchLobby"));
        assert!(!state.lobbies.lock().await[0].in_game());
    }

    #[tokio::test]
    async fn drain_status_is_drained_once_lobbies_are_gone() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        assert!(state.drain_status().await.is_drained());

        state
            .add_lobby(&mut lobby_payload(LobbyKind::Standard, "room-1"), host, 1)
            .await
            .unwrap();

        assert!(!state.drain_status().await.is_drained());
    }

    #[tokio::test]
    async fn notices_reach_only_running_matches() {
        let playing_host = test_addr(1);
        let idle_host = test_addr(2);
        let state = state_with_clients(&[playing_host, idle_host]).await;
        let mut playing_receiver = connect(&state, playing_host).await;
        let mut idle_receiver = connect(&state, idle_host).await;

        state
            .add_lobby(
                &mut lobby_payload(LobbyKind::Standard, "room-game"),
                playing_host,
                1,
            )
            .await
            .unwrap();
        state
            .add_lobby(
                &mut lobby_payload(LobbyKind::Standard, "room-idle"),
                idle_host,
                1,
            )
            .await
            .unwrap();
        state
            .lobbies
            .lock()
            .await
            .iter_mut()
            .find(|l| l.room_id() == "room-game")
            .unwrap()
            .set_in_game(true);

        state.notify_matches("hello".to_string()).await;

        assert_eq!(received_kinds(&mut playing_receiver), vec!["Notice"]);
        assert!(received_kinds(&mut idle_receiver).is_empty());
    }

    #[test]
    fn match_end_notice_rounds_up_to_minutes() {
        assert_eq!(
            match_ends_notice(2640),
            "Server update: this match ends in 44 min"
        );
        assert!(match_ends_notice(60).ends_with("in 1 min"));
    }
}
