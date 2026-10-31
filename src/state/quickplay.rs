use std::{collections::HashMap, net::SocketAddr, time::Instant};

use metrics::{counter, histogram};
use tracing::info;

use crate::{
    constants::{MAX_PLAYERS, QUICK_PLAY_AUTO_START_GRACE_SECS, SERVER_UPDATED_MESSAGE},
    msg::ServerMsg,
    services::geoip::Location,
    state::lobby::{GameData, LobbyBuilder, LobbyKind},
    telemetry,
};

use super::{
    client::Room,
    lobby::{self, is_valid_local_players, Lobby, Player},
    queue::{QueueCounts, QueueEntry},
    AppState,
};

pub const WIDER_SET_MOD_NAME: &str = "Teuria.WiderSet";

#[derive(Debug, Clone)]
pub struct QuickplayQueued {
    pub addr: SocketAddr,
    pub name: String,
    pub location: Location,
    pub is_wide: bool,
    pub local_players: usize,
    pub queued_at: Instant,
}

impl QueueEntry for QuickplayQueued {
    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn is_wide(&self) -> bool {
        self.is_wide
    }
}

/// WiderSet changes level geometry, so wide and standard players can never match
fn lobby_is_wide(lobby: &Lobby) -> bool {
    lobby.mods().iter().any(|m| {
        m.name() == WIDER_SET_MOD_NAME
            && m.data()
                .get("IsWide")
                .map(|value| value.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
    })
}

#[derive(Debug)]
pub enum QuickPlayOutcome {
    Queued(QueueCounts),
    Seated {
        lobby: Lobby,
        room_peer_id: String,
    },
    Paired {
        lobby: Lobby,
        caller_peer_id: String,
        partner_addr: String,
        partner_peer_id: String,
    },
    Failed {
        message: String,
    },
}

pub fn should_auto_start(
    peer_count: usize,
    is_full: bool,
    secs_since_all_ready: u64,
    secs_since_membership_change: u64,
    secs_since_connection_switch: u64,
    grace_secs: u64,
) -> bool {
    peer_count >= 2
        && secs_since_connection_switch >= grace_secs
        && (is_full
            || (secs_since_all_ready >= grace_secs && secs_since_membership_change >= grace_secs))
}

impl AppState {
    pub async fn enter_quick_play(
        &self,
        addr: SocketAddr,
        name: String,
        is_wide: bool,
        local_players: usize,
    ) -> QuickPlayOutcome {
        if self.is_draining() {
            return QuickPlayOutcome::Failed {
                message: SERVER_UPDATED_MESSAGE.to_string(),
            };
        }

        if !is_valid_local_players(local_players) {
            return QuickPlayOutcome::Failed {
                message: "Invalid local player count".to_string(),
            };
        }

        let location = {
            let clients = self.clients.lock().await;

            match clients
                .iter()
                .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
            {
                Some((_, client)) => client.location.clone(),
                None => {
                    return QuickPlayOutcome::Failed {
                        message: "Client not found".to_string(),
                    }
                }
            }
        };

        let mut queue = self.queues.quick_play.lock().await;
        let mut lobbies = self.lobbies.lock().await;

        if lobbies.iter().any(|l| l.has_member(&addr.to_string())) {
            return QuickPlayOutcome::Failed {
                message: "Already in a lobby".to_string(),
            };
        }

        queue.retain(|q| q.addr != addr);

        if let Some(lobby) =
            Self::nearest_open_quick_play(&mut lobbies, &location, is_wide, local_players)
        {
            return match self
                .seat_member(lobby, addr, name, true, local_players)
                .await
            {
                Ok(room_peer_id) => QuickPlayOutcome::Seated {
                    lobby: lobby.clone(),
                    room_peer_id,
                },
                Err(message) => QuickPlayOutcome::Failed { message },
            };
        }

        let partner_index = queue
            .iter()
            .enumerate()
            .filter(|(_, q)| q.is_wide == is_wide && q.local_players + local_players <= MAX_PLAYERS)
            .min_by_key(|(_, q)| q.location.distance_to(location.clone()) as i64)
            .map(|(index, _)| index);

        if let Some(partner_index) = partner_index {
            let partner = queue.remove(partner_index);

            let mut clients = self.clients.lock().await;

            let room_id = uuid::Uuid::new_v4().to_string();

            let partner_client = clients
                .iter_mut()
                .find(|((ipv4, ipv6), _)| {
                    *ipv4 == partner.addr || ipv6.is_some() && ipv6.unwrap() == partner.addr
                })
                .map(|(_, client)| client);

            let Some(partner_client) = partner_client else {
                drop(clients);
                queue.push(QuickplayQueued {
                    addr,
                    name,
                    location,
                    is_wide,
                    local_players,
                    queued_at: Instant::now(),
                });

                return QuickPlayOutcome::Queued(QueueCounts::of(&queue));
            };

            partner_client.room = Some(Room::new(room_id.clone()));
            partner_client.name = partner.name.clone();
            let partner_peer_id = partner_client.peer_id(room_id.clone()).unwrap();

            let caller_client = clients
                .iter_mut()
                .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
                .map(|(_, client)| client);

            let Some(caller_client) = caller_client else {
                return QuickPlayOutcome::Failed {
                    message: "Client not found".to_string(),
                };
            };

            caller_client.room = Some(Room::new(room_id.clone()));
            caller_client.name = name.clone();
            let caller_peer_id = caller_client.peer_id(room_id.clone()).unwrap();

            drop(clients);

            let mut host = Player::new(
                partner.name.clone(),
                partner.addr.to_string(),
                partner_peer_id.clone(),
                true,
            );
            host.set_seat(0);
            let owner = host.clone();

            let mods = if is_wide {
                vec![lobby::Mod::new(
                    WIDER_SET_MOD_NAME.to_string(),
                    HashMap::from([("IsWide".to_string(), "True".to_string())]),
                )]
            } else {
                vec![]
            };

            let mut lobby = LobbyBuilder::new()
                .name("QUICK PLAY".to_string())
                .room_id(room_id)
                .game_data(GameData {
                    map_id: -1,
                    mode: 3,
                    match_length: 2,
                    variants: vec![],
                    seed: lobby::new_seed(),
                    best_of: 0,
                    custom_mode: String::new(),
                })
                .players(vec![host])
                .kind(LobbyKind::QuickPlay)
                .origin(Some(partner.location.clone()))
                .mods(mods)
                .is_report_enabled(self.reports.is_some())
                .build();

            lobby.add_guests(&owner, partner.local_players);
            lobby.add_player_device(
                Player::new(
                    name.clone(),
                    addr.to_string(),
                    caller_peer_id.clone(),
                    false,
                ),
                local_players,
            );

            lobbies.push(lobby.clone());

            info!(
                "[quickplay] matched {}",
                lobby
                    .players()
                    .iter()
                    .map(|p| p.name())
                    .collect::<Vec<_>>()
                    .join(" vs ")
            );

            histogram!(telemetry::QUICKPLAY_WAIT_SECONDS)
                .record(partner.queued_at.elapsed().as_secs_f64());

            return QuickPlayOutcome::Paired {
                lobby,
                caller_peer_id,
                partner_addr: partner.addr.to_string(),
                partner_peer_id,
            };
        }

        queue.push(QuickplayQueued {
            addr,
            name,
            location,
            is_wide,
            local_players,
            queued_at: Instant::now(),
        });

        QuickPlayOutcome::Queued(QueueCounts::of(&queue))
    }

    pub async fn exit_quick_play(&self, addr: SocketAddr) -> QueueCounts {
        let mut queue = self.queues.quick_play.lock().await;

        queue.retain(|q| q.addr != addr);

        QueueCounts::of(&queue)
    }

    fn nearest_open_quick_play<'a>(
        lobbies: &'a mut Vec<Lobby>,
        location: &Location,
        is_wide: bool,
        local_players: usize,
    ) -> Option<&'a mut Lobby> {
        lobbies
            .iter_mut()
            .filter(|l| {
                l.kind() == LobbyKind::QuickPlay
                    && !l.in_game()
                    && l.free_seats() >= local_players
                    && lobby_is_wide(l) == is_wide
            })
            .min_by_key(|l| {
                l.origin()
                    .map(|origin| origin.distance_to(location.clone()) as i64)
                    .unwrap_or(i64::MAX)
            })
    }

    pub async fn quick_play_tick(&self) {
        if self.is_draining() {
            return;
        }

        self.drain_queue_into_open_lobbies().await;
        self.auto_start_quick_play_lobbies().await;
        self.push_queue_status().await;
    }

    async fn drain_queue_into_open_lobbies(&self) {
        let mut seated: Vec<(String, Lobby, String)> = vec![];

        {
            let mut queue = self.queues.quick_play.lock().await;
            let mut lobbies = self.lobbies.lock().await;

            let mut remaining: Vec<QuickplayQueued> = vec![];
            let waiting: Vec<QuickplayQueued> = queue.drain(..).collect();

            for queued in waiting {
                let target = Self::nearest_open_quick_play(
                    &mut lobbies,
                    &queued.location,
                    queued.is_wide,
                    queued.local_players,
                );

                match target {
                    Some(lobby) => {
                        match self
                            .seat_member(
                                lobby,
                                queued.addr,
                                queued.name.clone(),
                                true,
                                queued.local_players,
                            )
                            .await
                        {
                            Ok(peer) => {
                                histogram!(telemetry::QUICKPLAY_WAIT_SECONDS)
                                    .record(queued.queued_at.elapsed().as_secs_f64());

                                seated.push((queued.addr.to_string(), lobby.clone(), peer))
                            }
                            Err(_) => remaining.push(queued),
                        }
                    }
                    None => remaining.push(queued),
                }
            }

            *queue = remaining;
        }

        for (addr, lobby, peer) in seated {
            info!(
                "Quickplay drain seated {} into {}",
                self.member_label(&addr).await,
                lobby.room_id()
            );

            self.send_msg_to_addrs(
                &[addr],
                ServerMsg::QuickPlayMatchFound {
                    lobby: lobby.clone(),
                    room_peer_id: peer,
                },
            )
            .await;

            let _ = self.send_lobby_update(lobby.room_id()).await;
        }
    }

    async fn auto_start_quick_play_lobbies(&self) {
        let mut started: Vec<Lobby> = vec![];

        {
            let mut lobbies = self.lobbies.lock().await;

            for lobby in lobbies.iter_mut() {
                if lobby.kind() != LobbyKind::QuickPlay
                    || lobby.in_game()
                    || lobby.is_checking_connections()
                {
                    continue;
                }

                let Some(secs_since_all_ready) = lobby.secs_since_all_ready() else {
                    continue;
                };

                if !should_auto_start(
                    lobby.peer_count(),
                    lobby.is_full(),
                    secs_since_all_ready,
                    lobby.secs_since_membership_change(),
                    lobby.secs_since_connection_switch(),
                    QUICK_PLAY_AUTO_START_GRACE_SECS,
                ) {
                    continue;
                }

                if lobby.end_game_choice().is_none() {
                    lobby.initialize_end_game_choice();
                }

                lobby.set_in_game(true);
                info!("{}", super::started_line(lobby));
                started.push(lobby.clone());
            }
        }

        for lobby in started {
            info!("Auto starting quickplay lobby {}", lobby.room_id());

            counter!(telemetry::MATCHES_STARTED_TOTAL, "kind" => telemetry::kind_label(lobby.kind()), "trigger" => "auto")
                .increment(1);

            self.send_msg_to_lobby(lobby, ServerMsg::StartLobby {})
                .await;
        }
    }

    async fn push_queue_status(&self) {
        let (addrs, counts) = {
            let queue = self.queues.quick_play.lock().await;

            (
                queue.iter().map(|q| q.addr.to_string()).collect::<Vec<_>>(),
                QueueCounts::of(&queue),
            )
        };

        if addrs.is_empty() {
            return;
        }

        self.send_msg_to_addrs(
            &addrs,
            ServerMsg::QuickPlayStatus {
                queued: true,
                searching_standard: counts.standard,
                searching_wide: counts.wide,
                message: "".to_string(),
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_support::*;

    #[test]
    fn auto_start_needs_two_players_and_full_or_both_graces() {
        assert!(!should_auto_start(1, false, 100, 100, u64::MAX, 5));
        assert!(!should_auto_start(2, false, 2, 100, u64::MAX, 5));
        assert!(!should_auto_start(2, false, 100, 2, u64::MAX, 5));
        assert!(should_auto_start(2, false, 5, 5, u64::MAX, 5));
        assert!(should_auto_start(4, true, 0, 0, u64::MAX, 5));
    }

    #[test]
    fn auto_start_waits_out_a_connection_switch_even_when_full() {
        assert!(!should_auto_start(2, false, 100, 100, 2, 5));
        assert!(!should_auto_start(4, true, 100, 100, 2, 5));
        assert!(should_auto_start(4, true, 100, 100, 5, 5));
    }

    #[tokio::test]
    async fn quick_play_pairs_then_fills_and_hides_from_browser() {
        let first = test_addr(1);
        let second = test_addr(2);
        let third = test_addr(3);
        let state = state_with_clients(&[first, second, third]).await;

        let queued = state
            .enter_quick_play(first, "first".to_string(), false, 1)
            .await;
        assert!(matches!(
            queued,
            QuickPlayOutcome::Queued(QueueCounts {
                standard: 1,
                wide: 0
            })
        ));

        let paired = state
            .enter_quick_play(second, "second".to_string(), false, 1)
            .await;
        let QuickPlayOutcome::Paired {
            lobby,
            caller_peer_id,
            partner_addr,
            partner_peer_id,
        } = paired
        else {
            panic!("expected a pair");
        };

        assert_eq!(partner_addr, first.to_string());
        assert_ne!(caller_peer_id, partner_peer_id);
        assert_eq!(lobby.players().len(), 2);
        assert_eq!(lobby.kind(), LobbyKind::QuickPlay);
        assert!(lobby
            .players()
            .iter()
            .any(|p| p.is_host() && p.addr() == first.to_string()));
        assert!(state.get_lobbies(first).await.is_empty());

        let seated = state
            .enter_quick_play(third, "third".to_string(), false, 1)
            .await;
        let QuickPlayOutcome::Seated { lobby, .. } = seated else {
            panic!("expected a seat in the open lobby");
        };

        assert_eq!(lobby.players().len(), 3);
    }

    #[tokio::test]
    async fn quick_play_host_migrates_on_leave() {
        let first = test_addr(1);
        let second = test_addr(2);
        let state = state_with_clients(&[first, second]).await;

        state
            .enter_quick_play(first, "first".to_string(), false, 1)
            .await;
        state
            .enter_quick_play(second, "second".to_string(), false, 1)
            .await;

        state
            .remove_from_lobby(first, crate::state::lobby::Departure::Left)
            .await
            .unwrap();

        let lobbies = state.lobbies.lock().await;
        let lobby = lobbies.first().unwrap();

        assert_eq!(lobby.players().len(), 1);
        assert!(lobby
            .players()
            .iter()
            .any(|p| p.is_host() && p.addr() == second.to_string()));
    }

    #[tokio::test]
    async fn quick_play_guards_double_entry_and_exit() {
        let first = test_addr(1);
        let second = test_addr(2);
        let state = state_with_clients(&[first, second]).await;

        state
            .enter_quick_play(first, "first".to_string(), false, 1)
            .await;
        let requeued = state
            .enter_quick_play(first, "first".to_string(), false, 1)
            .await;
        assert!(matches!(
            requeued,
            QuickPlayOutcome::Queued(QueueCounts {
                standard: 1,
                wide: 0
            })
        ));

        assert_eq!(state.exit_quick_play(first).await, QueueCounts::default());

        state
            .enter_quick_play(first, "first".to_string(), false, 1)
            .await;
        state
            .enter_quick_play(second, "second".to_string(), false, 1)
            .await;

        let in_lobby = state
            .enter_quick_play(first, "first".to_string(), false, 1)
            .await;
        assert!(matches!(in_lobby, QuickPlayOutcome::Failed { .. }));
    }

    #[tokio::test]
    async fn quick_play_never_mixes_wide_and_standard() {
        let first = test_addr(1);
        let second = test_addr(2);
        let third = test_addr(3);
        let state = state_with_clients(&[first, second, third]).await;

        let queued = state
            .enter_quick_play(first, "first".to_string(), true, 1)
            .await;
        assert!(matches!(
            queued,
            QuickPlayOutcome::Queued(QueueCounts {
                standard: 0,
                wide: 1
            })
        ));

        let still_queued = state
            .enter_quick_play(second, "second".to_string(), false, 1)
            .await;
        assert!(matches!(
            still_queued,
            QuickPlayOutcome::Queued(QueueCounts {
                standard: 1,
                wide: 1
            })
        ));

        let paired = state
            .enter_quick_play(third, "third".to_string(), true, 1)
            .await;
        let QuickPlayOutcome::Paired {
            lobby,
            partner_addr,
            ..
        } = paired
        else {
            panic!("expected the two wide players to pair");
        };

        assert_eq!(partner_addr, first.to_string());
        assert!(lobby_is_wide(&lobby));
        assert_eq!(state.exit_quick_play(second).await, QueueCounts::default());
    }

    #[tokio::test]
    async fn quick_play_auto_starts_after_grace() {
        let first = test_addr(1);
        let second = test_addr(2);
        let state = state_with_clients(&[first, second]).await;

        state
            .enter_quick_play(first, "first".to_string(), false, 1)
            .await;
        state
            .enter_quick_play(second, "second".to_string(), false, 1)
            .await;

        state.quick_play_tick().await;
        assert!(!state.lobbies.lock().await.first().unwrap().in_game());

        {
            let mut lobbies = state.lobbies.lock().await;
            let lobby = lobbies.first_mut().unwrap();
            lobby.set_all_ready();
            lobby.backdate_membership(QUICK_PLAY_AUTO_START_GRACE_SECS);
        }

        state.quick_play_tick().await;
        assert!(!state.lobbies.lock().await.first().unwrap().in_game());

        let second_peer = {
            let mut lobbies = state.lobbies.lock().await;
            let lobby = lobbies.first_mut().unwrap();
            lobby.backdate_ready(QUICK_PLAY_AUTO_START_GRACE_SECS);

            lobby
                .players()
                .iter()
                .find(|p| p.addr() == second.to_string())
                .unwrap()
                .room_peer_id()
        };

        state.quick_play_tick().await;
        assert!(!state.lobbies.lock().await.first().unwrap().in_game());

        state.record_peer_measured(first, second_peer).await;
        state.quick_play_tick().await;

        let lobbies = state.lobbies.lock().await;
        let lobby = lobbies.first().unwrap();

        assert!(lobby.in_game());
        assert!(lobby.end_game_choice().is_some());
    }

    #[tokio::test]
    async fn quick_play_pairs_only_devices_whose_seats_fit() {
        let trio = test_addr(1);
        let duo = test_addr(2);
        let solo = test_addr(3);
        let state = state_with_clients(&[trio, duo, solo]).await;

        state
            .enter_quick_play(trio, "trio".to_string(), false, 3)
            .await;

        let still_queued = state
            .enter_quick_play(duo, "duo".to_string(), false, 2)
            .await;
        assert!(matches!(
            still_queued,
            QuickPlayOutcome::Queued(QueueCounts {
                standard: 2,
                wide: 0
            })
        ));

        let paired = state
            .enter_quick_play(solo, "solo".to_string(), false, 1)
            .await;
        let QuickPlayOutcome::Paired {
            lobby,
            partner_addr,
            ..
        } = paired
        else {
            panic!("expected the solo player to pair with the trio");
        };

        assert_eq!(partner_addr, trio.to_string());
        assert_eq!(lobby.players().len(), 4);
        assert_eq!(lobby.peer_count(), 2);
        assert_eq!(lobby.players().iter().filter(|p| p.is_guest()).count(), 2);
    }
}
