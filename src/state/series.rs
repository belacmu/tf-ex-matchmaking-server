use std::net::SocketAddr;

use metrics::counter;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    constants::{SERIES_READY_SECONDS, SERIES_TOWER_COUNT},
    msg::ServerMsg,
    telemetry,
};

use super::{
    lobby::{Lobby, LobbyKind},
    AppState,
};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SeriesStatus {
    InProgress,
    Finished,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SeriesGame {
    pub map_id: i32,
    pub winner_side: usize,
    pub scores: Vec<i32>,
    #[serde(skip)]
    pub duration_secs: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Series {
    pub best_of: u32,
    /// Seats grouped by side (a 1v1 is `[[0], [1]]`)
    pub sides: Vec<Vec<usize>>,
    pub games: Vec<SeriesGame>,
    pub status: SeriesStatus,
    pub winner_side: Option<usize>,
    pub picker_side: Option<usize>,
    pub next_map_id: Option<i32>,
    pub abort_reason: Option<String>,
    pub awaiting_result: bool,
    #[serde(default)]
    pub picks: Vec<SeriesPick>,
    #[serde(default)]
    pub forfeit_side: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SeriesPick {
    pub seat: usize,
    pub map_id: i32,
}

impl Series {
    pub fn new(best_of: u32, sides: Vec<Vec<usize>>) -> Self {
        let mut series = Self {
            best_of,
            sides,
            games: vec![],
            status: SeriesStatus::InProgress,
            winner_side: None,
            picker_side: None,
            next_map_id: None,
            abort_reason: None,
            awaiting_result: false,
            picks: vec![],
            forfeit_side: None,
        };

        series.next_map_id = series.random_unplayed_map();

        series
    }

    pub fn wins_needed(&self) -> u32 {
        self.best_of / 2 + 1
    }

    pub fn wins(&self, side: usize) -> u32 {
        self.games.iter().filter(|g| g.winner_side == side).count() as u32
    }

    pub fn side_of_seat(&self, seat: usize) -> Option<usize> {
        self.sides.iter().position(|side| side.contains(&seat))
    }

    pub fn played_map_ids(&self) -> Vec<i32> {
        self.games.iter().map(|g| g.map_id).collect()
    }

    pub fn is_map_available(&self, map_id: i32) -> bool {
        (0..SERIES_TOWER_COUNT).contains(&map_id) && !self.played_map_ids().contains(&map_id)
    }

    pub fn random_unplayed_map(&self) -> Option<i32> {
        (0..SERIES_TOWER_COUNT)
            .filter(|id| self.is_map_available(*id))
            .choose(&mut rand::thread_rng())
    }

    pub fn begin_game(&mut self) -> Option<i32> {
        let map_id = self.next_map_id.take()?;
        self.awaiting_result = true;
        self.picks.clear();
        Some(map_id)
    }

    pub fn record_game(
        &mut self,
        map_id: i32,
        winner_seat: usize,
        scores: Vec<i32>,
        duration_secs: Option<f64>,
    ) -> Result<(), String> {
        if self.status != SeriesStatus::InProgress {
            return Err("Series is not in progress".to_string());
        }

        let winner_side = self
            .side_of_seat(winner_seat)
            .ok_or(format!("Seat {} is not part of the series", winner_seat))?;

        self.games.push(SeriesGame {
            map_id,
            winner_side,
            scores,
            duration_secs,
        });
        self.awaiting_result = false;

        self.next_map_id = None;
        self.picks.clear();

        if self.wins(winner_side) >= self.wins_needed() {
            self.status = SeriesStatus::Finished;
            self.winner_side = Some(winner_side);
            self.picker_side = None;
        } else {
            self.picker_side = (0..self.sides.len()).find(|side| *side != winner_side);
        }

        Ok(())
    }

    pub fn pick_map(&mut self, seat: usize, map_id: i32) -> Result<(), String> {
        if self.status != SeriesStatus::InProgress {
            return Err("Series is not in progress".to_string());
        }

        if self.awaiting_result {
            return Err("A game is still running".to_string());
        }

        if self.side_of_seat(seat) != self.picker_side {
            return Err("Not your pick".to_string());
        }

        if !self.is_map_available(map_id) {
            return Err(format!("Tower {} already played or unknown", map_id));
        }

        match self.picks.iter_mut().find(|pick| pick.seat == seat) {
            Some(pick) => pick.map_id = map_id,
            None => self.picks.push(SeriesPick { seat, map_id }),
        }

        self.resolve_picks();

        Ok(())
    }

    /// Every seat of the picker side has picked, either their tower if they agree or a draw between their picks otherwise
    fn resolve_picks(&mut self) {
        let Some(side) = self.picker_side else {
            return;
        };

        if self.picks.len() < self.sides[side].len() {
            self.next_map_id = None;
            return;
        }

        self.next_map_id = self.draw_among_picks();
    }

    fn draw_among_picks(&self) -> Option<i32> {
        let mut choices: Vec<i32> = self.picks.iter().map(|pick| pick.map_id).collect();
        choices.sort_unstable();
        choices.dedup();

        choices.into_iter().choose(&mut rand::thread_rng())
    }

    pub fn fill_missing_pick(&mut self) -> bool {
        if self.status != SeriesStatus::InProgress || self.next_map_id.is_some() {
            return false;
        }

        self.next_map_id = self
            .draw_among_picks()
            .or_else(|| self.random_unplayed_map());
        self.next_map_id.is_some()
    }

    pub fn abort(&mut self, reason: &str) {
        if self.status != SeriesStatus::InProgress {
            return;
        }

        self.status = SeriesStatus::Aborted;
        self.abort_reason = Some(reason.to_string());
        self.picker_side = None;
        self.next_map_id = None;
        self.picks.clear();
        self.awaiting_result = false;
    }

    pub fn forfeit(&mut self, loser_side: usize) {
        if self.status != SeriesStatus::InProgress {
            return;
        }

        self.status = SeriesStatus::Finished;
        self.winner_side = (0..self.sides.len()).find(|side| *side != loser_side);
        self.forfeit_side = Some(loser_side);
        self.picker_side = None;
        self.next_map_id = None;
        self.picks.clear();
        self.awaiting_result = false;
    }

    pub fn has_started(&self) -> bool {
        !self.games.is_empty() || self.awaiting_result
    }

    pub fn is_in_progress(&self) -> bool {
        self.status == SeriesStatus::InProgress
    }

    pub fn is_finished(&self) -> bool {
        self.status == SeriesStatus::Finished
    }

    pub fn can_start_game(&self) -> bool {
        self.is_in_progress() && !self.awaiting_result && self.next_map_id.is_some()
    }
}

impl AppState {
    pub(super) async fn continue_series(&self, lobby: Lobby) {
        let Some(series) = lobby.series() else {
            return;
        };

        match series.status {
            SeriesStatus::Aborted => {
                info!(
                    "Series in room {} aborted ({}), back to rollcall",
                    lobby.room_id(),
                    series.abort_reason.clone().unwrap_or_default()
                );

                let msg = if lobby.kind() == LobbyKind::Ranked {
                    ServerMsg::SeriesLobby {}
                } else {
                    ServerMsg::ArcherSelectLobby {}
                };

                self.send_msg_to_lobby(lobby, msg).await;
            }
            SeriesStatus::Finished => {
                self.send_msg_to_lobby(lobby, ServerMsg::SeriesLobby {})
                    .await;
            }
            SeriesStatus::InProgress => {
                let room_id = lobby.room_id();

                self.send_msg_to_lobby(lobby, ServerMsg::SeriesLobby {})
                    .await;
                self.start_series_countdown(room_id).await;
            }
        }
    }

    pub async fn series_continue_choice(&self, addr: SocketAddr) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        lobby.continue_choice(addr.to_string());

        Ok(lobby.room_id())
    }

    pub async fn pick_series_map(&self, addr: SocketAddr, map_id: i32) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        let seats = lobby.seats_from(&addr.to_string());

        if seats.is_empty() {
            return Err("Not seated".to_string());
        }

        let series = lobby
            .series_mut()
            .ok_or("No series in this lobby".to_string())?;

        let picking: Vec<usize> = seats
            .into_iter()
            .filter(|seat| series.side_of_seat(*seat) == series.picker_side)
            .collect();

        if picking.is_empty() {
            return Err("Not your pick".to_string());
        }

        for seat in picking {
            series.pick_map(seat, map_id)?;
        }

        Ok(lobby.room_id())
    }

    pub async fn try_start_series_game(&self, room_id: String) {
        if self.ranking.is_series_start_held(self, &room_id).await {
            return;
        }

        let started = {
            let mut lobbies = self.lobbies.lock().await;

            let Some(lobby) = lobbies.iter_mut().find(|l| l.room_id() == room_id) else {
                return;
            };

            if lobby.in_game()
                || !lobby.all_players_ready()
                || lobby.is_checking_connections()
                || lobby.peer_count() < 2
            {
                return;
            }

            let Some(series) = lobby.series() else {
                return;
            };

            match series.status {
                SeriesStatus::InProgress => {
                    if !series.can_start_game() {
                        return;
                    }
                }
                SeriesStatus::Finished => {
                    if lobby.kind() == LobbyKind::Ranked {
                        return;
                    }

                    if self.is_draining() {
                        return;
                    }

                    if lobby.start_series().is_err() {
                        return;
                    }

                    counter!(telemetry::SERIES_OUTCOMES_TOTAL, "outcome" => "started").increment(1);
                }
                SeriesStatus::Aborted => return,
            }

            if lobby.begin_series_game().is_none() {
                return;
            }

            if lobby.end_game_choice().is_none() {
                lobby.initialize_end_game_choice();
            }

            lobby.set_in_game(true);
            tracing::info!("{}", super::started_line(lobby));
            lobby.clone()
        };

        self.countdowns.series.cancel(&room_id).await;

        info!(
            "Series game {} starting in room {} on tower {}",
            started.series().map(|s| s.games.len() + 1).unwrap_or(0),
            room_id,
            started.game_data().map_id
        );

        counter!(telemetry::MATCHES_STARTED_TOTAL, "kind" => telemetry::kind_label(started.kind()), "trigger" => "series")
            .increment(1);

        self.send_msg_to_lobby(
            started.clone(),
            ServerMsg::LobbyUpdate {
                lobby: started.clone(),
            },
        )
        .await;
        self.send_msg_to_lobby(started, ServerMsg::StartLobby {})
            .await;
    }

    pub async fn start_series_countdown(&self, room_id: String) {
        let generation = self.countdowns.series.start(&room_id).await;

        info!(
            "Series ready window open for room {} ({}s)",
            room_id, SERIES_READY_SECONDS
        );

        let state = self.clone();

        tokio::spawn(async move {
            if state
                .countdowns
                .series
                .wait(&room_id, generation, SERIES_READY_SECONDS)
                .await
            {
                state.expire_series_wait(room_id).await;
            }
        });
    }

    async fn expire_series_wait(&self, room_id: String) {
        if !self.countdowns.series.cancel(&room_id).await {
            return;
        }

        let forced = {
            let mut lobbies = self.lobbies.lock().await;

            let Some(lobby) = lobbies.iter_mut().find(|l| l.room_id() == room_id) else {
                return;
            };

            if lobby.in_game() || !lobby.series_in_progress() {
                return;
            }

            if let Some(series) = lobby.series_mut() {
                series.fill_missing_pick();
            }

            lobby.ready_all()
        };

        if forced > 0 {
            info!(
                "Series ready window for room {} : {} player(s) readied",
                room_id, forced
            );

            counter!(telemetry::SERIES_OUTCOMES_TOTAL, "outcome" => "ready_timeout")
                .increment(forced as u64);
        }

        let _ = self.send_lobby_update(room_id.clone()).await;
        self.try_start_series_game(room_id).await;
    }
}
