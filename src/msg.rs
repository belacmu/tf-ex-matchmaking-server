use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::state::lobby::{GameData, Lobby, Mod, Player};

#[derive(Debug, Serialize, Clone)]
pub enum ServerMsg {
    KeepAlive {},
    GetLobbiesResponse {
        lobbies: Vec<Lobby>,
    },
    Leaderboard {
        entries: Vec<LeaderboardEntry>,
        own: Option<LeaderboardEntry>,
        around: Vec<LeaderboardEntry>,
        total: u32,
    },
    PlayerProfile {
        rank: Option<u32>,
        profile: Option<PlayerProfile>,
    },
    CreateLobbyResponse {
        success: bool,
        message: String,
        lobby: Option<Lobby>,
    },
    JoinLobbyResponse {
        success: bool,
        message: String,
        room_peer_id: String,
        lobby: Option<Lobby>,
    },
    LobbyUpdate {
        lobby: Lobby,
    },
    LeaveLobbyResponse {
        success: bool,
        message: String,
    },
    RematchLobby {},
    StartLobby {},
    ArcherSelectLobby {},
    SeriesLobby {},
    PrivateJoinResult {
        success: bool,
        message: String,
        room_peer_id: String,
        lobby: Option<Lobby>,
    },
    QuickPlayStatus {
        queued: bool,
        searching_standard: u32,
        searching_wide: u32,
        searching_hz60: u32,
        searching_hz240: u32,
        message: String,
    },
    QuickPlayMatchFound {
        lobby: Lobby,
        room_peer_id: String,
    },
    RankedStatus {
        queued: bool,
        searching_standard: u32,
        searching_wide: u32,
        message: String,
    },
    RankedMatchFound {
        lobby: Lobby,
        room_peer_id: String,
    },
    RankedReady {},
    SpectatorJoined {
        room_peer_id: String,
    },
    SkinChunk {
        chunk: SkinChunk,
        from: String,
    },
    KickedFromLobby {
        reason: String,
    },
    Notice {
        text: String,
    },
    UpdateRequired {
        version: String,
    },
}

impl ServerMsg {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::KeepAlive {} => "KeepAlive",
            Self::GetLobbiesResponse { .. } => "GetLobbiesResponse",
            Self::Leaderboard { .. } => "Leaderboard",
            Self::PlayerProfile { .. } => "PlayerProfile",
            Self::CreateLobbyResponse { .. } => "CreateLobbyResponse",
            Self::JoinLobbyResponse { .. } => "JoinLobbyResponse",
            Self::LobbyUpdate { .. } => "LobbyUpdate",
            Self::LeaveLobbyResponse { .. } => "LeaveLobbyResponse",
            Self::RematchLobby {} => "RematchLobby",
            Self::StartLobby {} => "StartLobby",
            Self::ArcherSelectLobby {} => "ArcherSelectLobby",
            Self::SeriesLobby {} => "SeriesLobby",
            Self::PrivateJoinResult { .. } => "PrivateJoinResult",
            Self::QuickPlayStatus { .. } => "QuickPlayStatus",
            Self::QuickPlayMatchFound { .. } => "QuickPlayMatchFound",
            Self::RankedStatus { .. } => "RankedStatus",
            Self::RankedMatchFound { .. } => "RankedMatchFound",
            Self::RankedReady {} => "RankedReady",
            Self::SpectatorJoined { .. } => "SpectatorJoined",
            Self::SkinChunk { .. } => "SkinChunk",
            Self::KickedFromLobby { .. } => "KickedFromLobby",
            Self::Notice { .. } => "Notice",
            Self::UpdateRequired { .. } => "UpdateRequired",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkinChunk {
    pub bundle_id: String,
    pub custom_archer_id: String,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub data: String,
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    KeepAlive {},
    Identify {
        player_id: String,
        name: String,
    },
    GetLobbies {},
    GetLeaderboard {},
    GetPlayerProfile {
        #[serde(default)]
        rank: Option<u32>,
    },
    CreateLobby {
        lobby: Lobby,
        #[serde(default = "default_local_player")]
        local_players: usize,
    },
    JoinLobby {
        room_id: String,
        name: String,
        is_player: bool,
        #[serde(default = "default_local_player")]
        local_players: usize,
        #[serde(default)]
        game_logic_speed: String,
    },
    UpdatePlayer {
        player: Player,
    },
    UpdateLobbySettings {
        max_players: usize,
        game_data: GameData,
        mods: Vec<Mod>,
    },
    LeaveLobby {
        #[serde(default)]
        link_lost: bool,
    },
    RematchLobbyChoice {},
    StartLobbyChoice {},
    ArcherSelectChoice {},
    SeriesContinueChoice {},
    SeriesPickMap {
        map_id: i32,
    },
    MatchEnded {
        #[serde(default)]
        winner_seat: Option<i32>,
        #[serde(default)]
        frame: Option<i32>,
        #[serde(default)]
        checksum: Option<String>,
        #[serde(default)]
        scores: Option<Vec<i32>>,
        #[serde(default)]
        stats: Option<Vec<SeatStats>>,
        #[serde(default)]
        tower: Option<i32>,
    },
    JoinPrivate {
        code: String,
        name: String,
        is_player: bool,
        #[serde(default = "default_local_player")]
        local_players: usize,
        #[serde(default)]
        game_logic_speed: String,
    },
    EnterQuickPlay {
        name: String,
        #[serde(default)]
        is_wide: bool,
        #[serde(default = "default_local_player")]
        local_players: usize,
        #[serde(default)]
        game_logic_speed: String,
    },
    ExitQuickPlay {},
    EnterRanked {
        name: String,
        #[serde(default)]
        is_wide: bool,
    },
    ExitRanked {},
    SkinChunk {
        chunk: SkinChunk,
    },
    PeerMeasured {
        room_peer_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeatStats {
    pub seat: i32,
    pub archer: i32,
    pub archer_alt: i32,
    #[serde(default)]
    pub custom_archer_id: String,
    #[serde(default)]
    pub awards: Vec<i32>,
    #[serde(default)]
    pub raw_stats: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LeaderboardEntry {
    pub rank: u32,
    pub name: String,
    pub rating: i32,
    pub matches: i64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct LeaderboardView {
    pub entries: Vec<LeaderboardEntry>,
    pub own: Option<LeaderboardEntry>,
    pub around: Vec<LeaderboardEntry>,
    pub total: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlayerProfile {
    pub rank: Option<u32>,
    pub rating: Option<i32>,
    pub series: Option<i64>,
    #[serde(flatten)]
    pub history: PlayerHistory,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatTotal {
    pub total: f64,
    pub best: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ArcherPlays {
    pub archer: i32,
    pub archer_alt: i32,
    pub custom_archer_id: String,
    pub games: i64,
    pub wins: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TowerPlays {
    pub map_id: i32,
    pub games: i64,
    pub wins: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AwardCount {
    pub award: i32,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MonthSummary {
    pub month: i32,
    pub games: i64,
    pub wins: i64,
    pub kills: i64,
    pub deaths: i64,
    pub seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlayerHistory {
    pub name: String,
    pub games: i64,
    pub wins: i64,
    pub kills: i64,
    pub deaths: i64,
    pub seconds: f64,
    pub months: Vec<MonthSummary>,
    pub ranked_months: Vec<MonthSummary>,
    pub casual_months: Vec<MonthSummary>,
    pub archers: Vec<ArcherPlays>,
    pub best_towers: Vec<TowerPlays>,
    pub awards: Vec<AwardCount>,
    pub stats: BTreeMap<String, StatTotal>,
}

fn default_local_player() -> usize {
    1
}
