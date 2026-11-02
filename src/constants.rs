pub const SECONDS_PER_DAY: i64 = 86_400;

pub static PURGE_JOB_INTERVAL: u64 = 10;
pub static KEEP_ALIVE_INTERVAL: u64 = 3;

pub static KEEP_ALIVE_MAX_MISSED: u8 = 6;

pub const MAX_PLAYERS: usize = 4;

pub const MAX_LOCAL_PLAYERS: usize = 3;

/// How long players get to pick an end match option before the undecided ones are dropped
pub static END_GAME_CHOICE_SECONDS: u64 = 30;
pub static RANKED_END_GAME_CHOICE_SECONDS: u64 = 10;

pub static PRIVATE_JOIN_COOLDOWN_SECS: u64 = 3;

pub static QUICK_PLAY_TICK_INTERVAL: u64 = 1;

/// Once a non full quickplay lobby is all ready, wait this long for extra players
pub static QUICK_PLAY_AUTO_START_GRACE_SECS: u64 = 5;

pub static SERIES_READY_SECONDS: u64 = 60;

pub static CONNECTIVITY_TICK_INTERVAL: u64 = 1;

/// How long a pair of players or a player and the relay gets to connect before it counts as failed
pub static CONNECTION_CHECK_SECS: u64 = 10;

/// Spectators per lobby allowed to watch through the relay
pub const MAX_RELAYED_SPECTATORS: usize = 2;

/// Vanilla versus towers eligible in a series
pub const SERIES_TOWER_COUNT: i32 = 16;

/// `TowerFall.Modes.TeamDeathmatch`
pub const TEAM_DEATHMATCH_MODE: i32 = 5;

/// `TowerFall.Modes.LastManStanding`
pub const LAST_MAN_STANDING_MODE: i32 = 3;

pub const SERIES_TEAM_SIZE: usize = 2;

pub const REPORTS_RETENTION_SECS: u64 = 3 * SECONDS_PER_DAY as u64;

pub const DEFAULT_REPORTS_MAX_MB: u64 = 750;

pub const MAX_REPORT_BYTES: usize = 32 * 1024 * 1024;

pub static REPORTS_PURGE_INTERVAL: u64 = 3600;

pub const DEFAULT_RELAY_PORT: u16 = 4537;

pub const DEFAULT_RELAY_PORT_CHECK_HOST: &str = "host.docker.internal";

pub const RELAY_PORT_CHECK_TIMEOUT_MS: u64 = 3000;

pub const RELAY_PORT_CHECK_RESEND_MS: u64 = 500;

pub static DRAIN_TICK_SECS: u64 = 5;

pub static DRAIN_STATUS_LOG_SECS: u64 = 30;

pub const DEFAULT_DRAIN_DEADLINE_SECS: u64 = 2640;

pub const SERVER_UPDATED_MESSAGE: &str = "Server updated, try again";

pub const DRAIN_REMINDER_SECS: [u64; 2] = [300, 60];

pub const LEADERBOARD_COOLDOWN_SECS: u64 = 2;

pub const PROFILE_COOLDOWN_MS: u64 = 250;
