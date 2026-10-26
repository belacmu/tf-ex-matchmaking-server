pub mod matchmaking;
pub mod relay;
pub mod signaling;

use std::collections::HashMap;

use axum::{
    http::{header::ORIGIN, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tracing::info;

use crate::{extensions::clients::ClientsExts, state::AppState};

/// Browser pages allowed to connect (the browser port of TowerFall): the site and local dev servers, plus
/// `ALLOWED_ORIGINS` (comma separated, `*` for any page)
const DEFAULT_ALLOWED_ORIGINS: [&str; 1] = ["https://belacmu.github.io"];

fn is_origin_allowed(origin: &str) -> bool {
    let is_local = ["http://localhost", "http://127.0.0.1"].iter().any(|host| {
        origin == *host
            || origin
                .strip_prefix(host)
                .and_then(|rest| rest.strip_prefix(':'))
                .is_some_and(|port| !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()))
    });

    let extra = std::env::var("ALLOWED_ORIGINS").unwrap_or_default();

    is_local
        || DEFAULT_ALLOWED_ORIGINS.contains(&origin)
        || extra
            .split(',')
            .map(str::trim)
            .any(|allowed| allowed == "*" || allowed == origin)
}

pub async fn refuse_browsers<B>(request: Request<B>, next: Next<B>) -> Response {
    if let Some(origin) = request.headers().get(ORIGIN) {
        if origin.to_str().is_ok_and(is_origin_allowed) {
            return next.run(request).await;
        }

        info!(
            "Refusing browser connection to {} from origin {:?}",
            request.uri().path(),
            origin
        );
        return StatusCode::FORBIDDEN.into_response();
    }

    next.run(request).await
}

pub async fn is_room_peer_known(
    state: &AppState,
    room_id: &str,
    params: &HashMap<String, String>,
) -> bool {
    let Some(peer) = params
        .get("peer")
        .and_then(|peer| uuid::Uuid::parse_str(peer).ok())
    else {
        return false;
    };

    state
        .clients
        .get_client_by_room_peer(room_id.to_string(), peer.to_string())
        .await
        .is_some()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::state::{lobby::LobbyKind, test_support::*};

    fn peer_params(peer: &str) -> HashMap<String, String> {
        HashMap::from([("peer".to_string(), peer.to_string())])
    }

    #[tokio::test]
    async fn only_a_member_peer_of_the_room_is_known() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        state
            .add_lobby(&mut lobby_payload(LobbyKind::Standard, "room-1"), host, 1)
            .await
            .unwrap();

        let peer = state
            .clients
            .get_client(host)
            .await
            .unwrap()
            .peer_id("room-1".to_string())
            .unwrap();

        assert!(is_room_peer_known(&state, "room-1", &peer_params(&peer)).await);
        assert!(!is_room_peer_known(&state, "room-2", &peer_params(&peer)).await);
        assert!(
            !is_room_peer_known(
                &state,
                "room-1",
                &peer_params(&uuid::Uuid::new_v4().to_string())
            )
            .await
        );
        assert!(!is_room_peer_known(&state, "room-1", &HashMap::new()).await);
    }
}
