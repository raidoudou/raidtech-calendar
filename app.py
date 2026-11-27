import json
import logging
import os
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta, timezone

import requests
from flask import Flask, jsonify, render_template

app = Flask(__name__)
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = app.logger

SEERR_URL = os.environ.get("SEERR_URL", "").rstrip("/")
SEERR_API_KEY = os.environ.get("SEERR_API_KEY", "")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "").strip()
TMDB_BASE_URL = "https://api.themoviedb.org/3"
JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "").rstrip("/")
JELLYFIN_API_KEY = os.environ.get("JELLYFIN_API_KEY", "")
SYNC_SECONDS = max(300, int(os.environ.get("SYNC_SECONDS", "3600")))
MEDIA_CACHE_HOURS = max(1, int(os.environ.get("MEDIA_CACHE_HOURS", "6")))
SEASON_CACHE_HOURS = max(1, int(os.environ.get("SEASON_CACHE_HOURS", "24")))
DB_PATH = os.environ.get("DB_PATH", "/app/data/raidtech-calendar.sqlite3")
TMDB_IMAGE = "https://image.tmdb.org/t/p/w342"

CACHE = {"events": [], "last_sync": None, "error": None}
LOCK = threading.Lock()
SYNC_LOCK = threading.Lock()


def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with db_connect() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS media_cache (
                media_type TEXT NOT NULL, tmdb_id TEXT NOT NULL,
                details_json TEXT NOT NULL, fetched_at TEXT NOT NULL,
                PRIMARY KEY (media_type, tmdb_id)
            );
            CREATE TABLE IF NOT EXISTS season_cache (
                tmdb_id TEXT NOT NULL, season_number INTEGER NOT NULL,
                episodes_json TEXT NOT NULL, fetched_at TEXT NOT NULL,
                PRIMARY KEY (tmdb_id, season_number)
            );
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY, media_key TEXT NOT NULL, date TEXT NOT NULL,
                kind TEXT NOT NULL, title TEXT NOT NULL, subtitle TEXT NOT NULL,
                poster TEXT NOT NULL, overview TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_date ON events(date);
            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS media_sync (
                media_key TEXT PRIMARY KEY,
                selection_json TEXT NOT NULL,
                synced_at TEXT NOT NULL
            );
        """)


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def cache_is_fresh(timestamp, hours):
    try:
        fetched = datetime.fromisoformat(timestamp)
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - fetched < timedelta(hours=hours)
    except (TypeError, ValueError):
        return False


def api_get(path, params=None):
    if not SEERR_URL:
        raise RuntimeError("SEERR_URL manquante")
    response = requests.get(
        f"{SEERR_URL}/api/v1/{path.lstrip('/')}",
        headers={"X-Api-Key": SEERR_API_KEY},
        params=params, timeout=30,
    )
    response.raise_for_status()
    return response.json()

def tmdb_api_get(path, params=None):
    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY manquante")
    query = dict(params or {})
    headers = {}
    # TMDB v4 Read Access Tokens are JWTs; v3 API keys are sent as api_key.
    if TMDB_API_KEY.count(".") == 2:
        headers["Authorization"] = f"Bearer {TMDB_API_KEY}"
    else:
        query["api_key"] = TMDB_API_KEY
    response = requests.get(
        f"{TMDB_BASE_URL}/{path.lstrip('/')}",
        headers=headers, params=query, timeout=30,
    )
    # Ne jamais laisser requests inclure l'URL complète dans les erreurs :
    # avec une clé v3, celle-ci se trouve dans la query string.
    if not response.ok:
        raise requests.HTTPError(
            f"TMDB a répondu HTTP {response.status_code} pour {path}",
            response=response,
        )
    return response.json()


def normalize_tmdb_episode(episode):
    return {
        **episode,
        "airDate": episode.get("air_date"),
        "seasonNumber": episode.get("season_number"),
        "episodeNumber": episode.get("episode_number"),
    }


def normalize_tmdb_details(kind, details):
    normalized = {
        **details,
        "posterPath": details.get("poster_path"),
        "overview": details.get("overview") or "",
    }
    if kind == "movie":
        normalized.update({
            "title": details.get("title"),
            "releaseDate": details.get("release_date"),
        })
    else:
        normalized.update({
            "name": details.get("name"),
            "seasons": [
                {**season, "seasonNumber": season.get("season_number")}
                for season in details.get("seasons", [])
            ],
            "nextEpisodeToAir": (
                normalize_tmdb_episode(details["next_episode_to_air"])
                if isinstance(details.get("next_episode_to_air"), dict) else None
            ),
        })
    return normalized


def as_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None


def image_url(path):
    return f"{TMDB_IMAGE}{path}" if path else ""


def get_requests():
    if not SEERR_API_KEY:
        raise RuntimeError("SEERR_API_KEY manquante")
    items, skip, take = [], 0, 100
    while True:
        payload = api_get("request", {
            "take": take, "skip": skip, "filter": "approved",
            "sort": "added", "sortDirection": "desc",
        })
        batch = payload.get("results", []) if isinstance(payload, dict) else []
        items.extend(batch)
        total = (payload.get("pageInfo") or {}).get("results", 0)
        skip += len(batch)
        if not batch or len(batch) < take or (total and skip >= total):
            break
    return items


def get_jellyfin_media():
    if not JELLYFIN_URL or not JELLYFIN_API_KEY:
        log.warning("Jellyfin non configuré : synchronisation Seerr uniquement")
        return []
    headers = {"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"'}
    items, start = [], 0
    while True:
        response = requests.get(
            f"{JELLYFIN_URL}/Items", headers=headers,
            params={
                "Recursive": "true", "IncludeItemTypes": "Series,Movie",
                "Fields": "ProviderIds", "StartIndex": start, "Limit": 100,
            }, timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("Items", [])
        items.extend(batch)
        total = payload.get("TotalRecordCount", 0)
        if not batch or (total and len(items) >= total):
            break
        start += len(batch)

    media = []
    for item in items:
        kind = {"Series": "tv", "Movie": "movie"}.get(item.get("Type"))
        tmdb_id = (item.get("ProviderIds") or {}).get("Tmdb")
        if kind and tmdb_id:
            media.append({"mediaType": kind, "tmdbId": str(tmdb_id)})
    log.info("Jellyfin : %s médias parcourus, %s avec TMDB", len(items), len(media))
    return media


def get_media_details(kind, tmdb_id):
    with db_connect() as db:
        row = db.execute(
            "SELECT details_json, fetched_at FROM media_cache WHERE media_type=? AND tmdb_id=?",
            (kind, str(tmdb_id)),
        ).fetchone()
    if row and cache_is_fresh(row["fetched_at"], MEDIA_CACHE_HOURS):
        return json.loads(row["details_json"])

    try:
        details = normalize_tmdb_details(kind, tmdb_api_get(f"{kind}/{tmdb_id}", {"language": "fr-FR"}))
        with db_connect() as db:
            db.execute("""
                INSERT INTO media_cache VALUES (?, ?, ?, ?)
                ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                details_json=excluded.details_json, fetched_at=excluded.fetched_at
            """, (kind, str(tmdb_id), json.dumps(details, ensure_ascii=False), iso_now()))
        return details
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            if row:
                log.warning(
                    "TMDB ne trouve plus %s/%s : conservation du cache existant",
                    kind, tmdb_id,
                )
                return json.loads(row["details_json"])
            log.warning(
                "Média ignoré : TMDB ne trouve pas %s/%s (HTTP 404)",
                kind, tmdb_id,
            )
            return {}
        if row:
            log.warning("Utilisation du cache TMDB périmé pour %s/%s", kind, tmdb_id)
            return json.loads(row["details_json"])
        raise
    except requests.RequestException:
        if row:
            log.warning("Utilisation du cache TMDB périmé pour %s/%s", kind, tmdb_id)
            return json.loads(row["details_json"])
        raise


def get_season_episodes(tmdb_id, season_number):
    with db_connect() as db:
        row = db.execute(
            "SELECT episodes_json, fetched_at FROM season_cache WHERE tmdb_id=? AND season_number=?",
            (str(tmdb_id), int(season_number)),
        ).fetchone()
    if row and cache_is_fresh(row["fetched_at"], SEASON_CACHE_HOURS):
        return json.loads(row["episodes_json"])
    try:
        payload = tmdb_api_get(f"tv/{tmdb_id}/season/{season_number}", {"language": "fr-FR"})
        episodes = [normalize_tmdb_episode(ep) for ep in (payload.get("episodes") or [])]
        with db_connect() as db:
            db.execute("""
                INSERT INTO season_cache VALUES (?, ?, ?, ?)
                ON CONFLICT(tmdb_id, season_number) DO UPDATE SET
                episodes_json=excluded.episodes_json, fetched_at=excluded.fetched_at
            """, (str(tmdb_id), int(season_number),
                  json.dumps(episodes, ensure_ascii=False), iso_now()))
        return episodes
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            if row:
                log.warning(
                    "TMDB ne trouve plus la saison %s de %s : conservation du cache",
                    season_number, tmdb_id,
                )
                return json.loads(row["episodes_json"])
            log.warning(
                "Saison %s ignorée : TMDB ne la trouve pas pour la série %s (HTTP 404)",
                season_number, tmdb_id,
            )
            return []
        if row:
            log.warning("Utilisation du cache périmé pour la saison %s de %s",
                        season_number, tmdb_id)
            return json.loads(row["episodes_json"])
        raise
    except requests.RequestException:
        if row:
            log.warning("Utilisation du cache périmé pour la saison %s de %s",
                        season_number, tmdb_id)
            return json.loads(row["episodes_json"])
        raise


def collect_candidates():
    candidates = {}
    for req in get_requests():
        media = req.get("media") or {}
        kind = media.get("mediaType") or req.get("mediaType") or req.get("type")
        tmdb_id = media.get("tmdbId") or req.get("mediaId") or media.get("id")
        if kind not in ("movie", "tv") or not tmdb_id:
            continue
        key = (kind, str(tmdb_id))
        selection = candidates.setdefault(key, {"seasons": set(), "all_seasons": False})
        seasons = req.get("seasons")
        if isinstance(seasons, list) and seasons:
            selection["seasons"].update(
                int(s) for s in seasons if str(s).isdigit() and int(s) > 0
            )
        else:
            selection["all_seasons"] = True

    for item in get_jellyfin_media():
        key = (item["mediaType"], str(item["tmdbId"]))
        selection = candidates.setdefault(key, {"seasons": set(), "all_seasons": False})
        if item["mediaType"] == "tv":
            selection["all_seasons"] = True
    return candidates


def build_events(kind, tmdb_id, selection):
    today = date.today()
    details = get_media_details(kind, tmdb_id)
    media_key = f"{kind}:{tmdb_id}"
    events = []

    if kind == "movie":
        release = as_date(details.get("releaseDate"))
        if release and release >= today:
            events.append({
                "id": f"movie-{tmdb_id}", "media_key": media_key,
                "date": release.isoformat(), "kind": "movie",
                "title": details.get("title") or "Film", "subtitle": "Sortie du film",
                "poster": image_url(details.get("posterPath")),
                "overview": details.get("overview") or "",
            })
        return events

    title = details.get("name") or "Série"
    seasons = details.get("seasons") or []
    if selection["all_seasons"]:
        numbers = [
            int(s["seasonNumber"]) for s in seasons
            if s.get("seasonNumber") is not None
            and str(s.get("seasonNumber")).isdigit()
            and int(s["seasonNumber"]) > 0
        ]
    else:
        numbers = sorted(selection["seasons"])

    episodes = []
    next_episode = details.get("nextEpisodeToAir")
    if isinstance(next_episode, dict):
        episodes.append(next_episode)
    for number in sorted(set(numbers)):
        episodes.extend(get_season_episodes(tmdb_id, number))

    seen = set()
    for episode in episodes:
        air_date = as_date(episode.get("airDate"))
        if not air_date or air_date < today:
            continue
        season = int(episode.get("seasonNumber") or 0)
        number = int(episode.get("episodeNumber") or 0)
        event_id = f"tv-{tmdb_id}-s{season}-e{number}"
        if event_id in seen:
            continue
        seen.add(event_id)
        events.append({
            "id": event_id, "media_key": media_key,
            "date": air_date.isoformat(), "kind": "tv", "title": title,
            "subtitle": f"S{season:02d}E{number:02d} · {episode.get('name') or 'Épisode'}",
            "poster": image_url(details.get("posterPath")),
            "overview": episode.get("overview") or details.get("overview") or "",
        })
    return events


def load_events():
    with db_connect() as db:
        rows = db.execute("""
            SELECT id, date, kind, title, subtitle, poster, overview
            FROM events WHERE date >= ? ORDER BY date, title
        """, (date.today().isoformat(),)).fetchall()
        state = dict(db.execute("SELECT key, value FROM sync_state").fetchall())
    return [dict(row) for row in rows], state.get("last_sync")


def perform_sync():
    if not SYNC_LOCK.acquire(blocking=False):
        log.info("Synchronisation déjà en cours")
        return

    try:
        candidates = collect_candidates()
        log.info("%s médias uniques détectés", len(candidates))
        updated = {}
        failed = set()
        skipped = 0
        now = iso_now()

        with db_connect() as db:
            previous = {
                row["media_key"]: row
                for row in db.execute(
                    "SELECT media_key, selection_json, synced_at FROM media_sync"
                ).fetchall()
            }

        for (kind, tmdb_id), selection in candidates.items():
            media_key = f"{kind}:{tmdb_id}"
            selection_data = {
                "all_seasons": selection["all_seasons"],
                "seasons": sorted(selection["seasons"])
            }
            selection_json = json.dumps(selection_data, sort_keys=True)
            old = previous.get(media_key)

            if (
                old
                and old["selection_json"] == selection_json
                and cache_is_fresh(old["synced_at"], MEDIA_CACHE_HOURS)
            ):
                skipped += 1
                continue

            try:
                events = build_events(kind, tmdb_id, selection)
                updated[media_key] = {
                    "events": events,
                    "selection_json": selection_json
                }
            except Exception:
                failed.add(media_key)
                log.exception("Erreur de traitement pour %s", media_key)

        with db_connect() as db:
            for media_key, result in updated.items():
                db.execute("DELETE FROM events WHERE media_key=?", (media_key,))
                for event in result["events"]:
                    db.execute("""
                        INSERT OR REPLACE INTO events
                        (id, media_key, date, kind, title, subtitle, poster, overview)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, tuple(event[k] for k in (
                        "id", "media_key", "date", "kind", "title",
                        "subtitle", "poster", "overview"
                    )))
                db.execute("""
                    INSERT INTO media_sync (media_key, selection_json, synced_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(media_key) DO UPDATE SET
                    selection_json=excluded.selection_json,
                    synced_at=excluded.synced_at
                """, (media_key, result["selection_json"], now))

            active = {f"{kind}:{tmdb_id}" for kind, tmdb_id in candidates}
            for row in db.execute("SELECT media_key FROM media_sync").fetchall():
                if row["media_key"] not in active:
                    db.execute("DELETE FROM media_sync WHERE media_key=?", (row["media_key"],))
            for row in db.execute("SELECT DISTINCT media_key FROM events").fetchall():
                if row["media_key"] not in active:
                    db.execute("DELETE FROM events WHERE media_key=?", (row["media_key"],))
            db.execute("DELETE FROM events WHERE date < ?", (date.today().isoformat(),))
            db.execute("""
                INSERT INTO sync_state(key,value) VALUES('last_sync',?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (now,))

        events, last_sync = load_events()
        error = f"{len(failed)} média(s) en erreur" if failed else None
        with LOCK:
            CACHE.update(events=events, last_sync=last_sync, error=error)
        log.info(
            "Synchronisation terminée : %s événements, %s actualisés, %s ignorés, %s erreurs",
            len(events), len(updated), skipped, len(failed)
        )
    finally:
        SYNC_LOCK.release()


def sync_loop():
    while True:
        try:
            perform_sync()
        except Exception as exc:
            with LOCK:
                CACHE["error"] = str(exc)
            log.exception("Échec de synchronisation")
        time.sleep(SYNC_SECONDS)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/events")
def api_events():
    with LOCK:
        return jsonify(CACHE)


@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "configured": bool(SEERR_API_KEY and SEERR_URL and TMDB_API_KEY),
        "database": os.path.exists(DB_PATH),
    })


if __name__ == "__main__":
    init_db()
    try:
        events, last_sync = load_events()
        CACHE.update(events=events, last_sync=last_sync, error=None)
    except Exception:
        log.exception("Impossible de charger le cache SQLite au démarrage")
    threading.Thread(target=sync_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=30050, threaded=True)
