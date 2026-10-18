# intake-digest

*Formerly `video-digest`.* A queue of **items**, each of a **kind** — `video`
today, `pdf` next — that is acquired, summarised and written to the vault as
a note. What is the same for every kind (the job queue, stage checkpoints,
the runner, the LLM client, the vault writer) lives once; what differs is one
module in `intake_digest/pipeline/kinds/`. The rest of this README describes
the `video` kind, which is the only one so far.

Takes a video URL (YouTube, and wired.com/video/watch pages), gets the best available transcript,
produces a structured summary, and writes an Obsidian note with deep links
back into the source — so the content can be read instead of watched. Sibling
to `podcast-digest`, `security-digest`, `taster`, `clippings-topics` and
`vault-ask` — see `~/projects/homelab/README.md` for the shared deploy
contract and vault-writing rules this follows.

**Status: pipeline + ingress wired (M0–M6).** `POST /jobs` resolves and
stores a video, and a background runner (`pipeline/runner.py`, scheduled)
walks it resolve → metadata → transcript → normalise → summarise → write
until a note lands in the vault, resuming from the first unfinished stage on
a retry. Caption-free videos park until the ASR worker is next awake; a
failed job writes nothing and (when `notifications.enabled`) alerts. A
scheduled watcher turns bare URLs dropped into `13 video-summaries/_video-queue.md`
into jobs, and once a note exists moves the line out to
`13 video-summaries/_video-completed.md` as a wikilink rather than rewriting
it in place — the queue holds only what's new, still running, or failed, and
is down to a single placeholder line the rest of the time (a 0-byte note
vanishes in LiveSync, so it is never left empty; a lost one is recreated).
`_video-completed.md` is grouped by the day each video was requested, newest
first, with `## Older` for entries the database cannot date.
`GET /jobs/{id}` reports stage progress, `POST /videos/{id}/rewrite`
re-renders from the stored digest, `GET /videos` exports finished summaries
(see below), and `GET /metrics` gives tier / spend totals. Still to do: the iOS Shortcut itself (client-side) and the watchlist
poller (deliberately deferred — see the design doc).

It is the **sixth** application writing to this vault, after `taster`,
`podcast-digest`, `security-digest`, `clippings-topics` and (read-only)
`vault-ask`. The rules for doing that without eating another writer's work are
in `~/.claude/skills/obsidian-vault-writer`, and this follows them: whole-file
ownership with a pinned filename for `13 video-summaries/` and
`14 video-transcripts/`, section ownership (`<!-- begin:intake-digest -->`,
`video_` frontmatter prefix) only in the shared `99 topics/`.

```sh
./deploy                 # build, ship, bring it up on the NAS
./deploy check            # what's running, and reachable over the LAN
uv run python -m intake_digest   # run locally, against ./config.yaml + .env
uv run pytest
```

## What it does not do (yet)

No iOS Shortcut — the API is Shortcut-shaped (202 + job id,
`status: "queued_for_transcription"` for caption-free videos, `GET /jobs/{id}`
to poll) but the Shortcut itself is not built, and the service is LAN-only
(no port-forward, no tunnel). No watchlist polling — the `watchlist` config
block is parsed and validated but deliberately unused (design §4). yt-dlp is
pinned in the image with no runtime self-update (the rootfs is read-only);
its version shows at `/healthz`.

## Reading the summaries elsewhere

`GET /videos` lists finished summaries as JSON, newest first, so another
service can mirror them. It is what `podcast-digest` imports from: the vault is
a fine place to keep notes and a poor place to work through them, since nothing
there tracks what has been read, and podcast-digest's console already does.

```
GET /videos?limit=50&since=<written_at>&since_id=<id>     X-API-Key required
```

Only rows that finished writing a note are returned (`stage_write = done`, with
both a `written_at` and a `digest`). Each carries `id`, `video_id`, `adapter`,
`note_path`, `written_at`, `updated_at` and the parsed `metadata` and `digest`.
**Never `transcript_json`** — by far the largest column, and the point of the
endpoint is the summary.

Two details a client depends on:

- **The cursor is the `(written_at, id)` pair, not the timestamp alone.**
  `written_at` is not unique: migration 2 backfilled it from `created_at`, and
  expanding a playlist stamps several rows in the same second. A timestamp-only
  cursor silently drops whichever rows share a page boundary — data loss the
  caller cannot detect, because the response looks complete.
- **A `+` in the offset must be encoded.** `+00:00` in a query string decodes
  to a space, so a hand-built URL yields an empty page and a client that stops
  early. The endpoint repairs that specific corruption rather than returning
  nothing; a cursor that still will not parse is a 400, so "no results" and
  "your cursor was garbage" never look alike.

`written_at` is write-once (enforced in SQL with `COALESCE`) precisely so this
ordering is stable: a value that moved would reorder a row relative to a
client's cursor, and moving forward would skip it for good.

## Topic linking

A video's `topics`/`entities` link into the shared `99 topics/` page for the
same subject when one already exists, rather than each video minting its own
near-duplicate. Two layers, in the order they actually help:

1. **Vocabulary hinting** (`pipeline/write.py::fetch_known_topics`, fed into
   the reduce prompt as `known_topics`). The vault's existing topic titles
   are shown to the model with an explicit instruction to reuse one exactly
   rather than invent a near-spelling — `clippings-topics`' pattern, ported
   here. This is the primary defence: it stops "Anthropic PBC" from being
   generated in the first place, for the common case where the model
   recognises the two as the same thing once shown the option.
2. **Alias matching** (`pipeline/write.py::resolve_topic_links` /
   `_alias_match`), for what hinting can't catch — an LLM that doesn't
   follow the instruction, or a spelling nobody's shown it yet. A name that
   doesn't slug-match any existing page is also checked against every
   candidate's `aliases:` (generic, unprefixed — nothing populates it yet,
   but a human or a future sibling writer could, and this needs no code
   change the day one does) and this writer's own `video_aliases:`
   (prefixed, accumulated in `_update_topic_page`: whenever a mention
   resolves to a page under a name that differs from its established
   `title:`, that phrasing is recorded as an alias, so the *next* time it
   shows up it resolves without minting a duplicate page).

Honest limit: alias matching is reactive, not proactive. The very first
time a genuinely novel spelling appears — nothing already links it by slug
or by a recorded alias — it still falls through to the ordinary
`topic_creation_threshold` path, same as before this existed. Vocabulary
hinting is what's meant to catch that case; aliases are the backstop for
once something's been seen at least once.

## Configuration

Non-secret settings in `config.yaml` (placeholders only — see the header
comment there for the two YAML traps this has to avoid). Secrets and
deployment topology (the vault's CouchDB address, the ASR worker's address)
are environment-only: copy `.env.example` to `.env` for local runs, and
`deploy.env.example` to `.deploy.env` for `./deploy`. Neither is tracked.

## PDFs

Each PDF becomes a note in `15 PDFs/`: a detailed summary (TL;DR, prose, key
points, the document's sections). Topic links come later, from
`clippings-topics`, which reads these notes (`CLIP_EXTRA_PREFIXES="15 PDFs/"`);
this app never writes `99 topics/` for PDFs. They arrive three ways:

- **A link in the intake queue.** Paste a link ending in `.pdf` into
  `00 intake/_intake_queue.md` (the same note that takes video links; the old
  `13 video-summaries/_video-queue.md` is still polled). It is fetched once and a
  copy kept; when the summary note exists the line moves to
  `00 intake/_intake_completed.md` as a wikilink. A link that cannot work
  (404/403, not a PDF, too big) stays in the queue and is not fetched again;
  a timeout or server error is retried on the next poll. `POST /jobs` takes the
  same links, for the iOS Shortcut.
- **The upload page.** `http://intake-digest.servers.zou/upload` — choose or drop
  PDFs. It sits behind the homelab login (Traefik `forwardAuth`, see
  `homelab-auth/README.md`): you sign in once like for the other apps and Traefik
  adds the app's own admin key to the request, so the page has no key field. It
  will not work at the container's own address, which has no login in front of
  it. `POST /pdfs` takes the file as the request body; `GET /pdfs/<job>` is the
  progress poll. The route lives in Traefik's `dynamic-config.yml` on the NAS
  (untracked, as for the other apps): router `intakeDigest-upload` gates
  `/upload` and `/pdfs*`, middleware `intakeDigest-key` holds the key, so
  **rotating the admin key means updating that one line too**.
- **A watched folder (optional, off by default).** Set `PDF_INPUT_DIR` (a NAS host
  path) in `.deploy.env` and it is mounted read-only at `/inbox` and scanned
  every 15 minutes. The originals there never move or change. `./deploy` never
  creates the folder, and the deploy account must be able to read it.

Links and uploads are kept in `/data/pdfs/<sha256>.pdf` — nothing else holds
those originals — up to `pdf.max_upload_mb` (100) each.

- **Identity** is the SHA-256 of the file: the same document by link, upload and
  folder is one note, and a renamed or moved file is not a new document. A PDF
  edited in place is a new document; the old note stays.
- **Scans:** pages with no text layer are rendered and read by the `vision`
  tier (`llm.tiers.vision`); without that tier a scanned PDF fails with a
  message saying so. Only the first `pdf.max_pages` (40) pages are read, and the
  note says when it covers less than the whole.
- **Folder removal** never deletes anything: a PDF that leaves a watched folder
  gets `pdf_missing_since` in its note's frontmatter, once. An empty or unmounted
  share is not read as "everything was deleted".
- `GET /items?kind=pdf` is the export; `GET /videos` never includes PDFs.

## Renamed from video-digest

What changed and what deliberately did not:

- **Env vars** are `INTAKE_*` now. The old `VIDEODIGEST_*` names are still
  read (a deprecation warning names each one), and `./deploy` still accepts
  them in the local `.env`; the NAS always receives the new names. The
  fallback is a one-release courtesy: drop it once nothing sets the old ones.
- **Unchanged on purpose:** the SQLite file is still `video_digest.sqlite`
  (so an existing `data/` opens as is), the vault owner tag is still
  `<!-- begin:video-digest -->` and the transcript note's `type:` is still
  `video-digest` (both live inside notes already in the vault), and
  `GET /videos` keeps its shape and only ever returns videos. `GET /items`
  is the kind-neutral export.
- **One-time NAS cutover** (the compose project name changed, so `up` will
  not adopt the old container, and both want the same macvlan IP):
  1. snapshot the database (`nas_sqlite_get`, never `ssh cat`);
  2. stop and remove the old project from its old directory
     (`docker compose -p video-digest down`);
  3. `mv /share/Container/video-digest /share/Container/intake-digest`;
  4. `./deploy`, then check `/healthz` from the Mac over the LAN;
  5. update podcast-digest's `PODAGENT_VIDEO_DIGEST_API_KEY` only if the
     admin key value itself changed — the URL and key are the same.

## The ASR worker

Transcription-by-audio (design's T2 tier) always runs remotely, on whichever
machine runs `~/projects/asr-server` (a `speaches` container speaking the
OpenAI audio API shape). There is no local fallback — the NAS's realtime
factor makes local ASR a non-option, the same finding that shaped
`podcast-digest`'s own remote ASR backend, which this vendors.

## Wired

`wired.com/video/watch/<slug>` URLs are accepted anywhere a YouTube URL is.
The page carries its own transcript, so there is no yt-dlp or ASR step
(`sources/wired.py`). The transcript has no timestamps: times are estimated
from line lengths, so Wired notes show highlight times as `~m:ss` without a
deep link.
