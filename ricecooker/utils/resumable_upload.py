"""Client for the GCS resumable upload protocol used by Kolibri Studio.

Uploads a file to an already-initiated resumable upload session in sequential
chunks. Studio bakes integrity metadata (md5, content-type) into the session
at creation time, so chunk PUTs carry no `Content-Type`, `x-goog-hash`, or
auth headers of their own.
"""

import re

from requests.exceptions import RequestException

CHUNK_SIZE = 8 * 1024 * 1024

# Consecutive transport failures (adapter retries already exhausted) tolerated
# per upload before giving up and re-raising.
MAX_RESUME_ATTEMPTS = 5

_RANGE_RE = re.compile(r"bytes=\d+-(\d+)")


def _query_offset(session, session_uri, total_size):
    """Ask GCS how many bytes of `session_uri` it has persisted so far.

    :param session: object exposing a `requests`-compatible `.put()`.
    :param session_uri: GCS resumable session URI to query.
    :param total_size: total size of the file in bytes.
    :return: next byte offset the caller should send from.
    :raises requests.exceptions.RequestException: on any unexpected status.
    """
    response = session.put(
        session_uri,
        data=b"",
        headers={"Content-Range": f"bytes */{total_size}"},
        allow_redirects=False,
    )

    if response.status_code == 308:
        match = _RANGE_RE.match(response.headers.get("Range", ""))
        return int(match.group(1)) + 1 if match else 0
    if response.status_code in (200, 201):
        return total_size
    raise RequestException(
        f"Unexpected status {response.status_code} querying offset for {session_uri}: {response.text}"
    )


def resumable_upload(session, session_uri, file_obj, total_size, chunk_size=CHUNK_SIZE):
    """Upload `file_obj` to `session_uri` via sequential chunked PUTs.

    Tolerates transport-level failures (after the GCS-scoped `Retry` adapter's
    own retries are exhausted) by re-querying the server-persisted offset and
    resuming from there, up to `MAX_RESUME_ATTEMPTS` consecutive failures.

    :param session: object exposing a `requests`-compatible `.put()`.
    :param session_uri: GCS resumable session URI to PUT chunks to.
    :param file_obj: seekable binary stream to read chunks from.
    :param total_size: total size of the file in bytes.
    :param chunk_size: max bytes to send per PUT.
    :raises requests.exceptions.RequestException: on any unexpected status,
        or after `MAX_RESUME_ATTEMPTS` consecutive transport failures.
    """
    if total_size == 0:
        response = session.put(
            session_uri,
            data=b"",
            headers={"Content-Range": "bytes */0"},
            allow_redirects=False,
        )
        if response.status_code in (200, 201):
            return
        raise RequestException(
            f"Unexpected status {response.status_code} uploading {session_uri}: {response.text}"
        )

    start = 0
    consecutive_failures = 0
    while start < total_size:
        file_obj.seek(start)
        chunk = file_obj.read(chunk_size)
        end = start + len(chunk) - 1

        try:
            response = session.put(
                session_uri,
                data=chunk,
                headers={"Content-Range": f"bytes {start}-{end}/{total_size}"},
                allow_redirects=False,
            )
        except RequestException:
            consecutive_failures += 1
            if consecutive_failures > MAX_RESUME_ATTEMPTS:
                raise
            start = _query_offset(session, session_uri, total_size)
            continue

        if response.status_code == 308:
            consecutive_failures = 0
            match = _RANGE_RE.match(response.headers.get("Range", ""))
            start = int(match.group(1)) + 1 if match else end + 1
            continue
        if response.status_code in (200, 201):
            return
        raise RequestException(
            f"Unexpected status {response.status_code} uploading {session_uri}: {response.text}"
        )
