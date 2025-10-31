import os
import time
from urllib.parse import urlparse
import shutil


import atlasopenmagic as atom
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session(total_retries=5, backoff_factor=0.6, status_forcelist=(500,502,503,504), timeout=(10,120)):
    s = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s._request_timeout = timeout
    return s


def download_with_retries(url, dest_path, session=None, chunk_size=1024*1024):
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        print(f"Already exists: {dest_path}")
        return dest_path
    if session is None:
        session = make_session()
    tmp = dest_path + ".part"
    attempts = 5
    for attempt in range(1, attempts+1):
        try:
            with session.get(url, stream=True, timeout=session._request_timeout) as r:
                r.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            fh.write(chunk)
            os.replace(tmp, dest_path)
            print(f"Downloaded: {dest_path}")
            return dest_path
        except Exception as exc:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass
            if attempt < attempts:
                wait = 2 ** attempt
                print(f"Download failed (attempt {attempt}) for {url}: {exc}. Retrying in {wait}s...")
                time.sleep(wait)
                continue
            print(f"Failed to download {url}: {exc}")
            raise


def local_filename_from_url(url):
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path)
    if not filename:
        filename = (parsed.netloc + parsed.path).replace('/', '_')
    return filename


def normalize_url_for_requests(url):
    """Return a normalized URL suitable for requests or a file:// URL for local paths.

    Handles common cases that trigger requests' "No connection adapters were found":
    - URLs starting with '//' -> prepend 'https:'
    - Backslashes in paths -> convert to forward slashes
    - Windows absolute paths or existing local files -> return file://<abs-path>
    - Missing scheme but looks like a host/path -> prepend https://
    """
    if not isinstance(url, str):
        return url
    u = url.strip()
    if '::' in u:
        u = u.split('::', 1)[1]
    try:
        if os.path.exists(u):
            return 'file://' + os.path.abspath(u)
    except Exception:
        pass

    if '\\' in u and not u.lower().startswith(('http://', 'https://', 'file://')):
        u = u.replace('\\', '/')

    if u.startswith('//'):
        u = 'https:' + u

    parsed = urlparse(u)
    if parsed.scheme == '':
        first_segment = parsed.path.split('/')[0]
        if first_segment.startswith('www.') or ('.' in first_segment and ' ' not in first_segment):
            u = 'https://' + u

    parsed = urlparse(u)
    if parsed.scheme and parsed.scheme.lower() != parsed.scheme:
        u = u.replace(parsed.scheme + ':', parsed.scheme.lower() + ':', 1)

    return u


def main():
    skim = 'GamGam'
    release = '2025e-13tev-beta'
    protocol = 'https'
    cache_dir = 'atlas_cache'
    limit = 0

    cache_dir = os.path.abspath(cache_dir)
    os.makedirs(cache_dir, exist_ok=True)

    print(f"Using atlasopenmagic release: {release}")
    atom.set_release(release)

    print(f"Fetching file list for skim '{skim}' (protocol={protocol})...")
    files_list = atom.get_urls('data', skim, protocol=protocol, cache=True)

    n_total = len(files_list)
    if limit > 0:
        files_list = files_list[:limit]
    print(f"Found {n_total} files, will download {len(files_list)} files to {cache_dir}")

    session = make_session()
    for i, url in enumerate(files_list, start=1):
        try:
            norm = normalize_url_for_requests(url)
            if norm.startswith('file://'):
                local_path = urlparse(norm).path
                if not os.path.exists(local_path):
                    print(f"({i}/{len(files_list)}) Local file not found: {local_path}")
                    continue
                fname = os.path.basename(local_path)
                dest = os.path.join(cache_dir, fname)
                print(f"({i}/{len(files_list)}) copying local file {local_path} -> {dest}")
                try:
                    if not os.path.exists(dest):
                        shutil.copy(local_path, dest)
                        print(f"Copied: {dest}")
                    else:
                        print(f"Already in cache: {dest}")
                except Exception as exc:
                    print(f"ERROR copying {local_path} -> {dest}: {exc}")
                continue

            # Otherwise assume HTTP(S) URL
            fname = local_filename_from_url(norm)
            dest = os.path.join(cache_dir, fname)
            print(f"({i}/{len(files_list)}) -> {fname} (from {norm})")
            download_with_retries(norm, dest, session=session)
        except Exception as exc:
            print(f"ERROR processing {url}: {exc}")

    print("Done. Files are in:", cache_dir)

if __name__ == '__main__':
    main()
