# SPDX-License-Identifier: GPL-3.0-or-later
import asyncio
import base64
import logging
import ssl
import os
import json
from os import PathLike
from pathlib import Path
from typing import Any, Optional, Union
from urllib.parse import urlparse

import aiohttp
import aiohttp_retry
import oras.client
import requests
from requests.auth import AuthBase

from hermeto.core.config import get_config
from hermeto.core.errors import FetchError
from hermeto.core.http_requests import (
    DEFAULT_RETRY_OPTIONS,
    SAFE_REQUEST_METHODS,
    get_requests_session,
)

pkg_requests_session = get_requests_session(retry_options={"allowed_methods": SAFE_REQUEST_METHODS})

log = logging.getLogger(__name__)


def download_binary_file(
    url: str,
    download_path: Union[str, PathLike[str]],
    auth: Optional[AuthBase] = None,
    insecure: bool = False,
    chunk_size: int = 8192,
) -> None:
    """
    Download a binary file (such as a TAR archive) from a URL.

    :param str url: URL for file download
    :param (str | PathLike) download_path: Path to download file to
    :param requests.auth.AuthBase auth: Authentication for the URL
    :param bool insecure: Do not verify SSL for the URL
    :param int chunk_size: Chunk size param for Response.iter_content()
    :raise FetchError: If download failed
    """
    timeout = get_config().requests_timeout
    try:
        resp = pkg_requests_session.get(
            url, stream=True, verify=not insecure, auth=auth, timeout=timeout
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        raise FetchError(f"Could not download {url}: {e}")

    with open(download_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            f.write(chunk)


async def _async_download_binary_file(
    session: aiohttp_retry.RetryClient,
    url: str,
    download_path: Union[str, PathLike[str]],
    auth: Optional[aiohttp.BasicAuth] = None,
    ssl_context: Optional[ssl.SSLContext] = None,
    chunk_size: int = 8192,
) -> None:
    """
    Download a binary file (such as a TAR archive) from a URL using asyncio.

    :param aiohttp_retry.RetryClient session: Aiohttp interface for making HTTP requests.
    :param str url: URL for file download
    :param str download_path: File path location
    :param aiohttp.BasicAuth auth: Authentication for the URL
    :param int chunk_size: Chunk size param for Response.content.read()
    :raise FetchError: If download failed
    """
    try:
        timeout = aiohttp.ClientTimeout(total=get_config().requests_timeout)

        log.debug(
            f"aiohttp.ClientSession.get(url: {url}, timeout: {timeout}, raise_for_status: True)"
        )
        async with session.get(
            url, timeout=timeout, auth=auth, raise_for_status=True, ssl=ssl_context
        ) as resp:
            with open(download_path, "wb") as f:
                while True:
                    chunk = await resp.content.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)

    except Exception as exception:
        log.error(f"Unsuccessful download: {url}")
        # "from None" since we have the exception context in the logs
        raise FetchError(
            f"exception_name: {exception.__class__.__name__}, " f"details: {exception}"
        ) from None

    log.debug(f"Download completed - {url}")


async def _async_download_oci_file(
    oci_url: str,
    download_path: Union[str, PathLike[str]],
    digest: str,
) -> None:
    """
    Download a binary file from an OCI registry.

    :param str oci_url: OCI URL in format oci://registry/repo:tag
    :param str download_path: File path location
    :param str digest: Expected digest of the layer to download
    """
    try:
        url = oci_url.removeprefix("oci://")
        client = _get_oras_client(url)

        log.debug(
            f"OrasClient.download_blob(url: {url}, digest: {digest}, download_path: {download_path})"
        )
        client.download_blob(url, digest, download_path)

    except Exception as exception:
        log.error(f"Unsuccessful OCI download: {oci_url}")
        # "from None" since we have the exception context in the logs
        raise FetchError(
            f"exception_name: {exception.__class__.__name__}, " f"details: {exception}"
        ) from None

    log.debug(f"Download OCI completed - {url}")

def _get_oras_client(url: str) -> oras.client.OrasClient:
    hostname = url.split("/")[0]
    client = oras.client.OrasClient(hostname=hostname)

    authfile = os.environ.get("REGSITRY_AUTH", os.path.expanduser("~/.docker/config.json"))
    if os.path.exists(authfile):
        # Remove tag from the URL if present (e.g., registry:5000/image:tag -> registry:5000/image)
        # Only remove the tag if it's after the last slash (i.e., not a port)
        last_slash = url.rfind("/")
        last_colon = url.rfind(":")
        if last_colon > last_slash:
            url = url[:last_colon]

        username = password = ""
        with open(authfile, "r") as f:
            config = json.load(f)
            while True:
                if url in config["auths"]:
                    log.debug(f"Found auth for {url}")
                    auth_b64 = config["auths"][url]["auth"]
                    decoded_auth = base64.b64decode(auth_b64).decode("utf-8")

                    username, password = decoded_auth.split(":", 1)
                    if username == password == "":
                        # Don't exit the loop yet. There may be more specific credentials.
                        log.warning(f"Ignoring empty credentials for {url}")
                    else:
                        client.login(username=username, password=password)
                        break

                # Keep searching for less specific credentials:
                #   registry:5000/org/image, registry:5000/org, registry:5000
                if "/" in url:
                    url = url.rsplit("/", 1)[0]
                    continue

                break

    return client

async def async_download_files(
    files_to_download: dict[str, Union[str, PathLike[str]]],
    concurrency_limit: int,
    ssl_context: Optional[ssl.SSLContext] = None,
    metadata: dict[PathLike[str], dict[str, str]] = None,
) -> None:
    """Asynchronous function to download files.

    :param files_to_download: Dict of files to download with file paths
    :param concurrency_limit: Max number of concurrent tasks (downloads).
    :param ssl_context: Optional SSL context for secure connections.
    :param metadata: Optional metadata dict indexed by file path, containing info such as checksum.
    """
    trace_config = aiohttp.TraceConfig()
    num_attempts: int = int(DEFAULT_RETRY_OPTIONS["total"])
    retry_options = aiohttp_retry.JitterRetry(attempts=num_attempts, retry_all_server_errors=True)
    retry_client = aiohttp_retry.RetryClient(
        retry_options=retry_options,
        trace_configs=[trace_config],
        # respect proxy settings and .netrc
        trust_env=True,
    )

    async with retry_client as session:
        tasks: set[asyncio.Task] = set()

        for url, download_path in files_to_download.items():
            if len(tasks) >= concurrency_limit:
                # Wait for some download to finish before adding a new one
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                # Check for exceptions
                try:
                    await asyncio.gather(*done)
                except FetchError:
                    # Close retry_client if any request fails (other tasks can be running,
                    # if a task is closed with the client open, an Warning is raised).
                    await retry_client.close()
                    for t in tasks:
                        t.cancel()
                    raise

            # Route to appropriate download function based on URL scheme
            if url.startswith("oci://"):
                if metadata is None:
                    raise ValueError("metadata is required for OCI downloads")
                digest = metadata[Path(download_path)]["checksum"]
                task = _async_download_oci_file(url, download_path, digest)
            else:
                task = _async_download_binary_file(
                    session, url, download_path, ssl_context=ssl_context
                )

            tasks.add(asyncio.create_task(task))

        await asyncio.gather(*tasks)


def extract_git_info(vcs_url: str) -> dict[str, Any]:
    """
    Extract important info from a VCS requirement URL.

    Given a URL such as git+https://user:pass@host:port/namespace/repo.git@123456?foo=bar#egg=spam
    this function will extract:
    - the "clean" URL: https://user:pass@host:port/namespace/repo.git
    - the git ref: 123456
    - the host, namespace and repo: host:port, namespace, repo

    The clean URL and ref can be passed straight to scm.Git to fetch the repo.
    The host, namespace and repo will be used to construct the file path under deps/pip.

    :param str vcs_url: The URL of a VCS requirement, must be valid (have git ref in path)
    :return: Dict with url, ref, host, namespace and repo keys
    """
    # If scheme is git+protocol://, keep only protocol://
    # Do this before parsing URL, otherwise urllib may not extract URL params
    if vcs_url.startswith("git+"):
        vcs_url = vcs_url[len("git+") :]

    url = urlparse(vcs_url)

    ref = url.path[-40:]  # Take the last 40 characters (the git ref)
    clean_path = url.path[:-41]  # Drop the last 41 characters ('@' + git ref)

    # Note: despite starting with an underscore, the namedtuple._replace() method is public
    clean_url = url._replace(path=clean_path, params="", query="", fragment="")

    # Assume everything up to the last '@' is user:pass. This should be kept in the
    # clean URL used for fetching, but should not be considered part of the host.
    _, _, clean_netloc = url.netloc.rpartition("@")

    namespace_repo = clean_path.strip("/")
    if namespace_repo.endswith(".git"):
        namespace_repo = namespace_repo[: -len(".git")]

    # Everything up to the last '/' is namespace, the rest is repo
    namespace, _, repo = namespace_repo.rpartition("/")

    return {
        "url": clean_url.geturl(),
        "ref": ref.lower(),
        "host": clean_netloc,
        "namespace": namespace,
        "repo": repo,
    }
