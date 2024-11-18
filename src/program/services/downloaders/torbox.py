import contextlib
from datetime import datetime
from pathlib import Path
from posixpath import splitext
from typing import Dict, List, Optional, Tuple
from requests import ConnectTimeout, Session
from RTN import parse
from RTN.exceptions import GarbageTorrent
from program.db.db import db
from program.media.item import MediaItem
from program.media.state import States
from program.media.stream import Stream
from program.settings.manager import settings_manager
from program.services.downloaders.shared import DownloaderBase, InfoHash, DebridTorrentId, FileFinder, premium_days_left
from program.utils.request import (
    BaseRequestHandler,
    HttpMethod,
    ResponseType,
    create_service_session,
    get_rate_limit_params,
)
from loguru import logger
API_URL = "https://api.torbox.app/v1/api"
WANTED_FORMATS = {".mkv", ".mp4", ".avi"}
class TorBoxError(Exception):
    """Base exception for TorBox related errors"""
class TorBoxRequestHandler(BaseRequestHandler):
    def __init__(self, session: Session, base_url: str, request_logging: bool = False):
        super().__init__(
            session, 
            response_type=ResponseType.DICT, 
            base_url=base_url, 
            custom_exception=TorBoxError,
            request_logging=request_logging
        )
    def execute(self, method: HttpMethod, endpoint: str, **kwargs) -> dict:
        response = super()._request(method, endpoint, **kwargs)
        if not response.is_ok or not response.data:
            raise TorBoxError("Invalid response from TorBox")
        return response.data
class TorBoxAPI:
    """Handles TorBox API communication"""
    def __init__(self, api_key: str, proxy_url: Optional[str] = None):
        self.api_key = api_key
        rate_limit_params = get_rate_limit_params(per_minute=60)
        self.session = create_service_session(rate_limit_params=rate_limit_params)
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        if proxy_url:
            self.session.proxies = {"http": proxy_url, "https": proxy_url}
        self.request_handler = TorBoxRequestHandler(self.session, API_URL)
class TorBoxDownloader(DownloaderBase):
    """TorBox Downloader implementation"""
    def __init__(self):
        self.key = "torbox_downloader"
        self.settings = settings_manager.settings.downloaders.torbox
        self.api = None
        self.file_finder = None
        self.initialized = self.validate()
    def validate(self) -> bool:
        """Validate the TorBox Downloader service"""
        if not self._validate_settings():
            return False
        self.api = TorBoxAPI(
            api_key=self.settings.api_key,
            proxy_url=self.settings.proxy_url if self.settings.proxy_enabled else None
        )
        if not self._validate_premium():
            return False
        self.file_finder = FileFinder("name", "size")
        logger.success("TorBox Downloader initialized!")
        return True
    def _validate_settings(self) -> bool:
        """Validate configuration settings"""
        if not self.settings.enabled:
            return False
        if not self.settings.api_key:
            logger.warning("TorBox API key is not set")
            return False
        if self.settings.proxy_enabled and not self.settings.proxy_url:
            logger.error("Proxy is enabled but no proxy URL is provided")
            return False
        return True
    def _validate_premium(self) -> bool:
        """Validate premium status"""
        try:
            user_info = self.api.request_handler.execute(HttpMethod.GET, "user/me")
            if user_info.get("data", {}).get("plan", 0) == 0:
                logger.error("Premium membership required")
                return False
            expiration = datetime.fromisoformat(user_info["data"]["premium_expires_at"])
            logger.log("DEBRID", premium_days_left(expiration))
            return True
        except ConnectTimeout:
            logger.error("Connection to TorBox timed out")
        except Exception as e:
            logger.error(f"Failed to validate premium status: {e}")
        return False
    def get_instant_availability(self, hashes: List[InfoHash]) -> Dict[InfoHash, dict]:
        """Check instant availability of hashes"""
        if not self.initialized:
            return {}
        try:
            hash_string = ",".join(hashes)
            response = self.api.request_handler.execute(
                HttpMethod.GET,
                "torrents/checkcached",
                params={"hash": hash_string, "list_files": True}
            )
            return response.get("data", {})
        except Exception as e:
            logger.error(f"Failed to get instant availability: {e}")
            return {}
    def add_torrent(self, infohash: InfoHash) -> Optional[DebridTorrentId]:
        """Add a torrent to TorBox"""
        if not self.initialized:
            raise TorBoxError("Downloader not properly initialized")
        try:
            magnet_url = f"magnet:?xt=urn:btih:{infohash}&dn=&tr="
            response = self.api.request_handler.execute(
                HttpMethod.POST,
                "torrents/createtorrent",
                data={"magnet": magnet_url, "seed": 1, "allow_zip": False}
            )
            return response.get("data", {}).get("torrent_id")
        except Exception as e:
            logger.error(f"Failed to add torrent {infohash}: {e}")
            raise
    def select_files(self, torrent_id: DebridTorrentId, file_ids: List[int]) -> bool:
        """Select files to download - Not needed for TorBox"""
        return True
    def get_torrent_info(self, torrent_id: DebridTorrentId) -> Optional[dict]:
        """Get torrent information"""
        if not self.initialized:
            raise TorBoxError("Downloader not properly initialized")
        try:
            response = self.api.request_handler.execute(
                HttpMethod.GET,
                "torrents/mylist",
                params={"bypass_cache": True}
            )
            torrents = response.get("data", [])
            for torrent in torrents:
                if torrent["id"] == torrent_id:
                    return torrent
            return None
        except Exception as e:
            logger.error(f"Failed to get torrent info for {torrent_id}: {e}")
            raise
    def delete_torrent(self, torrent_id: DebridTorrentId) -> bool:
        """Delete a torrent"""
        if not self.initialized:
            raise TorBoxError("Downloader not properly initialized")
        try:
            self.api.request_handler.execute(
                HttpMethod.POST,
                "torrents/controltorrent",
                data={"torrent_id": torrent_id, "operation": "Delete"}
            )
            return True
        except Exception as e:
            logger.error(f"Failed to delete torrent {torrent_id}: {e}")
            raise
