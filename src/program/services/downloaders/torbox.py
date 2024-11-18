import contextlib
from datetime import datetime
from pathlib import Path
from posixpath import splitext
from typing import Dict, List, Optional, Tuple
from requests import ConnectTimeout
from RTN import parse
from RTN.exceptions import GarbageTorrent
from program.db.db import db
from program.db.db_functions import get_stream_count, load_streams_in_pages
from program.media.item import MediaItem
from program.media.state import States
from program.media.stream import Stream
from program.settings.manager import settings_manager
from program.services.downloaders.shared import DownloaderBase, InfoHash, DebridTorrentId
from loguru import logger
from program.utils.request import get, post

API_URL = "https://api.torbox.app/v1/api"
WANTED_FORMATS = {".mkv", ".mp4", ".avi"}

class TorBoxDownloader(DownloaderBase):
    """TorBox Downloader implementation"""
    
    def __init__(self):
        self.key = "torbox_downloader"
        self.settings = settings_manager.settings.downloaders.torbox
        self.api_key = self.settings.api_key
        self.base_url = API_URL
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        self.initialized = self.validate()
        if not self.initialized:
            return
        logger.success("TorBox Downloader initialized!")

    def validate(self) -> bool:
        """Validate the TorBox Downloader service"""
        if not self.settings.enabled:
            return False
            
        if not self.settings.api_key:
            logger.error("Torbox API key is not set")
            return False
            
        try:
            response = get(f"{self.base_url}/user/me", headers=self.headers)
            if response.is_ok:
                user_info = response.data.data
                expiration = user_info.premium_expires_at
                expiration_date = datetime.fromisoformat(expiration)
                delta = expiration_date - datetime.now().replace(tzinfo=expiration_date.tzinfo)
                
                if delta.days > 0:
                    logger.log("DEBRID", f"Your account expires in {delta.days} days.")
                else:
                    logger.log("DEBRID", "Your account expires soon.")
                    
                if user_info.plan == 0:
                    logger.error("You are not a premium member.")
                    return False
                    
                return True
                
        except ConnectTimeout:
            logger.error("Connection to Torbox timed out.")
        except Exception as e:
            logger.exception(f"Failed to validate Torbox settings: {e}")
            
        return False

    def get_instant_availability(self, hashes: List[InfoHash]) -> Dict[InfoHash, dict]:
        """Check instant availability of hashes"""
        hash_string = ",".join(hashes)
        response = get(
            f"{self.base_url}/torrents/checkcached?hash={hash_string}&list_files=True",
            headers=self.headers
        )
        if not response.is_ok:
            return {}
            
        return response.data.data

    def add_torrent(self, infohash: InfoHash) -> Optional[DebridTorrentId]:
        """Add a torrent to TorBox"""
        magnet_url = f"magnet:?xt=urn:btih:{infohash}&dn=&tr="
        response = post(
            f"{self.base_url}/torrents/createtorrent",
            data={"magnet": magnet_url, "seed": 1, "allow_zip": False},
            headers=self.headers
        )
        
        if not response.is_ok:
            return None
            
        return response.data.data.torrent_id

    def select_files(self, torrent_id: DebridTorrentId, file_ids: List[int]) -> bool:
        """Select files to download - Not needed for TorBox"""
        return True

    def get_torrent_info(self, torrent_id: DebridTorrentId) -> Optional[dict]:
        """Get torrent information"""
        response = get(
            f"{self.base_url}/torrents/mylist?bypass_cache=true",
            headers=self.headers
        )
        
        if not response.is_ok:
            return None
            
        torrents = response.data.data
        for torrent in torrents:
            if torrent["id"] == torrent_id:
                return torrent
                
        return None

    def delete_torrent(self, torrent_id: DebridTorrentId) -> bool:
        """Delete a torrent"""
        response = post(
            f"{self.base_url}/torrents/controltorrent",
            data={"torrent_id": torrent_id, "operation": "Delete"},
            headers=self.headers
        )
        return response.is_ok

    def run(self, item: MediaItem) -> bool:
        """Main download method for TorBox"""
        if not self.initialized:
            return False
            
        stream_count = get_stream_count(item._id)
        processed_hashes = set()
        stream_hashes = {}
        
        for page in range((stream_count // 5) + 1):
            with db.Session() as session:
                for _, infohash, stream in load_streams_in_pages(session, item._id, page, 5):
                    hash_lower = infohash.lower()
                    if hash_lower in processed_hashes:
                        continue
                        
                    processed_hashes.add(hash_lower)
                    stream_hashes[hash_lower] = stream
                    
                cached = self.get_instant_availability(list(stream_hashes.keys()))
                if cached:
                    for cache in cached.values():
                        if self.find_required_files(item, cache["files"]):
                            logger.log("DEBRID", f"Found cached files for: {item.log_string}")
                            torrent_id = self.add_torrent(cache["hash"])
                            if torrent_id:
                                return True
                                
                        stream = stream_hashes.get(cache["hash"].lower())
                        if stream:
                            stream.blacklisted = True
                            
                else:
                    logger.log("DEBRID", f"No cached files found for: {item.log_string}")
                    for stream in stream_hashes.values():
                        stream.blacklisted = True
                        
        return False
