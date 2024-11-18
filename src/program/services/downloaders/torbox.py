import contextlib
from datetime import datetime
from pathlib import Path
from posixpath import splitext
from typing import Dict, List, Optional, Tuple
from requests import ConnectTimeout
from RTN import parse
from RTN.exceptions import GarbageTorrent
from program.db.db import db
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
        processed_hashes = set()
        stream_hashes = {}
        batch_size = 5
        with db.Session() as session:
            streams = session.query(Stream).filter(Stream.item_id == item._id).all()
            for i in range(0, len(streams), batch_size):
                batch = streams[i:i + batch_size]
                for stream in batch:
                    hash_lower = stream.infohash.lower()
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
    def find_required_files(self, item, container):
        files = [
            file
            for file in container
            if file
            and file["size"] > 10000
            and splitext(file["name"].lower())[1] in WANTED_FORMATS
        ]
        parsed_file = parse(file["name"])
        if item.type == "movie":
            for file in files:
                if parsed_file.type == "movie":
                    return [file]
        if item.type == "show":
            # Create a dictionary to map seasons and episodes needed
            needed_episodes = {}
            acceptable_states = [
                States.Indexed,
                States.Scraped,
                States.Unknown,
                States.Failed,
            ]
            for season in item.seasons:
                if season.state in acceptable_states and season.is_released:
                    needed_episode_numbers = {
                        episode.number
                        for episode in season.episodes
                        if episode.state in acceptable_states and episode.is_released
                    }
                    if needed_episode_numbers:
                        needed_episodes[season.number] = needed_episode_numbers
            if not needed_episodes:
                return False
            # Iterate over each file to check if it matches
            # the season and episode within the show
            matched_files = []
            for file in files:
                if not parsed_file.seasons or parsed_file.seasons == [0]:
                    continue
                # Check each season and episode to find a match
                for season_number, episodes in needed_episodes.items():
                    if season_number in parsed_file.season:
                        for episode_number in list(episodes):
                            if episode_number in parsed_file.episode:
                                # Store the matched file for this episode
                                matched_files.append(file)
                                episodes.remove(episode_number)
            if not matched_files:
                return False
            if all(len(episodes) == 0 for episodes in needed_episodes.values()):
                return matched_files
        if item.type == "season":
            needed_episodes = {
                episode.number: episode
                for episode in item.episodes
                if episode.state
                in [States.Indexed, States.Scraped, States.Unknown, States.Failed]
            }
            one_season = len(item.parent.seasons) == 1
            # Dictionary to hold the matched files for each episode
            matched_files = []
            season_num = item.number
            # Parse files once and assign to episodes
            for file in files:
                if not file or not file.get("name"):
                    continue
                if not parsed_file.seasons or parsed_file.seasons == [
                    0
                ]:  # skip specials
                    continue
                # Check if the file's season matches the item's season or if there's only one season
                if season_num in parsed_file.seasons or one_season:
                    for ep_num in parsed_file.episodes:
                        if ep_num in needed_episodes:
                            matched_files.append(file)
            if not matched_files:
                return False
            # Check if all needed episodes are captured (or atleast half)
            if len(needed_episodes) == len(matched_files):
                return matched_files
        if item.type == "episode":
            for file in files:
                if not file or not file.get("name"):
                    continue
                if (
                    item.number in parsed_file.episodes
                    and item.parent.number in parsed_file.seasons
                ):
                    return [file]
        return []
