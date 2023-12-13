""" Torrentio scraper module """
from datetime import datetime
import re
from requests.exceptions import RequestException
from utils.logger import logger
from utils.request import RateLimitExceeded, get, RateLimiter
from utils.settings import settings_manager
from program.media import (
    MediaItem,
    MediaItemContainer,
    MediaItemState,
)


class Scraper:
    """Scraper for torrentio"""

    def __init__(self):
        self.settings = "torrentio"
        self.class_settings = settings_manager.get(self.settings)
        self.last_scrape = 0
        self.filters = self.class_settings["filter"]
        self.minute_limiter = RateLimiter(
            max_calls=140, period=60 * 5, raise_on_limit=True
        )
        self.second_limiter = RateLimiter(max_calls=1, period=1)
        self.initialized = True

    def scrape(self, media_items: MediaItemContainer):
        """Scrape the torrentio site for the given media items
        and update the object with scraped streams"""
        logger.info("Scraping...")
        scraped_amount = 0
        items = [item for item in media_items if self._can_we_scrape(item)]
        for item in items:
            try:
                if item.type == "movie":
                    scraped_amount += self._scrape_items([item])
                else:
                    scraped_amount += self._scrape_show(item)
            except RequestException as exception:
                logger.error("%s, trying again next cycle", exception)
                break
            except RateLimitExceeded as exception:
                logger.error("%s, trying again next cycle", exception)
                break
        if scraped_amount > 0:
            logger.info("Scraped %s streams", scraped_amount)
        logger.info("Done!")

    def _scrape_show(self, item: MediaItem):
        scraped_amount = 0
        seasons = [season for season in item.seasons if self._can_we_scrape(season)]
        scraped_amount += self._scrape_items(seasons)
        episodes = [
            episode
            for season in item.seasons
            for episode in season.episodes
            if not season.is_scraped() and self._can_we_scrape(episode)
        ]
        scraped_amount += self._scrape_items(episodes)
        return scraped_amount

    def _scrape_items(self, items: list):
        amount_scraped = 0
        for item in items:
            data = self.api_scrape(item)
            log_string = item.title
            if item.type == "season":
                log_string = f"{item.parent.title} season {item.number}"
            if item.type == "episode":
                log_string = f"{item.parent.parent.title} season {item.parent.number} episode {item.number}"
            if len(data) > 0:
                item.set("streams", data)
                logger.debug("Found %s streams for %s", len(data), log_string)
                amount_scraped += 1
                continue
            logger.debug("Could not find streams for %s", log_string)
        return amount_scraped

    def _can_we_scrape(self, item: MediaItem) -> bool:
        def is_released():
            return (
                item.aired_at is not None
                and datetime.strptime(item.aired_at, "%Y-%m-%d:%H") < datetime.now()
            )

        def needs_new_scrape():
            return (
                datetime.now().timestamp() - item.scraped_at > 60 * 30
                or item.scraped_at == 0
            )

        if item.type == "show" and item.state in [
            MediaItemState.CONTENT,
            MediaItemState.LIBRARY_PARTIAL,
        ]:
            return True

        if item.type in ["movie", "season", "episode"] and is_released():
            valid_states = {
                "movie": [MediaItemState.CONTENT],
                "season": [MediaItemState.CONTENT],
                "episode": [MediaItemState.CONTENT],
            }
            if (item.state in valid_states[item.type]):
                return needs_new_scrape()

        return False

    def api_scrape(self, item):
        """Wrapper for torrentio scrape method"""
        with self.minute_limiter:
            if item.type == "season":
                identifier = f":{item.number}:1"
                scrape_type = "show"
                imdb_id = item.parent.imdb_id
            elif item.type == "episode":
                identifier = f":{item.parent.number}:{item.number}"
                scrape_type = "show"
                imdb_id = item.parent.parent.imdb_id
            else:
                identifier = None
                scrape_type = "movie"
                imdb_id = item.imdb_id

            url = (
                f"https://torrentio.strem.fun/{self.filters}"
                + f"/stream/{scrape_type}/{imdb_id}"
            )
            if identifier:
                url += f"{identifier}"
            with self.second_limiter:
                response = get(f"{url}.json", retry_if_failed=False)
                item.set("scraped_at", datetime.now().timestamp())
            if response.is_ok:
                data = {}
                for stream in response.data.streams:
                    if len(data) >= 20:
                        break
                    data[stream.infoHash] = {
                        "name": stream.title.split("\n👤")[0],
                    }
                if len(data) > 0:
                    return data
            return {}
