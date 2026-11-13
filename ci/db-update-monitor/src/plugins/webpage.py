"""HTML release-notes checker for vendors without RSS."""

from __future__ import annotations

import hashlib
import html as html_lib
import re
from typing import Any, Dict, List, Optional, Tuple

import requests

from models import FeatureUpdate
from plugins.feature_base import FeatureSourcePlugin
from text_util import USER_AGENT, collapse_whitespace, match_keywords, truncate

_SCRIPT = re.compile(r"<script[\s\S]*?</script>", re.IGNORECASE)
_STYLE = re.compile(r"<style[\s\S]*?</style>", re.IGNORECASE)
_TAG = re.compile(r"<[^>]+>")


def html_to_text(raw: str) -> str:
    text = _SCRIPT.sub(" ", raw)
    text = _STYLE.sub(" ", text)
    text = _TAG.sub(" ", text)
    return collapse_whitespace(html_lib.unescape(text))


class WebpageFeatureChecker(FeatureSourcePlugin):
    def plugin_type(self) -> str:
        return "webpage"

    def check_features(
        self,
        source_config: Dict[str, Any],
        database: Dict[str, Any],
        cursor: Optional[str],
    ) -> Tuple[List[FeatureUpdate], Optional[str]]:
        url = source_config["url"]
        timeout = source_config.get("timeout_seconds", 30)
        response = requests.get(url, timeout=(10, timeout), headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        text = html_to_text(response.text)
        page_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if cursor == page_hash:
            return [], page_hash

        keywords: List[str] = []
        seen: set[str] = set()
        for keyword in list(source_config.get("keywords") or []) + list(database.get("watch_features") or []):
            folded = keyword.casefold()
            if folded not in seen:
                seen.add(folded)
                keywords.append(keyword)
        matched = match_keywords(text, keywords)
        if not matched:
            return [], page_hash

        snippet = truncate(text, 400)
        return [
            FeatureUpdate(
                database_id=database["id"],
                database_name=database["name"],
                title=f"{database['name']}: {', '.join(matched)}",
                description=snippet,
                source_type=self.plugin_type(),
                matched_keywords=matched,
                url=url,
                adapter_capabilities=database.get("adapter_capabilities"),
            )
        ], page_hash
