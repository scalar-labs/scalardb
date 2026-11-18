"""HTML release-notes checker for vendors without RSS."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from models import FeatureUpdate
from plugins.feature_base import FeatureSourcePlugin
from text_util import USER_AGENT, html_to_blocks, html_to_text, match_keywords, truncate

__all__ = ["WebpageFeatureChecker", "html_to_text"]

# Short block hashes keep the cursor small (the Oracle page has ~600 blocks);
# collisions at this length are negligible for a single page.
_BLOCK_HASH_LENGTH = 10
_MAX_SNIPPETS = 5


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _encode_cursor(page_hash: str, block_hashes: Set[str]) -> str:
    return f"{page_hash}:{','.join(sorted(block_hashes))}"


def _decode_cursor(cursor: Optional[str]) -> Tuple[Optional[str], Optional[Set[str]]]:
    """Return (page hash, block hashes). Block hashes are None when unknown."""
    if not cursor:
        return None, None
    page_hash, separator, blocks = cursor.partition(":")
    if not separator:
        return page_hash, None
    return page_hash, set(blocks.split(",")) if blocks else set()


class WebpageFeatureChecker(FeatureSourcePlugin):
    """Reports blocks of a page that are new since the last run and match keywords.

    The cursor stores a hash of the whole page plus a short hash of every block,
    so a later run can tell which blocks were added.
    """

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

        blocks = html_to_blocks(response.text)
        page_hash = _sha256("\n".join(blocks))
        block_hashes = {_sha256(block)[:_BLOCK_HASH_LENGTH] for block in blocks}
        new_cursor = _encode_cursor(page_hash, block_hashes)

        previous_page_hash, previous_blocks = _decode_cursor(cursor)
        if previous_page_hash == page_hash:
            return [], new_cursor

        if previous_blocks is None:
            new_blocks = blocks
        else:
            new_blocks = [
                block
                for block in blocks
                if _sha256(block)[:_BLOCK_HASH_LENGTH] not in previous_blocks
            ]

        keywords = self._keywords(source_config, database)
        matched: List[str] = []
        snippets: List[str] = []
        for block in new_blocks:
            block_matches = match_keywords(block, keywords)
            if not block_matches:
                continue
            snippets.append(block)
            matched.extend(keyword for keyword in block_matches if keyword not in matched)
        if not snippets:
            return [], new_cursor

        description = "; ".join(snippets[:_MAX_SNIPPETS])
        if len(snippets) > _MAX_SNIPPETS:
            description += f" (and {len(snippets) - _MAX_SNIPPETS} more)"
        return [
            FeatureUpdate(
                database_id=database["id"],
                database_name=database["name"],
                title=f"{database['name']}: page updated ({', '.join(matched)})",
                description=truncate(description, 500),
                source_type=self.plugin_type(),
                matched_keywords=matched,
                url=url,
                adapter_capabilities=database.get("adapter_capabilities"),
                fingerprint=f"{url}#{page_hash[:16]}",
            )
        ], new_cursor

    @staticmethod
    def _keywords(source_config: Dict[str, Any], database: Dict[str, Any]) -> List[str]:
        keywords: List[str] = []
        seen: Set[str] = set()
        for keyword in list(source_config.get("keywords") or []) + list(
            database.get("watch_features") or []
        ):
            folded = keyword.casefold()
            if folded not in seen:
                seen.add(folded)
                keywords.append(keyword)
        return keywords
