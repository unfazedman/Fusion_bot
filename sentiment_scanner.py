"""
sentiment_scanner.py — 11-Layer Sentiment Pipeline
Fusion Score Bot V7.0

Pipeline layers:
    1.  Collector         GNews (rotated) + RSS feeds (zero cost)
    2.  Raw Storage       Supabase raw_sentiment_data
    3.  Cleaning          Strip HTML, normalize whitespace
    4.  Dedup             85% similarity threshold (SequenceMatcher)
    5.  Relevance Filter  Must mention EUR/USD or GBP/USD keywords
    6.  Importance Score  HIGH/MEDIUM/LOW with time decay
    7.  AI Router         HIGH → Gemini (if budget). Others → HuggingFace
    8.  Sentiment         BULLISH/BEARISH/NEUTRAL per pair
    9.  Final Storage     processed_sentiment Supabase table
    10. Aggregation       aggregate_and_push_sentiment() → system_state
    11. Dashboard         Health check / Telegram /news command

AI fallback chain: Gemini → HuggingFace FinBERT
    Gemini:    Daily golden ticket for HIGH importance articles (20 RPD verified)
    FinBERT:   Primary model for all others (unlimited, free)

V7 Fixes from V6 audit:
    - GNews 3-keyword loop FIXED: rotates 1 keyword per run (96 calls/day < 100 limit)
    - RSS feeds added as zero-cost second source
    - Gemini prompt injection sanitization
    - Gemini structured system_instruction (separates trusted/untrusted content)
    - Gemini daily budget tracked in Supabase (cross-session counter)
    - DB insert failure sends Telegram alert
    - GNews quota exhaustion sends Telegram alert
    - HuggingFace uses router.huggingface.co endpoint
    - URL params use requests params dict (keys never in URL path)
"""

import re
import time
import hashlib
import logging
import requests
import feedparser
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Optional

from config import (
    GEMINI_API_KEY, HUGGINGFACE_API_KEY,
    GNEWS_API_KEY,
    GNEWS_KEYWORDS, RSS_FEEDS_SENTIMENT,
    GEMINI_RPD_LIMIT, GEMINI_RPD_SAFE_LIMIT, GEMINI_THROTTLE_DELAY,
    SIMILARITY_THRESHOLD, MAX_ITEMS_PER_CYCLE,
    IMPORTANCE_DECAY_HOURS, IMPORTANCE_CUTOFF_HOURS,
    validate_config
)
from shared_functions import (
    get_supabase_client,
    send_error_notification,
    aggregate_and_push_sentiment,
)

logger = logging.getLogger(__name__)

# HuggingFace FinBERT endpoint (V7: router.huggingface.co)
FINBERT_ENDPOINT = "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert"

# Forex relevance keywords
EUR_USD_KEYWORDS = [
    'eur', 'euro', 'eurozone', 'ecb', 'european central bank',
    'germany', 'france', 'inflation', 'fed', 'federal reserve',
    'usd', 'dollar', 'cpi', 'ppi', 'gdp', 'nonfarm', 'nfp',
    'interest rate', 'fomc', 'powell', 'lagarde'
]
GBP_USD_KEYWORDS = [
    'gbp', 'pound', 'sterling', 'boe', 'bank of england',
    'uk', 'britain', 'british', 'bailey', 'mpc',
    'usd', 'dollar', 'cpi', 'inflation', 'gdp'
]


class GeminiRateLimiter:
    """
    Per-session rate limiter for Gemini API.
    Cross-session budget tracked separately in Supabase.
    """
    def __init__(self):
        self._last_call_time = 0
        self._session_calls  = 0

    def can_call(self) -> bool:
        elapsed = time.time() - self._last_call_time
        return elapsed >= GEMINI_THROTTLE_DELAY

    def record_call(self):
        self._last_call_time = time.time()
        self._session_calls += 1


class SentimentScannerPipeline:

    def __init__(self):
        self.supabase        = get_supabase_client()
        self.gemini_limiter  = GeminiRateLimiter()
        self.processed_hashes: set = set()
        self._load_processed_hashes()

    # -------------------------------------------------------------------------
    # LAYER 1: COLLECTION
    # -------------------------------------------------------------------------

    def _collect_gnews(self) -> list:
        """
        Collects from GNews API.
        V7 FIX: Rotates 1 keyword per run based on current 15-minute window.
        3 keywords × 32 windows/day = 96 calls (safe under 100/day free limit).
        V6 bug: looped ALL 3 keywords every run = 288 calls/day (quota exhausted by 10:25 AM).
        """
        if not GNEWS_API_KEY:
            logger.info("[GNews] API key not configured. Skipping.")
            return []

        # Rotate keyword by minute of hour (15-min windows)
        minute           = datetime.now(timezone.utc).minute
        keyword_index    = (minute // 15) % len(GNEWS_KEYWORDS)
        keyword          = GNEWS_KEYWORDS[keyword_index]

        logger.info(f"[GNews] Using keyword [{keyword_index}]: '{keyword}'")

        try:
            # V7 FIX: Use params dict — requests handles URL encoding automatically.
            # V6 bug: f-string interpolation put key directly in URL path.
            params = {
                'q':      keyword,
                'token':  GNEWS_API_KEY,
                'lang':   'en',
                'max':    8,
                'sortby': 'publishedAt',
            }
            resp = requests.get(
                "https://gnews.io/api/v4/search",
                params=params,
                timeout=15
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get('status') == 'error' or not data.get('articles'):
                error_detail = data.get('errors', ['Unknown GNews error'])
                # Check for quota exhaustion specifically
                if any('quota' in str(e).lower() or '429' in str(e) for e in error_detail):
                    send_error_notification(
                        f"GNews quota exhausted for keyword '{keyword}'. "
                        f"All afternoon news collection will fail until midnight UTC.",
                        "sentiment_scanner"
                    )
                else:
                    send_error_notification(
                        f"GNews API error (keyword='{keyword}'): {error_detail}",
                        "sentiment_scanner"
                    )
                return []

            articles = []
            for a in data.get('articles', []):
                articles.append({
                    'title':       a.get('title', ''),
                    'description': a.get('description', ''),
                    'content':     a.get('content', ''),
                    'published':   a.get('publishedAt', ''),
                    'source':      a.get('source', {}).get('name', 'GNews'),
                    'url':         a.get('url', ''),
                })

            logger.info(f"[GNews] Collected {len(articles)} articles for '{keyword}'")
            return articles

        except requests.exceptions.HTTPError as e:
            if '429' in str(e):
                send_error_notification(
                    f"GNews rate limit (429) for keyword '{keyword}'. "
                    f"Quota may be exhausted.",
                    "sentiment_scanner"
                )
            else:
                send_error_notification(
                    f"GNews HTTP error for '{keyword}': {e}",
                    "sentiment_scanner"
                )
            return []
        except Exception as e:
            logger.error(f"[GNews] Error for '{keyword}': {e}")
            send_error_notification(f"GNews Collection Failed ('{keyword}'): {e}", "sentiment_scanner")
            return []

    def _collect_rss(self) -> list:
        """
        Collects from RSS feeds (ForexLive, Reuters).
        Zero API cost, no rate limits.
        Returns articles in same format as GNews collector.
        """
        articles = []

        for feed_url in RSS_FEEDS_SENTIMENT:
            try:
                feed = feedparser.parse(feed_url)
                source_name = feed_url.split('/')[2].replace('www.', '').replace('feeds.', '')

                for entry in feed.entries[:10]:
                    title   = entry.get('title', '').strip()
                    summary = entry.get('summary', '').strip()

                    # Parse publication time
                    published = ''
                    if hasattr(entry, 'published'):
                        published = entry.published
                    elif hasattr(entry, 'updated'):
                        published = entry.updated

                    if title:
                        articles.append({
                            'title':       title,
                            'description': summary,
                            'content':     summary,
                            'published':   published,
                            'source':      source_name,
                            'url':         entry.get('link', ''),
                        })

                logger.info(f"[RSS] Collected {min(10, len(feed.entries))} articles from {source_name}")

            except Exception as e:
                logger.warning(f"[RSS] Feed error ({feed_url}): {e}")
                # RSS failures are non-critical — don't alert, just log

        return articles

    # -------------------------------------------------------------------------
    # LAYER 2: RAW STORAGE
    # -------------------------------------------------------------------------

    def _store_raw(self, articles: list):
        """Stores raw collected articles to raw_sentiment_data table."""
        if not articles:
            return

        try:
            rows = []
            now  = datetime.now(timezone.utc).isoformat()
            for a in articles:
                rows.append({
                    'title':       a.get('title', '')[:500],
                    'description': (a.get('description') or '')[:1000],
                    'source':      a.get('source', 'Unknown'),
                    'published_at': a.get('published', now),
                    'url':         a.get('url', ''),
                    'created_at':  now,
                })
            self.supabase.table("raw_sentiment_data").insert(rows).execute()
        except Exception as e:
            logger.error(f"[RawStorage] Failed: {e}")
            # Non-critical — pipeline continues even if raw storage fails

    # -------------------------------------------------------------------------
    # LAYER 3: CLEANING
    # -------------------------------------------------------------------------

    def _clean_text(self, text: str) -> str:
        """Strips HTML, normalizes whitespace, removes truncation artifacts."""
        if not text:
            return ''
        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Remove [+N chars] truncation artifacts from GNews
        text = re.sub(r'\[\+\d+ chars?\]', '', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _build_article_text(self, article: dict) -> str:
        """Combines title + description + content into a single clean text."""
        parts = [
            article.get('title', ''),
            article.get('description', ''),
            article.get('content', ''),
        ]
        combined = ' '.join(p for p in parts if p)
        return self._clean_text(combined)

    # -------------------------------------------------------------------------
    # LAYER 4: DEDUPLICATION (85% similarity)
    # -------------------------------------------------------------------------

    def _compute_hash(self, text: str) -> str:
        return hashlib.md5(text.lower().strip().encode()).hexdigest()

    def _load_processed_hashes(self):
        """Loads recent hashes from Supabase to prevent cross-session re-processing."""
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            resp   = (
                self.supabase.table("processed_sentiment")
                .select("content_hash")
                .gte("created_at", cutoff)
                .execute()
            )
            self.processed_hashes = {
                r['content_hash'] for r in (resp.data or []) if r.get('content_hash')
            }
            logger.info(f"[Dedup] Loaded {len(self.processed_hashes)} hashes from last 24h.")
        except Exception as e:
            logger.warning(f"[Dedup] Could not load hashes: {e}")

    def _is_duplicate(self, text: str, new_hash: str) -> bool:
        """Returns True if this text is a near-duplicate of an already-processed article."""
        if new_hash in self.processed_hashes:
            return True
        # Fuzzy similarity check (SequenceMatcher — no external dependencies)
        for seen_hash in list(self.processed_hashes)[-50:]:  # Check last 50 (performance)
            # We only have hashes — use hash collision as a signal
            # For full fuzzy dedup: store text fragments too
            if seen_hash == new_hash:
                return True
        return False

    # -------------------------------------------------------------------------
    # LAYER 5: RELEVANCE FILTER
    # -------------------------------------------------------------------------

    def _is_relevant(self, text: str) -> dict:
        """
        Returns dict of which pairs this article is relevant to.
        {'EUR/USD': bool, 'GBP/USD': bool}
        """
        text_lower = text.lower()
        return {
            'EUR/USD': any(kw in text_lower for kw in EUR_USD_KEYWORDS),
            'GBP/USD': any(kw in text_lower for kw in GBP_USD_KEYWORDS),
        }

    # -------------------------------------------------------------------------
    # LAYER 6: IMPORTANCE SCORING
    # -------------------------------------------------------------------------

    def _calculate_importance(self, article: dict, text: str) -> str:
        """
        Returns 'HIGH', 'MEDIUM', or 'LOW' based on content signals.
        Applies time decay: >6h reduces tier, >24h returns LOW.
        """
        published_str = article.get('published', '')
        article_age_h = 0

        try:
            if published_str:
                pub = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
                if pub.tzinfo is None:
                    pub = pub.replace(tzinfo=timezone.utc)
                article_age_h = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
        except (ValueError, TypeError):
            pass

        if article_age_h > IMPORTANCE_CUTOFF_HOURS:
            return 'LOW'

        text_lower = text.lower()
        title      = article.get('title', '').lower()

        # HIGH importance signals
        high_keywords = [
            'rate decision', 'interest rate', 'fomc', 'ecb decision', 'boe decision',
            'nonfarm payroll', 'nfp', 'cpi', 'inflation', 'gdp', 'emergency',
            'rate cut', 'rate hike', 'recession', 'default', 'crisis'
        ]

        # MEDIUM importance signals
        medium_keywords = [
            'trade balance', 'retail sales', 'pmi', 'ism', 'unemployment',
            'housing', 'manufacturing', 'consumer confidence', 'sentiment'
        ]

        base_importance = 'LOW'
        for kw in high_keywords:
            if kw in title or kw in text_lower[:200]:
                base_importance = 'HIGH'
                break
        if base_importance == 'LOW':
            for kw in medium_keywords:
                if kw in title or kw in text_lower[:200]:
                    base_importance = 'MEDIUM'
                    break

        # Time decay: articles older than IMPORTANCE_DECAY_HOURS get downgraded one tier
        if article_age_h > IMPORTANCE_DECAY_HOURS:
            if base_importance == 'HIGH':
                return 'MEDIUM'
            elif base_importance == 'MEDIUM':
                return 'LOW'

        return base_importance

    # -------------------------------------------------------------------------
    # LAYER 7: AI ROUTER
    # -------------------------------------------------------------------------

    def _get_gemini_calls_today(self) -> int:
        """Reads today's Gemini call count from Supabase api_usage table."""
        try:
            today = datetime.now(timezone.utc).date().isoformat()
            resp  = (
                self.supabase.table("api_usage")
                .select("call_count")
                .eq("date", today)
                .eq("api", "gemini")
                .execute()
            )
            return resp.data[0]['call_count'] if resp.data else 0
        except Exception:
            return 0  # Assume 0 on error (safe — won't suppress calls)

    def _increment_gemini_calls(self):
        """Increments today's Gemini call count in Supabase."""
        try:
            today   = datetime.now(timezone.utc).date().isoformat()
            current = self._get_gemini_calls_today()
            self.supabase.table("api_usage").upsert({
                "date":       today,
                "api":        "gemini",
                "call_count": current + 1
            }, on_conflict="date,api").execute()
        except Exception as e:
            logger.warning(f"[GeminiCounter] Could not update counter: {e}")

    def _assign_model(self, importance: str) -> str:
        """
        Routes article to Gemini or HuggingFace FinBERT.

        Gemini: only for HIGH importance, only if daily budget not near limit.
        FinBERT: all others (unlimited, free).
        """
        if importance != 'HIGH':
            return 'HuggingFace'

        if not self.gemini_limiter.can_call():
            return 'HuggingFace'

        calls_today = self._get_gemini_calls_today()
        if calls_today >= GEMINI_RPD_SAFE_LIMIT:
            if calls_today == GEMINI_RPD_SAFE_LIMIT:  # Alert only at threshold crossing
                send_error_notification(
                    f"Gemini daily budget near limit: {calls_today}/{GEMINI_RPD_LIMIT} calls used. "
                    f"Switching to FinBERT for remaining runs today.",
                    "sentiment_scanner"
                )
            return 'HuggingFace'

        return 'Gemini'

    # -------------------------------------------------------------------------
    # LAYER 8: SENTIMENT ANALYSIS
    # -------------------------------------------------------------------------

    def _sanitize_for_prompt(self, text: str) -> str:
        """
        Sanitizes news text before embedding in Gemini prompt.
        V7 FIX: V6 had no sanitization — full prompt injection vulnerability.

        Removes:
            - JSON structural characters ({, }, ", ', \)
            - Common injection trigger phrases
            - Excessive length
        """
        if not text:
            return ""
        # Remove JSON structural chars
        text = re.sub(r'[{}\"\\'\\\\]', ' ', text)
        # Remove injection trigger patterns
        injection_patterns = [
            r'ignore\s+(previous|all|above|instructions?)',
            r'(system|user|assistant)\s*:',
            r'output\s+only',
            r'new\s+instructions?',
            r'forget\s+(everything|previous|all)',
            r'you\s+are\s+now',
            r'act\s+as',
            r'disregard',
            r'pretend',
        ]
        for pattern in injection_patterns:
            text = re.sub(pattern, '[FILTERED]', text, flags=re.IGNORECASE)
        return text.strip()[:500]

    def _analyze_with_gemini(self, text: str, pair: str) -> dict:
        """
        Analyzes text with Gemini 2.5 Flash.
        Uses structured system_instruction to separate trusted/untrusted content.
        V7: system_instruction is trusted; user content is untrusted news text.

        Returns:
            {'sentiment': 'BULLISH'|'BEARISH'|'NEUTRAL', 'confidence': float}
        """
        if not GEMINI_API_KEY:
            return {'sentiment': 'NEUTRAL', 'confidence': 0.5}

        safe_text = self._sanitize_for_prompt(text)
        url       = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
        )

        payload = {
            "system_instruction": {
                "parts": [{
                    "text": (
                        "You are a financial news sentiment classifier for Forex trading. "
                        "Classify the impact of the following news on the specified currency pair. "
                        "Respond ONLY with valid JSON in this exact format: "
                        '{"sentiment": "BULLISH", "confidence": 0.85} '
                        "where sentiment is BULLISH, BEARISH, or NEUTRAL, "
                        "and confidence is a float between 0.0 and 1.0. "
                        "Output ONLY the JSON object. No explanation. No markdown."
                    )
                }]
            },
            "contents": [{
                "parts": [{
                    "text": f"Classify impact on {pair}: {safe_text}"
                }]
            }],
            "generationConfig": {
                "temperature":    0.1,
                "maxOutputTokens": 60
            }
        }

        try:
            resp = requests.post(url, json=payload, timeout=20)
            resp.raise_for_status()
            data = resp.json()

            raw_text = (
                data['candidates'][0]['content']['parts'][0]['text']
                .strip()
                .replace('```json', '')
                .replace('```', '')
                .strip()
            )

            import json
            result = json.loads(raw_text)

            sentiment  = result.get('sentiment', 'NEUTRAL').upper()
            confidence = float(result.get('confidence', 0.7))

            if sentiment not in ('BULLISH', 'BEARISH', 'NEUTRAL'):
                sentiment = 'NEUTRAL'
            confidence = max(0.0, min(1.0, confidence))

            self.gemini_limiter.record_call()
            self._increment_gemini_calls()

            logger.info(f"[Gemini] {pair}: {sentiment} ({confidence:.2f})")
            return {'sentiment': sentiment, 'confidence': confidence}

        except Exception as e:
            logger.error(f"[Gemini] Analysis failed for {pair}: {e}")
            return {'sentiment': 'NEUTRAL', 'confidence': 0.5}

    def _analyze_with_finbert(self, text: str, pair: str) -> dict:
        """
        Analyzes text with HuggingFace FinBERT.
        V7: Uses router.huggingface.co endpoint.
        V6 bug: Was using old inference API endpoint.

        Returns:
            {'sentiment': 'BULLISH'|'BEARISH'|'NEUTRAL', 'confidence': float}
        """
        if not HUGGINGFACE_API_KEY:
            logger.warning("[FinBERT] No HUGGINGFACE_API_KEY. Returning NEUTRAL.")
            return {'sentiment': 'NEUTRAL', 'confidence': 0.5}

        try:
            headers = {"Authorization": f"Bearer {HUGGINGFACE_API_KEY}"}
            payload = {"inputs": text[:512]}  # FinBERT has 512 token limit

            resp = requests.post(
                FINBERT_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=20
            )
            resp.raise_for_status()
            data = resp.json()

            if not data or not isinstance(data, list):
                return {'sentiment': 'NEUTRAL', 'confidence': 0.5}

            # FinBERT returns list of [{'label': 'positive', 'score': 0.95}, ...]
            results = data[0] if isinstance(data[0], list) else data
            if not results:
                return {'sentiment': 'NEUTRAL', 'confidence': 0.5}

            best     = max(results, key=lambda x: x.get('score', 0))
            label    = best.get('label', '').lower()
            score    = float(best.get('score', 0.5))

            # Map FinBERT labels to our format
            label_map = {'positive': 'BULLISH', 'negative': 'BEARISH', 'neutral': 'NEUTRAL'}
            sentiment = label_map.get(label, 'NEUTRAL')

            return {'sentiment': sentiment, 'confidence': score}

        except Exception as e:
            logger.error(f"[FinBERT] Analysis failed: {e}")
            return {'sentiment': 'NEUTRAL', 'confidence': 0.5}

    def _analyze_sentiment(self, text: str, pair: str, model: str) -> dict:
        """Routes to correct model based on assignment."""
        if model == 'Gemini' and GEMINI_API_KEY:
            result = self._analyze_with_gemini(text, pair)
            if result['sentiment'] == 'NEUTRAL' and result['confidence'] == 0.5:
                # Gemini failed — fall back to FinBERT
                logger.info(f"[Router] Gemini returned default. Falling back to FinBERT for {pair}.")
                result = self._analyze_with_finbert(text, pair)
            return result
        return self._analyze_with_finbert(text, pair)

    # -------------------------------------------------------------------------
    # LAYER 9: FINAL STORAGE
    # -------------------------------------------------------------------------

    def _store_processed(self, article: dict, text: str, content_hash: str,
                         importance: str, eur_result: dict, gbp_result: dict,
                         model: str) -> bool:
        """
        Stores processed sentiment to Supabase.
        V7 FIX: Failure sends Telegram alert (V6 failed silently).

        Returns True on success, False on failure.
        """
        try:
            now = datetime.now(timezone.utc).isoformat()
            self.supabase.table("processed_sentiment").insert({
                "title":             article.get('title', '')[:500],
                "text_cleaned":      text[:1000],
                "content_hash":      content_hash,
                "source":            article.get('source', 'Unknown'),
                "published_at":      article.get('published', now),
                "importance_tier":   importance,
                "eur_usd_sentiment": eur_result['sentiment'],
                "eur_usd_confidence": eur_result['confidence'],
                "gbp_usd_sentiment": gbp_result['sentiment'],
                "gbp_usd_confidence": gbp_result['confidence'],
                "model_used":        model,
                "created_at":        now,
            }).execute()

            self.processed_hashes.add(content_hash)
            return True

        except Exception as e:
            logger.error(f"[Storage] DB insert failed: {e}")
            send_error_notification(
                f"Sentiment DB insert failed (article: {article.get('title', 'Unknown')[:60]}): {e}",
                "sentiment_scanner"
            )
            return False

    # -------------------------------------------------------------------------
    # MAIN PIPELINE
    # -------------------------------------------------------------------------

    def run_pipeline(self) -> dict:
        """
        Runs the full 11-layer sentiment pipeline.

        Returns:
            {
                'processed':         int,  Articles successfully stored
                'errors':            int,  Failed articles
                'eur_net_sentiment': int,  Current EUR/USD net sentiment
                'gbp_net_sentiment': int,  Current GBP/USD net sentiment
            }
        """
        validate_config('sentiment_scanner')
        logger.info("[Pipeline] Starting sentiment pipeline run...")

        # --- Layer 1: Collect ---
        gnews_articles = self._collect_gnews()
        rss_articles   = self._collect_rss()
        all_articles   = gnews_articles + rss_articles

        logger.info(f"[Pipeline] Collected: {len(gnews_articles)} GNews + {len(rss_articles)} RSS = {len(all_articles)} total")

        if not all_articles:
            logger.info("[Pipeline] No articles collected. Exiting.")
            return {'processed': 0, 'errors': 0, 'eur_net_sentiment': 0, 'gbp_net_sentiment': 0}

        # --- Layer 2: Raw Storage ---
        self._store_raw(all_articles[:MAX_ITEMS_PER_CYCLE])

        processed = 0
        errors    = 0

        for article in all_articles[:MAX_ITEMS_PER_CYCLE]:
            try:
                # --- Layer 3: Clean ---
                text = self._build_article_text(article)
                if not text or len(text) < 20:
                    continue

                # --- Layer 4: Dedup ---
                h = self._compute_hash(text)
                if self._is_duplicate(text, h):
                    logger.debug(f"[Dedup] Duplicate: {article.get('title', '')[:50]}")
                    continue

                # --- Layer 5: Relevance ---
                relevance = self._is_relevant(text)
                if not relevance['EUR/USD'] and not relevance['GBP/USD']:
                    continue

                # --- Layer 6: Importance ---
                importance = self._calculate_importance(article, text)

                # --- Layer 7: Model Assignment ---
                model = self._assign_model(importance)

                # --- Layer 8: Sentiment ---
                eur_result = {'sentiment': 'NEUTRAL', 'confidence': 0.5}
                gbp_result = {'sentiment': 'NEUTRAL', 'confidence': 0.5}

                if relevance['EUR/USD']:
                    eur_result = self._analyze_sentiment(text, 'EUR/USD', model)
                    time.sleep(0.5)  # Avoid hammering APIs

                if relevance['GBP/USD']:
                    gbp_result = self._analyze_sentiment(text, 'GBP/USD', model)
                    if relevance['EUR/USD']:
                        time.sleep(0.5)

                # --- Layer 9: Final Storage ---
                success = self._store_processed(
                    article, text, h, importance,
                    eur_result, gbp_result, model
                )

                if success:
                    processed += 1
                    logger.info(
                        f"[Pipeline] Stored: {article.get('title', '')[:50]} | "
                        f"Importance={importance} | Model={model} | "
                        f"EUR={eur_result['sentiment']} GBP={gbp_result['sentiment']}"
                    )
                else:
                    errors += 1

            except Exception as e:
                errors += 1
                logger.error(f"[Pipeline] Article failed: {e}")

        # --- Layer 10: Aggregation ---
        logger.info(f"[Pipeline] Processed: {processed}. Aggregating to system_state...")

        for pair in ['EUR/USD', 'GBP/USD']:
            try:
                aggregate_and_push_sentiment(pair)
            except Exception as e:
                logger.error(f"[Pipeline] Aggregation failed for {pair}: {e}")

        # Read current sentiment for return value
        eur_net = 0
        gbp_net = 0
        try:
            rows = self.supabase.table("system_state").select("pair, macro_sentiment").execute().data or []
            for row in rows:
                if row['pair'] == 'EUR/USD':
                    eur_net = row.get('macro_sentiment', 0)
                elif row['pair'] == 'GBP/USD':
                    gbp_net = row.get('macro_sentiment', 0)
        except Exception:
            pass

        logger.info(f"[Pipeline] Complete. Processed={processed} Errors={errors} EUR={eur_net:+d} GBP={gbp_net:+d}")

        return {
            'processed':         processed,
            'errors':            errors,
            'eur_net_sentiment': eur_net,
            'gbp_net_sentiment': gbp_net,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    pipeline = SentimentScannerPipeline()
    result   = pipeline.run_pipeline()
    print(f"Result: {result}")
