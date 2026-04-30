"""
Synthesiser module.

Wraps Anthropic API calls for the two LLM-driven steps:
- Distribution descriptor generation (one short phrase)
- Application sentence synthesis (one full sentence or SKIP)

Both prompts are loaded from /prompts at module load. Versioned filenames so
prompt changes invalidate any future caching layer.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from anthropic import Anthropic

from config.settings import ANTHROPIC_MAX_TOKENS, ANTHROPIC_MODEL

log = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
DISTRIBUTION_PROMPT_PATH = PROMPTS_DIR / "distribution_descriptor_v1.md"
APPLICATION_PROMPT_PATH = PROMPTS_DIR / "application_synthesis_v1.md"

# Truncate context fed to the LLM to keep token usage in check
MAX_CONTEXT_CHARS = 6000


def _load_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _truncate_context(context: str) -> str:
    if len(context) <= MAX_CONTEXT_CHARS:
        return context
    return context[:MAX_CONTEXT_CHARS] + "\n\n[truncated]"


class Synthesiser:
    def __init__(self) -> None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY not set")
        self.client = Anthropic(api_key=api_key)
        self.distribution_prompt = _load_prompt(DISTRIBUTION_PROMPT_PATH)
        self.application_prompt = _load_prompt(APPLICATION_PROMPT_PATH)

    def _call(self, prompt: str) -> str:
        response = self.client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=ANTHROPIC_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        text_parts = [
            block.text for block in response.content if hasattr(block, "text")
        ]
        return "".join(text_parts).strip()

    def descriptor(self, case_context: str) -> str:
        """Return a short empathetic descriptor for a distribution case."""
        if not case_context.strip():
            return "a family in financial hardship"
        prompt = self.distribution_prompt.format(
            context=_truncate_context(case_context)
        )
        try:
            result = self._call(prompt)
        except Exception as exc:
            log.warning("Descriptor synthesis failed, using fallback: %s", exc)
            return "a family in financial hardship"
        # Strip stray quotes if the model added them despite instructions
        cleaned = result.strip().strip('"').strip("'").strip()
        if not cleaned or len(cleaned) > 120:
            return "a family in financial hardship"
        return cleaned

    def application_sentence(self, case_context: str) -> str | None:
        """Return a synthesised one-sentence summary, or None if the case should be skipped."""
        if not case_context.strip():
            return None
        prompt = self.application_prompt.format(
            context=_truncate_context(case_context)
        )
        try:
            result = self._call(prompt)
        except Exception as exc:
            log.warning("Application synthesis failed, skipping case: %s", exc)
            return None
        cleaned = result.strip().strip('"').strip("'").strip()
        if cleaned.upper() == "SKIP":
            return None
        if not cleaned:
            return None
        # Drop em/en dashes per NZF voice rules
        cleaned = cleaned.replace("\u2014", ", ").replace("\u2013", ", ")
        return cleaned
