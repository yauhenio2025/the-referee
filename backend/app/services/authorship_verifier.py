"""
Authorship Verification Module

Reusable LLM-based filtering to verify if works belong to a specific thinker.
Used for both search results and profile-seeded works.

Key use cases:
1. Name collisions (common names like "Álvaro Pinto" may match multiple people)
2. Works ABOUT vs BY the person
3. Works TO the person (letters, dedications)
4. Time period mismatches (work from 1911 can't be by someone born in 1909)
"""

import json
import re
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from anthropic import Anthropic

logger = logging.getLogger(__name__)


class AuthorshipVerifier:
    """
    Verifies authorship of works using LLM analysis.

    Can be used for:
    - Search results (existing use case)
    - Profile-seeded works (new use case - may include wrong-person matches)
    """

    def __init__(self, anthropic_client: Anthropic, model: str = "claude-sonnet-4-5-20250929"):
        self.client = anthropic_client
        self.model = model

    async def verify_works(
        self,
        thinker_name: str,
        thinker_birth_death: Optional[str],
        thinker_domains: List[str],
        thinker_notable_works: List[str],
        thinker_bio: Optional[str],
        works: List[Dict[str, Any]],
        source_context: str = "search_results",
    ) -> Dict[str, Any]:
        """
        Verify authorship for a batch of works.

        Args:
            thinker_name: Canonical name of the thinker
            thinker_birth_death: Life dates e.g., "(1909-1987)"
            thinker_domains: List of domains/fields
            thinker_notable_works: List of known works
            thinker_bio: Brief biography
            works: List of work dicts with title, authors, year, venue, etc.
            source_context: "search_results" or "scholar_profile"

        Returns:
            Dict with decisions list and counts
        """
        if not works:
            return {
                "success": True,
                "decisions": [],
                "accepted": 0,
                "rejected": 0,
                "uncertain": 0,
            }

        # Build works text
        works_text = ""
        for i, w in enumerate(works):
            works_text += f"""
WORK {i+1}:
  Title: {w.get('title', 'Unknown')}
  Authors: {w.get('authors') or w.get('authors_raw', 'Unknown')}
  Year: {w.get('year', 'Unknown')}
  Venue: {w.get('venue', 'N/A')}
  Citations: {w.get('citations') or w.get('citation_count', 0)}
"""

        # Extra context for profile-sourced works
        profile_warning = ""
        if source_context == "scholar_profile":
            profile_warning = """
IMPORTANT: These works come from a Google Scholar profile. Scholar profiles for
deceased academics are often created by third parties and may include:
- Works by DIFFERENT people with the same name (common names!)
- Works ABOUT the person, not BY them
- Letters/correspondence TO the person
- Works that merely CITE or MENTION the person
- Festschrifts or memorial volumes

Be especially careful with name collisions - "{name}" may be a common name.
""".format(name=thinker_name)

        # Parse birth year for date validation
        birth_year = None
        if thinker_birth_death:
            match = re.search(r'\((\d{4})', thinker_birth_death)
            if match:
                birth_year = int(match.group(1))

        birth_year_note = ""
        if birth_year:
            birth_year_note = f"\nNOTE: {thinker_name} was born in {birth_year}. Works dated before ~{birth_year + 18} are suspicious."

        prompt = f"""You are verifying authorship of works attributed to a specific thinker.

THINKER TO VERIFY:
- Name: {thinker_name}
- Life: {thinker_birth_death or 'Unknown'}
- Domains: {', '.join(thinker_domains) if thinker_domains else 'Not specified'}
- Notable works: {', '.join(thinker_notable_works[:5]) if thinker_notable_works else 'Not specified'}
- Bio: {thinker_bio or 'Not provided'}
{birth_year_note}
{profile_warning}

WORKS TO VERIFY:
{works_text}

YOUR TASK:
For each work, determine if it was AUTHORED BY {thinker_name} (the specific person described above).

ACCEPT if:
- Author field contains {thinker_name} or a clear name variant
- The work is in a domain this thinker works in
- The publication year is plausible given the thinker's life dates
- It appears to be written BY them

REJECT if:
- Different person with the same name (wrong field, wrong time period)
- The work is ABOUT the thinker, not BY them (e.g., "Analysis of X's philosophy")
- Letters/correspondence TO the thinker
- The thinker is only cited/referenced/mentioned
- Publication year impossible (e.g., published before thinker was ~18 years old)
- Clearly wrong field for this thinker

UNCERTAIN if:
- Can't determine from available information
- Could be a translation or variant title of a known work
- Name match but unclear if same person

Return a JSON array with one decision per work (same order as input):
[
  {{
    "work_index": 1,
    "decision": "accept",
    "confidence": 0.95,
    "reason": "Author matches, domain matches, year plausible"
  }},
  {{
    "work_index": 2,
    "decision": "reject",
    "confidence": 0.99,
    "reason": "Letters TO the person dated 1911-1919, but thinker born 1909 - different person"
  }}
]

ONLY return the JSON array, no other text."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text

            # Parse JSON from response
            json_match = re.search(r"\[[\s\S]*\]", text)
            if json_match:
                decisions = json.loads(json_match.group())

                accepted = sum(1 for d in decisions if d.get("decision") == "accept")
                rejected = sum(1 for d in decisions if d.get("decision") == "reject")
                uncertain = sum(1 for d in decisions if d.get("decision") == "uncertain")

                logger.info(
                    f"[AuthorshipVerifier] {thinker_name}: "
                    f"{accepted} accepted, {rejected} rejected, {uncertain} uncertain"
                )

                return {
                    "success": True,
                    "decisions": decisions,
                    "accepted": accepted,
                    "rejected": rejected,
                    "uncertain": uncertain,
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                }

        except json.JSONDecodeError as e:
            logger.error(f"[AuthorshipVerifier] JSON parse error: {e}")
        except Exception as e:
            logger.error(f"[AuthorshipVerifier] Error: {e}")

        # Fallback: mark all as uncertain
        return {
            "success": False,
            "decisions": [
                {"work_index": i+1, "decision": "uncertain", "confidence": 0.0, "reason": "Verification failed"}
                for i in range(len(works))
            ],
            "accepted": 0,
            "rejected": 0,
            "uncertain": len(works),
            "error": "LLM verification failed",
        }


# Convenience function
def get_authorship_verifier() -> AuthorshipVerifier:
    """Get a configured AuthorshipVerifier instance."""
    import os
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    return AuthorshipVerifier(client)
