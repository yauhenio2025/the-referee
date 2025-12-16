"""
Services for The Referee
"""
from .scholar_search import ScholarSearchService
from .edition_discovery import EditionDiscoveryService
from .paper_resolution import PaperResolutionService

__all__ = [
    "ScholarSearchService",
    "EditionDiscoveryService",
    "PaperResolutionService",
]
