# src/__init__.py
"""
Пакет конструктора SQL запросов
"""

from .query_constructor import QueryConstructor, ConstructorStats
from .llm_fallback import LLMTeacher, LLMResult

__all__ = [
    'QueryConstructor',
    'ConstructorStats',
    'LLMTeacher',
    'LLMResult'
]