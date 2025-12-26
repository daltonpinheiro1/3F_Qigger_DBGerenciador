"""
Engine de decisão para portabilidade
"""

from .qigger_decision_engine import QiggerDecisionEngine, DecisionResult
from .trigger_loader import TriggerLoader

__all__ = ['QiggerDecisionEngine', 'DecisionResult', 'TriggerLoader']

