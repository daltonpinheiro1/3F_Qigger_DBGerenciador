"""
Pipeline de auditoria de vendas TIM Pré/Controle.

Integra dados do EVA (SQL Server), retornos RPA (CSV) e tabela de
parametrização de status para gerar auditoria consolidada.
"""


def __getattr__(name):
    if name == "ClassificadorStatus":
        from .classificador_status import ClassificadorStatus
        return ClassificadorStatus
    if name == "ProcessadorRetornoRPA":
        from .processador_retorno_rpa import ProcessadorRetornoRPA
        return ProcessadorRetornoRPA
    if name == "PipelineAuditoria":
        from .pipeline import PipelineAuditoria
        return PipelineAuditoria
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["ClassificadorStatus", "ProcessadorRetornoRPA", "PipelineAuditoria"]
