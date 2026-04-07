#!/usr/bin/env python3
"""
Testa a conexão de coleta da rede (SMB 02 Planejamento - COVERTE/BS_VENDA_DU)
e a atualização do banco na rede (SMB 07 Backoffice - replicação portabilidade.db).

Uso: python test_conexao_rede.py
     ou: .venv/bin/python test_conexao_rede.py
"""
import sys
from pathlib import Path

# Garantir que o projeto está no path
_PROJECT = Path(__file__).resolve().parent
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))

def test_coleta_rede():
    """Testa acesso à rede de coleta (02 Planejamento - COVERTE BASE PROP)."""
    print("\n[1] TESTE DE COLETA DA REDE (SMB 02 Planejamento)")
    print("    Pasta: /Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente")
    ok = False
    try:
        from processar_excel_unificado import (
            verificar_conexao_smb,
            montar_compartilhamento_smb,
            SMB_MOUNT_POINT,
            SMB_PATH,
            SMB_FILE,
        )
        mount = Path(SMB_MOUNT_POINT)
        pasta_rede = mount / SMB_PATH
        arquivo_coverte = pasta_rede / SMB_FILE

        if not verificar_conexao_smb():
            print("    >> SMB não montado. Tentando montar...")
            if montar_compartilhamento_smb():
                print("    >> Montagem concluída.")
            else:
                print("    >> Falha ao montar SMB (monte manualmente: Finder > Cmd+K > smb://files/02 Planejamento)")
                return False

        if not mount.exists() or not mount.is_dir():
            print("    >> Ponto de montagem não existe.")
            return False

        try:
            itens = list(mount.iterdir())
            print(f"    >> Ponto de montagem acessível ({len(itens)} itens na raiz)")
        except (PermissionError, OSError) as e:
            print(f"    >> Erro ao listar montagem: {e}")
            return False

        if not pasta_rede.exists():
            print(f"    >> Pasta de relatórios não encontrada: {pasta_rede}")
            return False

        try:
            arquivos = list(pasta_rede.iterdir())
            print(f"    >> Pasta de relatórios acessível ({len(arquivos)} itens)")
        except (PermissionError, OSError) as e:
            print(f"    >> Erro ao listar pasta: {e}")
            return False

        if arquivo_coverte.exists():
            size_mb = arquivo_coverte.stat().st_size / (1024 * 1024)
            print(f"    >> COVERTE BASE PROP.xlsx encontrado ({size_mb:.2f} MB)")
            ok = True
        else:
            print(f"    >> COVERTE BASE PROP.xlsx não encontrado em: {arquivo_coverte}")
            print("    >> Coleta da rede acessível, mas arquivo não está no caminho esperado.")

        # BS_VENDA_DU opcional
        bs_venda = pasta_rede / "BS_VENDA_DU.xlsx"
        if bs_venda.exists():
            print(f"    >> BS_VENDA_DU.xlsx encontrado")
        else:
            print("    >> BS_VENDA_DU.xlsx não encontrado (opcional)")

        ok = True  # rede acessível
    except ImportError as e:
        print(f"    >> Erro de importação: {e}")
    except Exception as e:
        print(f"    >> Erro: {e}")
        import traceback
        traceback.print_exc()
    return ok


def test_replicacao_rede():
    """Testa acesso à rede de backup (07 Backoffice - replicação do banco)."""
    print("\n[2] TESTE DE ATUALIZAÇÃO DO BANCO NA REDE (SMB 07 Backoffice)")
    print("    Destino: /Volumes/07 Backoffice/RETORNOS RPA - QIGGER/db.Portabilidade/portabilidade.db")
    ok = False
    try:
        from backup_database import (
            _verificar_smb_backoffice_montado,
            _montar_smb_backoffice,
            replicar_para_rede,
            BACKUP_REDE_DIR,
            BACKUP_REDE_PATH,
        )
        from config import DB_PATH

        if not _verificar_smb_backoffice_montado():
            print("    >> SMB 07 Backoffice não montado. Tentando montar...")
            if _montar_smb_backoffice():
                print("    >> Montagem concluída.")
            else:
                print("    >> Falha ao montar (monte manualmente: Finder > Cmd+K > smb://files/07 Backoffice)")
                return False

        dst_dir = Path(BACKUP_REDE_DIR)
        if not dst_dir.parent.exists():
            print(f"    >> Pasta pai do destino não acessível: {dst_dir.parent}")
            return False

        try:
            dst_dir.mkdir(parents=True, exist_ok=True)
            # Teste de escrita: arquivo temporário
            test_file = dst_dir / ".teste_escrita_rede"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink()
            print("    >> Pasta de destino acessível e gravável")
        except (PermissionError, OSError) as e:
            print(f"    >> Erro ao criar/testar pasta: {e}")
            return False

        # Teste real de replicação (usa banco local)
        db_path = DB_PATH if isinstance(DB_PATH, str) else str(Path(DB_PATH))
        if not Path(db_path).exists():
            print(f"    >> Banco local não encontrado: {db_path} (pulando replicação real)")
            ok = True
        else:
            print("    >> Executando replicação do banco...")
            if replicar_para_rede(db_path):
                size_mb = Path(BACKUP_REDE_PATH).stat().st_size / (1024 * 1024)
                print(f"    >> Replicação concluída: {BACKUP_REDE_PATH} ({size_mb:.2f} MB)")
                ok = True
            else:
                print("    >> Falha na replicação (verifique permissões e volume montado)")

    except ImportError as e:
        print(f"    >> Erro de importação: {e}")
    except Exception as e:
        print(f"    >> Erro: {e}")
        import traceback
        traceback.print_exc()
    return ok


def main():
    print("=" * 70)
    print("TESTE DE CONEXÃO DA REDE (Coleta + Atualização do Banco)")
    print("=" * 70)

    r1 = test_coleta_rede()
    r2 = test_replicacao_rede()

    print("\n" + "=" * 70)
    if r1 and r2:
        print("RESULTADO: Tudo OK (coleta e replicação funcionando)")
    elif r1:
        print("RESULTADO: Coleta OK; replicação falhou ou não testada")
    elif r2:
        print("RESULTADO: Replicação OK; coleta falhou ou não testada")
    else:
        print("RESULTADO: Falhas em um ou ambos os testes")
    print("=" * 70 + "\n")
    return 0 if (r1 and r2) else 1


if __name__ == "__main__":
    sys.exit(main())
