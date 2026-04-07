#!/usr/bin/env python3
"""
Script de diagnóstico: verifica se as atualizações estão acontecendo corretamente.
Compara datas máximas em base_coverte_prop vs portabilidade_records.
"""
import sys
from pathlib import Path
from datetime import datetime

# Adicionar projeto ao path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DB_PATH
except ImportError:
    DB_PATH = str(PROJECT_ROOT / "data" / "portabilidade.db")

def main():
    import sqlite3
    
    print("=" * 70)
    print("DIAGNÓSTICO DE ATUALIZAÇÕES - 3F Qigger DB Gerenciador")
    print("=" * 70)
    print(f"Banco: {DB_PATH}")
    print(f"Data/hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    if not Path(DB_PATH).exists():
        print("❌ Banco de dados não encontrado!")
        return 1
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # 1. base_coverte_prop (COVERTE BASE PROP)
    print("[1] BASE_COVERTE_PROP (COVERTE BASE PROP.xlsx)")
    print("-" * 50)
    try:
        cur.execute("SELECT COUNT(*) FROM base_coverte_prop")
        total = cur.fetchone()[0]
        print(f"    Total de registros: {total:,}")
        
        # Data venda mais recente
        cur.execute("""
            SELECT data_venda, data_importacao, updated_at, origem_arquivo
            FROM base_coverte_prop
            WHERE data_venda IS NOT NULL AND TRIM(data_venda) != ''
            ORDER BY 
                CASE 
                    WHEN data_venda GLOB '[0-9][0-9][0-9][0-9]-*' THEN data_venda
                    WHEN data_venda GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]*' 
                         THEN SUBSTR(data_venda, 7, 4) || '-' || SUBSTR(data_venda, 4, 2) || '-' || SUBSTR(data_venda, 1, 2)
                    ELSE '0000-00-00'
                END DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        if rows:
            print(f"    Data venda mais recente (top 5):")
            for r in rows:
                print(f"      - {r['data_venda']} (import: {r['data_importacao']}, origem: {r['origem_arquivo']})")
        else:
            print("    ⚠ Nenhum registro com data_venda")
        
        # Última atualização da tabela
        cur.execute("SELECT MAX(updated_at) as ultimo FROM base_coverte_prop")
        ultimo = cur.fetchone()['ultimo']
        print(f"    Última atualização (updated_at): {ultimo or 'N/A'}")
        
    except sqlite3.OperationalError as e:
        print(f"    ❌ Tabela não existe ou erro: {e}")
    
    print()
    
    # 2. portabilidade_records (CSV Siebel)
    print("[2] PORTABILIDADE_RECORDS (CSV Siebel - Consultar portabilidade)")
    print("-" * 50)
    try:
        cur.execute("SELECT COUNT(*) FROM portabilidade_records")
        total = cur.fetchone()[0]
        print(f"    Total de registros: {total:,}")
        
        cur.execute("""
            SELECT data_inicial_processamento, MAX(id) as max_id
            FROM portabilidade_records
            WHERE data_inicial_processamento IS NOT NULL
            GROUP BY SUBSTR(data_inicial_processamento, 1, 10)
            ORDER BY data_inicial_processamento DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        if rows:
            print(f"    Data inicial processamento mais recente (top 5):")
            for r in rows:
                print(f"      - {r['data_inicial_processamento']} (id: {r['max_id']})")
        else:
            print("    ⚠ Nenhum registro com data_inicial_processamento")
            
        cur.execute("SELECT MAX(created_at) as ultimo FROM portabilidade_records")
        ultimo = cur.fetchone()['ultimo']
        print(f"    Última importação (created_at): {ultimo or 'N/A'}")
        
    except sqlite3.OperationalError as e:
        print(f"    ❌ Tabela não existe ou erro: {e}")
    
    print()
    
    # 3. Arquivo COVERTE na rede
    print("[3] ARQUIVO COVERTE BASE PROP (fonte)")
    print("-" * 50)
    caminho_rede = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/COVERTE BASE PROP.xlsx")
    if caminho_rede.exists():
        mtime = datetime.fromtimestamp(caminho_rede.stat().st_mtime)
        size_mb = caminho_rede.stat().st_size / (1024 * 1024)
        print(f"    ✓ Encontrado na rede")
        print(f"    Modificado em: {mtime.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"    Tamanho: {size_mb:.1f} MB")
    else:
        print(f"    ⚠ Não encontrado em: {caminho_rede}")
        print("    (Volume SMB pode não estar montado)")
    
    print()
    
    # 4. Resumo e recomendações
    print("=" * 70)
    print("RESUMO E RECOMENDAÇÕES")
    print("=" * 70)
    print("""
Os arquivos de homologação usam DUAS fontes:
  1. base_coverte_prop  → COVERTE BASE PROP.xlsx (vendas, data_venda)
  2. portabilidade_records → CSV Siebel (status, data_inicial_processamento)

Se as vendas param no dia 17/02:
  • base_coverte_prop desatualizado? → Execute processar_completo (processa COVERTE da rede)
  • portabilidade_records desatualizado? → Coloque o CSV mais recente do Siebel em
    IMPORTACOES_QIGGER ou data/entrada e execute processar_completo

Ordem correta: 1) Processar COVERTE  2) Processar CSV  3) Gerar homologação
""")
    
    conn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
