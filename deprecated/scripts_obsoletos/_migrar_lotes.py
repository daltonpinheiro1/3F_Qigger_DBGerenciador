#!/usr/bin/env python3
"""Migra tabela lotes_importacao para adicionar 'reprocessamento' ao CHECK constraint."""
import sqlite3
import sys
sys.path.insert(0, '.')
from config import DB_V2_PATH

conn = sqlite3.connect(DB_V2_PATH)
conn.execute("PRAGMA foreign_keys = OFF")

# 1. Salvar dados existentes
cur = conn.execute("SELECT * FROM lotes_importacao")
rows = cur.fetchall()
print(f"Lotes existentes: {len(rows)}")

# 2. Renomear tabela antiga
conn.execute("ALTER TABLE lotes_importacao RENAME TO _lotes_importacao_old")

# 3. Criar nova tabela com 'reprocessamento' no CHECK
conn.execute("""
CREATE TABLE lotes_importacao (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome_arquivo TEXT NOT NULL,
    caminho_origem TEXT,
    tipo_arquivo TEXT NOT NULL CHECK (tipo_arquivo IN (
        'coverte_prop', 'portabilidade_tim', 'gross',
        'relatorio_objetos', 'resultado_gross', 'backoffice',
        'consulta_siebel', 'migracao', 'reprocessamento'
    )),
    hash_sha256 TEXT NOT NULL,
    qtd_registros INTEGER DEFAULT 0,
    qtd_inseridos INTEGER DEFAULT 0,
    qtd_erros INTEGER DEFAULT 0,
    status TEXT DEFAULT 'em_andamento' CHECK (status IN (
        'em_andamento', 'concluido', 'erro', 'duplicado'
    )),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finalizado_em TIMESTAMP,
    UNIQUE(hash_sha256)
)
""")

# 4. Copiar dados
conn.execute("""
    INSERT INTO lotes_importacao
    SELECT * FROM _lotes_importacao_old
""")

# 5. Remover tabela antiga
conn.execute("DROP TABLE _lotes_importacao_old")

conn.execute("PRAGMA foreign_keys = ON")
conn.commit()

# Verificar
cur = conn.execute("SELECT COUNT(*) FROM lotes_importacao")
print(f"Lotes após migração: {cur.fetchone()[0]}")

# Testar inserção com reprocessamento
cur = conn.execute(
    "INSERT INTO lotes_importacao (nome_arquivo, tipo_arquivo, hash_sha256) "
    "VALUES ('teste_reprocessamento', 'reprocessamento', 'teste_hash_123')"
)
conn.execute("DELETE FROM lotes_importacao WHERE hash_sha256 = 'teste_hash_123'")
conn.commit()
print("CHECK constraint atualizado com sucesso — 'reprocessamento' aceito")

conn.close()
