# Guia de Instalação do Python no macOS

## Status Atual
✅ Python 3.9.6 já está instalado
✅ pip 21.2.4 já está instalado

## Opções de Instalação

### Opção 1: Usar o Python Atual (Mais Rápido)
O Python 3.9.6 já instalado é suficiente para este projeto. Você pode começar a usar imediatamente:

```bash
# Verificar versão
python3 --version

# Instalar dependências do projeto
python3 -m pip install -r requirements.txt
```

### Opção 2: Instalar Python Mais Recente via Homebrew (Recomendado)

#### Passo 1: Instalar Homebrew
Abra o Terminal e execute:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Siga as instruções na tela. Você precisará:
- Inserir sua senha de administrador
- Pressionar Enter quando solicitado

#### Passo 2: Instalar Python Mais Recente
Após instalar o Homebrew, execute:

```bash
# Atualizar Homebrew
brew update

# Instalar Python (versão mais recente)
brew install python

# Verificar instalação
python3 --version
```

#### Passo 3: Configurar o Ambiente
```bash
# Adicionar Python do Homebrew ao PATH (se necessário)
echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Opção 3: Instalar Python via Site Oficial

1. Acesse: https://www.python.org/downloads/
2. Baixe a versão mais recente do Python para macOS
3. Execute o instalador .pkg
4. Siga as instruções do instalador

## Após Instalação

### 1. Instalar Dependências do Projeto
```bash
cd "/Users/mac/Library/CloudStorage/GoogleDrive-dspinheiro31@gmail.com/Outros computadores/Meu laptop/Documents/Projetos_python/3F_Qigger_DBGerenciador"

# Instalar todas as dependências
python3 -m pip install -r requirements.txt
```

### 2. Verificar Instalação
```bash
# Verificar Python
python3 --version

# Verificar pip
python3 -m pip --version

# Verificar dependências instaladas
python3 -m pip list
```

### 3. Criar Ambiente Virtual (Opcional mas Recomendado)
```bash
# Criar ambiente virtual
python3 -m venv venv

# Ativar ambiente virtual
source venv/bin/activate

# Instalar dependências no ambiente virtual
pip install -r requirements.txt
```

## Comandos Úteis

```bash
# Verificar versão do Python
python3 --version

# Atualizar pip
python3 -m pip install --upgrade pip

# Listar pacotes instalados
python3 -m pip list

# Desinstalar um pacote
python3 -m pip uninstall nome_do_pacote
```

## Solução de Problemas

### Se o comando `python3` não funcionar:
```bash
# Verificar se Python está no PATH
which python3

# Se não encontrar, adicionar ao PATH
export PATH="/usr/local/bin:$PATH"
```

### Se houver conflito entre versões:
```bash
# Verificar todas as versões instaladas
ls -la /usr/bin/python*
ls -la /usr/local/bin/python* 2>/dev/null
ls -la /opt/homebrew/bin/python* 2>/dev/null
```

### Se o pip não funcionar:
```bash
# Reinstalar pip
python3 -m ensurepip --upgrade
```

## Próximos Passos

Após instalar o Python e as dependências:

1. **Processar arquivos:**
   ```bash
   python3 processar_arquivo.py
   ```

2. **Ou usar o main.py:**
   ```bash
   python3 main.py --csv "caminho/para/arquivo.csv"
   ```

3. **Monitorar pasta automaticamente:**
   ```bash
   python3 main.py --watch data/entrada
   ```

