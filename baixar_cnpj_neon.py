"""
Baixa dados de CNPJ da Receita Federal e salva na tabela "empresas" no Neon.
Processa arquivos grandes em chunks para não travar na memória.

Filtros aplicados:
  - Apenas empresas com situação cadastral ATIVA
  - Exclui CNAEs proibidos (lista PROHIBITED_CNAES)

Requisitos:
    pip install requests pandas tqdm psycopg2-binary sqlalchemy

Uso:
    python baixar_cnpj_neon.py                        # todas as empresas ativas
    python baixar_cnpj_neon.py --partes 0 1           # teste rápido
    python baixar_cnpj_neon.py --sem-download         # já baixou, só processa
    python baixar_cnpj_neon.py --forcar-reprocessar   # recomeçar do zero
"""

import argparse
import json
import logging
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from io import TextIOWrapper

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine, text
from tqdm import tqdm

# ─── Configurações ────────────────────────────────────────────
COMPETENCIA    = "2026-03"
BASE_URL       = "https://arquivos.receitafederal.gov.br/public.php/webdav"
TOKEN          = "YggdBLfdninEJX9"
PASTA_DOWNLOAD = Path("receita_federal_cnpj")
CHECKPOINT     = Path("cnpj_checkpoint.json")
LOG_FILE       = Path("cnpj_ingestao.log")
DATABASE_URL   = "postgresql://neondb_owner:npg_FjPvo2f6dxRy@ep-mute-bird-ac4p4q9g-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
DB_CHUNK       = 50_000   # linhas por lote
READ_CHUNK     = 200_000  # linhas lidas do CSV por vez
DB_WORKERS     = 4        # threads paralelas para inserção no banco
MAX_TENTATIVAS = 3
ESPERA_RETRY   = 10

# CNAEs proibidos — empresas com atividade principal nessa lista são ignoradas
PROHIBITED_CNAES = {
    '8299704', '8299706', '9002702', '9200301', '9200302', '9200399',
    '9329803', '9329804', '9491000', '9492800', '9529106', '9609204',
    '1210700', '1220401', '1220402', '1220403', '1220499', '2092401',
    '2442300', '2550101', '2550102', '3211602', '3211603', '4520005',
    '4681801', '4681802', '4681803', '4681804', '4681805', '4732600',
    '4782202', '4783101', '4783102', '4789009', '6434400', '6440900',
    '6491300', '6619399', '7912100', '8422100', '9420100', '9430800',
    '724301',  '729404',  '893200',  '899101',  '899102',  '899103',
    '899199',  '9499500', '9493600', '220906',  '5590601', '9411100',
    '8720401', '9412099', '8711504', '7911200',
}

PORTE_MAP = {
    "00": "Não informado", "01": "Micro Empresa",
    "03": "Empresa de Pequeno Porte", "05": "Demais",
}
SITUACAO_MAP = {
    "01": "Nula", "02": "Ativa", "03": "Suspensa",
    "04": "Inapta", "08": "Baixada",
}

# ─── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─── Checkpoint ───────────────────────────────────────────────
def carregar_checkpoint() -> dict:
    if CHECKPOINT.exists():
        with open(CHECKPOINT) as f:
            return json.load(f)
    return {"downloads": {}, "partes_processadas": [], "chunks": {}}

def salvar_checkpoint(cp: dict):
    with open(CHECKPOINT, "w") as f:
        json.dump(cp, f, indent=2)

def marcar_download_ok(cp, nome, tamanho):
    cp["downloads"][nome] = {"tamanho": tamanho, "ok": True}
    salvar_checkpoint(cp)

def marcar_chunk_ok(cp, parte, chunk_num, total_acumulado):
    """Salva o último chunk concluído dentro de uma parte."""
    cp["chunks"][str(parte)] = {
        "ultimo_chunk": chunk_num,
        "total_acumulado": total_acumulado
    }
    salvar_checkpoint(cp)

def get_chunk_inicial(cp, parte):
    """Retorna o chunk a partir do qual deve continuar (0 = do início)."""
    info = cp.get("chunks", {}).get(str(parte))
    if info:
        log.info(f"  ↩ Retomando parte {parte} a partir do chunk {info['ultimo_chunk'] + 1} "
                 f"(já inseridos: {info['total_acumulado']:,})")
        return info["ultimo_chunk"] + 1, info["total_acumulado"]
    return 0, 0

def marcar_parte_ok(cp, parte, filtro_natureza):
    chave = f"{parte}_{filtro_natureza or 'all'}"
    if chave not in cp["partes_processadas"]:
        cp["partes_processadas"].append(chave)
    # Limpa checkpoint de chunk ao concluir a parte
    cp.get("chunks", {}).pop(str(parte), None)
    salvar_checkpoint(cp)

def parte_ja_processada(cp, parte, filtro_natureza):
    return f"{parte}_{filtro_natureza or 'all'}" in cp["partes_processadas"]

# ─── Banco ────────────────────────────────────────────────────
def get_engine():
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True,
                               pool_reset_on_return="rollback")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("✅ Conectado ao Neon com sucesso.")
        return engine
    except Exception as e:
        log.error(f"❌ Falha ao conectar no banco: {e}")
        sys.exit(1)

def limpar_registro(record: dict) -> dict:
    """Converte NaT, NaN e float nan para None compatível com PostgreSQL."""
    resultado = {}
    for k, v in record.items():
        if v is pd.NaT:
            resultado[k] = None
        elif v is None:
            resultado[k] = None
        elif isinstance(v, float) and pd.isna(v):
            resultado[k] = None
        elif isinstance(v, pd.Timestamp):
            resultado[k] = None if pd.isna(v) else v.to_pydatetime()
        else:
            try:
                resultado[k] = None if pd.isna(v) else v
            except (TypeError, ValueError):
                resultado[k] = v
    return resultado


import io as _io
import io
import csv as _csv

ERRO_FILE = Path("cnpj_erros.csv")
_ERRO_COLS = None  # cabeçalho gravado na primeira vez

def registrar_erro(cnpj: str, motivo: str, dados: dict = None):
    """Anota CNPJ com erro no arquivo cnpj_erros.csv com todos os dados da empresa."""
    global _ERRO_COLS
    dados = dados or {}
    novo = not ERRO_FILE.exists()

    with open(ERRO_FILE, "a", encoding="utf-8-sig", newline="") as f:
        writer = _csv.writer(f, delimiter=";")
        if novo or _ERRO_COLS is None:
            _ERRO_COLS = ["timestamp", "motivo_erro"] + [k for k in dados.keys()]
            writer.writerow(_ERRO_COLS)
        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            str(motivo)[:500],
        ] + [str(dados.get(k, "")) for k in _ERRO_COLS[2:]]
        writer.writerow(row)


def upsert(engine, df: pd.DataFrame):
    """
    Usa COPY para inserir em tabela temporária e depois faz
    INSERT … ON CONFLICT UPDATE da temp para a tabela final.
    Até 20x mais rápido que INSERT linha a linha.
    """
    if df.empty:
        return 0

    # Garante CNPJ com 14 dígitos (zfill de segurança)
    df = df.copy()
    df["cnpj"] = df["cnpj"].astype(str).str.zfill(14)
    df = df[df["cnpj"].str.len() == 14]
    if df.empty:
        return 0

    cols = list(df.columns)

    # Trata datas e nulos antes de serializar
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].astype(object).where(df[col].notna(), None)
    df = df.where(pd.notnull(df), None)

    col_names = ", ".join([f'"{c}"' for c in cols])
    updates   = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c != "cnpj"])

    raw_url = DATABASE_URL.replace("&channel_binding=require", "")

    total = 0
    for start in range(0, len(df), DB_CHUNK):
        chunk = df.iloc[start:start + DB_CHUNK]
        try:
            conn = psycopg2.connect(raw_url)
            cur  = conn.cursor()

            # Cria tabela temporária com mesma estrutura
            cur.execute(f"""
                CREATE TEMP TABLE _tmp_empresas
                ON COMMIT DROP
                AS SELECT {col_names} FROM empresas LIMIT 0
            """)

            # COPY via buffer em memória
            buf = _io.StringIO()
            writer = _csv.writer(buf, delimiter=",", quotechar='"',
                                 quoting=_csv.QUOTE_MINIMAL)
            for row in chunk.itertuples(index=False):
                writer.writerow([
                    "" if v is None or (isinstance(v, float) and pd.isna(v))
                    else str(v).replace("\n", " ").replace("\r", " ")
                    for v in row
                ])
            buf.seek(0)
            col_list = ", ".join(cols)
            cur.copy_expert(
                f"COPY _tmp_empresas ({col_list}) FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '', QUOTE '\"')",
                buf
            )

            # Upsert da temp para a tabela final
            cur.execute(f"""
                INSERT INTO empresas ({col_names})
                SELECT {col_names} FROM _tmp_empresas
                ON CONFLICT (cnpj) DO UPDATE SET {updates}
            """)

            total += cur.rowcount if cur.rowcount > 0 else len(chunk)
            conn.commit()

        except Exception as e:
            log.warning(f"  ⚠ COPY falhou, tentando INSERT linha a linha: {e}")
            try:
                conn.rollback()
            except:
                pass
            # Fallback: INSERT um por um, pulando e registrando os que falharem
            sql = text(
                f"INSERT INTO empresas ({col_names}) VALUES "
                f"({', '.join([':'+c for c in cols])}) "
                f"ON CONFLICT (cnpj) DO UPDATE SET {updates}"
            )
            for rec in [limpar_registro(r) for r in chunk.to_dict("records")]:
                try:
                    with engine.begin() as econn:
                        econn.execute(sql, rec)
                    total += 1
                except Exception as e2:
                    cnpj = rec.get("cnpj", "?")
                    log.warning(f"    ⚠ CNPJ {cnpj} ignorado: {e2}")
                    registrar_erro(cnpj, e2, rec)

        finally:
            try:
                cur.close()
                conn.close()
            except:
                pass
    return total



# ─── Download ─────────────────────────────────────────────────
def baixar_arquivo(nome: str, destino: Path, cp: dict) -> bool:
    url = f"{BASE_URL}/{COMPETENCIA}/{nome}"
    try:
        head = requests.head(url, auth=(TOKEN, ""), timeout=30)
        tamanho_servidor = int(head.headers.get("content-length", 0))
    except:
        tamanho_servidor = 0

    if destino.exists() and tamanho_servidor > 0 and destino.stat().st_size == tamanho_servidor:
        log.info(f"  ✓ {nome} já baixado ({destino.stat().st_size / 1e6:.0f} MB)")
        marcar_download_ok(cp, nome, tamanho_servidor)
        return True

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        tmp = destino.with_suffix(".tmp")
        try:
            log.info(f"  ⬇ Baixando {nome} (tentativa {tentativa}/{MAX_TENTATIVAS})…")
            resp = requests.get(url, auth=(TOKEN, ""), stream=True, timeout=600)
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            destino.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp, "wb") as f, tqdm(total=total, unit="B", unit_scale=True,
                                             unit_divisor=1024, desc=nome, leave=False) as bar:
                for chunk in resp.iter_content(chunk_size=256 * 1024):
                    f.write(chunk)
                    bar.update(len(chunk))
            if total > 0 and tmp.stat().st_size != total:
                raise ValueError(f"Tamanho incorreto: esperado {total}, recebido {tmp.stat().st_size}")
            with zipfile.ZipFile(tmp) as z:
                z.namelist()
            tmp.rename(destino)
            log.info(f"  ✓ {nome} — {destino.stat().st_size / 1e6:.0f} MB")
            marcar_download_ok(cp, nome, destino.stat().st_size)
            return True
        except Exception as e:
            log.warning(f"  ✗ Falha: {e}")
            if tmp.exists(): tmp.unlink()
            if tentativa < MAX_TENTATIVAS:
                time.sleep(ESPERA_RETRY)

    log.error(f"  ❌ {nome} falhou após {MAX_TENTATIVAS} tentativas.")
    return False

# ─── Lookups ──────────────────────────────────────────────────
def ler_zip_completo(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as z:
        with z.open(z.namelist()[0]) as f:
            return pd.read_csv(f, sep=";", encoding="latin1", header=None,
                               dtype=str, quoting=1, quotechar='"', on_bad_lines="skip")

def carregar_lookups(pasta: Path) -> dict:
    log.info("📚 Carregando tabelas auxiliares…")
    lookups = {}
    for arquivo, chave in [
        ("Municipios.zip", "municipios"), ("Cnaes.zip", "cnaes"),
        ("Naturezas.zip",  "naturezas"),  ("Motivos.zip", "motivos"),
        ("Paises.zip",     "paises"),
    ]:
        caminho = pasta / arquivo
        if not caminho.exists():
            lookups[chave] = {}
            continue
        df = ler_zip_completo(caminho).iloc[:, :2]
        df.columns = ["codigo", "descricao"]
        df["codigo"]    = df["codigo"].fillna("").str.strip()
        df["descricao"] = df["descricao"].fillna("").str.strip()
        lookups[chave]  = df.set_index("codigo")["descricao"].to_dict()
        log.info(f"  ✓ {chave}: {len(lookups[chave]):,} registros")
    return lookups

# ─── Processamento em chunks ──────────────────────────────────

# Padrões esperados para detectar desalinhamento
_RE_UF       = r'^[A-Z]{2}$'
_RE_CEP      = r'^\d{8}$'
_RE_DATA     = r'^\d{8}$'
_RE_IBGE     = r'^\d{4,7}$'
_RE_CNAE     = r'^\d{4,9}$'
_RE_EMAIL    = r'^[^@\s]+@[^@\s]+\.[^@\s]+$'
_RE_FONE     = r'^\d{6,11}$'

def limpar_se_invalido(serie: pd.Series, pattern: str) -> pd.Series:
    """Retorna a série mas com strings que não batem o padrão substituídas por vazio."""
    import re
    mask = serie.str.match(pattern, na=False)
    return serie.where(mask, "")

def montar_resultado(df: pd.DataFrame, lookups: dict) -> pd.DataFrame:
    r = pd.DataFrame()

    # ── Campos críticos (nunca apagados) ──────────────────────
    r["cnpj"]                    = df["cnpj_basico"] + df["cnpj_ordem"] + df["cnpj_dv"]
    r["razao_social"]            = df["razao_social"].fillna("")
    r["natureza_juridica_cod"]   = df["natureza_cod"].fillna("")
    r["natureza_juridica"]       = df["natureza_cod"].map(lookups["naturezas"]).fillna("")
    r["atividade_principal_cod"] = df["cnae_principal_cod"].fillna("")
    r["atividade_principal"]     = df["cnae_principal_cod"].map(lookups["cnaes"]).fillna("")

    tel1 = (df["ddd1"].fillna("") + df["tel1"].fillna("")).str.strip()
    tel2 = (df["ddd2"].fillna("") + df["tel2"].fillna("")).str.strip()
    r["telefone_principal"]      = limpar_se_invalido(tel1, _RE_FONE)
    r["telefone_secundario"]     = limpar_se_invalido(tel2, _RE_FONE)
    r["email"]                   = df["email"].fillna("").str[:115]

    # ── Campos secundários (limpos se parecerem desalinhados) ──
    r["porte_cod"]               = df["porte_cod"].fillna("").str[:2]
    r["porte"]                   = df["porte_cod"].map(PORTE_MAP).fillna("")
    r["capital_social"]          = pd.to_numeric(
        df["capital_social_raw"].str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce")
    r["ente_federativo"]         = df["ente_federativo"].fillna("").str[:50]
    r["tipo"]                    = df["tipo"].map({"1": "Matriz", "2": "Filial"}).fillna("")
    r["data_abertura"]           = pd.to_datetime(df["data_abertura"], format="%Y%m%d", errors="coerce")
    r["nome_fantasia"]           = df["nome_fantasia"].fillna("").str[:255]
    r["situacao_cadastral_cod"]  = df["situacao_cadastral_cod"].fillna("").str[:2]
    r["situacao_cadastral"]      = df["situacao_cadastral_cod"].map(SITUACAO_MAP).fillna("")
    r["situacao_cadastral_data"] = pd.to_datetime(df["situacao_cadastral_data"], format="%Y%m%d", errors="coerce")
    r["situacao_motivo_cod"]     = df["situacao_motivo_cod"].fillna("").str[:10]
    r["situacao_motivo"]         = df["situacao_motivo_cod"].map(lookups["motivos"]).fillna("")
    r["situacao_especial_cod"]   = df["situacao_especial_cod"].fillna("").str[:10]
    r["situacao_especial"]       = df.get("situacao_especial", pd.Series([""] * len(df))).fillna("").str[:255]
    r["situacao_especial_data"]  = pd.to_datetime(df.get("situacao_especial_data", pd.Series([""] * len(df))), format="%Y%m%d", errors="coerce")

    # Endereço — limpa se parecer desalinhado
    municipio = df["municipio_cod"].fillna("")
    uf        = df["uf"].fillna("").str[:2]
    cep       = df["cep"].fillna("")

    r["municipio_ibge"] = limpar_se_invalido(municipio, _RE_IBGE)
    r["estado"]         = limpar_se_invalido(uf, _RE_UF)
    r["cep"]            = limpar_se_invalido(cep, _RE_CEP)
    r["cidade"]         = municipio.map(lookups["municipios"]).fillna("")
    # Se município parece inválido, apaga cidade também
    r.loc[r["municipio_ibge"] == "", "cidade"] = ""

    r["logradouro"]   = (df["tipo_logradouro"].fillna("") + " " + df["logradouro"].fillna("")).str.strip().str[:100]
    r["numero"]       = df["numero"].fillna("").str[:10]
    r["complemento"]  = df["complemento"].fillna("").str[:156]
    r["bairro"]       = df["bairro"].fillna("").str[:50]

    r["pais_cod"]          = "1058"
    r["pais"]              = lookups["paises"].get("1058", "Brasil")
    r["ultima_atualizacao"]= datetime.now()

    return r[r["cnpj"].str.len() == 14]



def carregar_todas_empresas(pasta: Path, partes: list) -> dict:
    """Carrega todos os arquivos Empresas em um único dict cnpj_basico → tupla.
    Usa tuplas em vez de dicts aninhados para economizar ~60% de memória."""
    log.info("📖 Carregando TODOS os arquivos Empresas em memória…")
    emp_dict = {}
    _VAZIO = ("", "", "", "", "")
    for p in partes:
        arq = pasta / f"Empresas{p}.zip"
        if not arq.exists():
            log.warning(f"  ⚠ Empresas{p}.zip não encontrado, pulando.")
            continue
        df = ler_zip_completo(arq).iloc[:, :7]
        df.columns = ["cnpj_basico", "razao_social", "natureza_cod",
                      "ente_federativo", "capital_social_raw", "porte_cod", "extra"]
        for col in ["cnpj_basico", "razao_social", "natureza_cod",
                    "ente_federativo", "capital_social_raw", "porte_cod"]:
            df[col] = df[col].fillna("").str.strip()
        df["cnpj_basico"] = df["cnpj_basico"].str.zfill(8)

        # Tupla: (razao_social, natureza_cod, ente_federativo, capital_social_raw, porte_cod)
        # 60% menos memória que dict aninhado
        for row in df[["cnpj_basico", "razao_social", "natureza_cod",
                        "ente_federativo", "capital_social_raw", "porte_cod"]].itertuples(index=False):
            emp_dict[row.cnpj_basico] = (
                row.razao_social, row.natureza_cod,
                row.ente_federativo, row.capital_social_raw, row.porte_cod,
            )
        del df  # libera memória imediatamente
        log.info(f"  ✓ Empresas{p}: carregadas (total acumulado: {len(emp_dict):,})")

    log.info(f"  ✅ Total de empresas carregadas: {len(emp_dict):,}")
    return emp_dict


def processar_parte(engine, pasta: Path, parte: str, lookups: dict,
                    cp: dict, emp_dict: dict, filtro_uf=None, filtro_cnae=None) -> int:

    arq_est = pasta / f"Estabelecimentos{parte}.zip"

    if not arq_est.exists():
        log.error(f"  ❌ Estabelecimentos{parte}.zip não encontrado.")
        return 0

    cnpjs_validos = set(emp_dict.keys())
    log.info(f"  📖 Lendo Estabelecimentos{parte} em chunks de {READ_CHUNK:,}…")

    chunk_inicial, total = get_chunk_inicial(cp, parte)
    chunk_num = 0

    colunas_est = [
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "tipo", "nome_fantasia",
        "situacao_cadastral_cod", "situacao_cadastral_data", "situacao_motivo_cod",
        "situacao_especial_cod", "situacao_especial_data", "data_abertura",
        "cnae_principal_cod", "cnae_secundario",
        "tipo_logradouro", "logradouro", "numero", "complemento",
        "bairro", "cep", "uf", "municipio_cod",
        "ddd1", "tel1", "ddd2", "tel2", "ddd_fax", "fax", "email",
        "situacao_especial", "situacao_especial_data2",
    ]

    with zipfile.ZipFile(arq_est) as z:
        nome_csv = z.namelist()[0]
        with z.open(nome_csv) as raw:
            reader = pd.read_csv(
                TextIOWrapper(raw, encoding="latin1"),
                sep=";", header=None, dtype=str,
                quoting=1, quotechar='"', on_bad_lines="skip",
                chunksize=READ_CHUNK,
            )
            for chunk_est in reader:
                chunk_num += 1

                # Pula chunks já processados em execução anterior
                if chunk_num <= chunk_inicial:
                    if chunk_num % 10 == 0:
                        log.info(f"  ⏭ Pulando chunk {chunk_num} (já processado)")
                    continue

                chunk_est = chunk_est.iloc[:, :len(colunas_est)]
                chunk_est.columns = colunas_est[:len(chunk_est.columns)]
                for col in chunk_est.columns:
                    chunk_est[col] = chunk_est[col].fillna("").str.strip()

                chunk_est["cnpj_basico"] = chunk_est["cnpj_basico"].str.zfill(8)

                # Filtra só CNPJs existentes em Empresas
                chunk_est = chunk_est[chunk_est["cnpj_basico"].isin(cnpjs_validos)]

                # Apenas empresas ATIVAS (situacao_cadastral_cod = "02")
                chunk_est = chunk_est[chunk_est["situacao_cadastral_cod"] == "02"]

                # Exclui CNAEs proibidos
                chunk_est = chunk_est[~chunk_est["cnae_principal_cod"].isin(PROHIBITED_CNAES)]

                if filtro_uf:
                    chunk_est = chunk_est[chunk_est["uf"].str.upper() == filtro_uf.upper()]
                if filtro_cnae:
                    chunk_est = chunk_est[chunk_est["cnae_principal_cod"] == str(filtro_cnae)]
                if chunk_est.empty:
                    marcar_chunk_ok(cp, parte, chunk_num, total)
                    continue

                chunk_est["cnpj_ordem"] = chunk_est["cnpj_ordem"].str.zfill(4)
                chunk_est["cnpj_dv"]    = chunk_est["cnpj_dv"].str.zfill(2)

                # Acessa tupla: (razao_social, natureza_cod, ente_federativo, capital_social_raw, porte_cod)
                _VAZIO = ("", "", "", "", "")
                chunk_est["razao_social"]      = chunk_est["cnpj_basico"].map(lambda x: emp_dict.get(x, _VAZIO)[0])
                chunk_est["natureza_cod"]       = chunk_est["cnpj_basico"].map(lambda x: emp_dict.get(x, _VAZIO)[1])
                chunk_est["ente_federativo"]    = chunk_est["cnpj_basico"].map(lambda x: emp_dict.get(x, _VAZIO)[2])
                chunk_est["capital_social_raw"] = chunk_est["cnpj_basico"].map(lambda x: emp_dict.get(x, _VAZIO)[3])
                chunk_est["porte_cod"]          = chunk_est["cnpj_basico"].map(lambda x: emp_dict.get(x, _VAZIO)[4])

                resultado = montar_resultado(chunk_est, lookups)

                n = upsert(engine, resultado)
                total += n
                log.info(f"  chunk {chunk_num}: +{n:,} registros (total: {total:,})")
                marcar_chunk_ok(cp, parte, chunk_num, total)

    log.info(f"  ✅ Parte {parte} concluída: {total:,} registros")
    return total


# ─── Sócios ───────────────────────────────────────────────────

def processar_socios(engine, pasta: Path, parte: str) -> int:
    """
    Lê Socios{parte}.zip e atualiza nome_socio/cpf_socio no banco.
    Usa COPY + temp table para máxima velocidade (igual ao Simples).
    """
    arq = pasta / f"Socios{parte}.zip"
    if not arq.exists():
        log.error(f"  ❌ {arq} não encontrado.")
        return 0

    log.info(f"  📖 Lendo Socios{parte} em chunks…")
    colunas = [
        "cnpj_basico", "identificador", "nome_socio", "cpf_socio",
        "qualificacao_cod", "data_entrada", "pais_cod",
        "representante_legal", "nome_representante", "qualificacao_rep_cod",
        "faixa_etaria",
    ]

    raw_url   = DATABASE_URL.replace("&channel_binding=require", "")
    total     = 0
    chunk_num = 0
    vistos    = set()  # evita duplicar sócio por empresa

    with zipfile.ZipFile(arq) as z:
        with z.open(z.namelist()[0]) as raw:
            reader = pd.read_csv(
                TextIOWrapper(raw, encoding="latin1"),
                sep=";", header=None, dtype=str,
                quoting=1, quotechar='"', on_bad_lines="skip",
                chunksize=500_000,
            )
            for chunk in reader:
                chunk_num += 1
                chunk = chunk.iloc[:, :len(colunas)]
                chunk.columns = colunas[:len(chunk.columns)]
                for col in chunk.columns:
                    chunk[col] = chunk[col].fillna("").str.strip()

                chunk["cnpj_basico"] = chunk["cnpj_basico"].str.zfill(8)

                # Só pessoas físicas com CPF válido
                chunk = chunk[chunk["identificador"] == "2"]
                chunk = chunk[chunk["cpf_socio"].str.len() == 11]
                chunk = chunk[chunk["cpf_socio"] != ""]

                # Remove empresas já vistas
                chunk = chunk[~chunk["cnpj_basico"].isin(vistos)]
                chunk = chunk.drop_duplicates(subset="cnpj_basico", keep="first")

                if chunk.empty:
                    continue

                vistos.update(chunk["cnpj_basico"].tolist())

                t0 = time.time()
                try:
                    conn = psycopg2.connect(raw_url)
                    cur  = conn.cursor()

                    # 1. Cria tabela temporária
                    cur.execute("""
                        CREATE TEMP TABLE _tmp_socios (
                            cnpj_basico CHAR(8),
                            nome_socio  TEXT,
                            cpf_socio   VARCHAR(11)
                        ) ON COMMIT DROP
                    """)

                    # 2. COPY para temp table
                    buf = io.StringIO()
                    for _, row in chunk[["cnpj_basico","nome_socio","cpf_socio"]].iterrows():
                        nome = str(row.nome_socio).replace("\t"," ").replace("\n"," ")
                        buf.write(f"{row.cnpj_basico}\t{nome}\t{row.cpf_socio}\n")
                    buf.seek(0)
                    cur.copy_from(buf, "_tmp_socios", sep="\t",
                                  columns=["cnpj_basico","nome_socio","cpf_socio"])

                    # 3. UPDATE em massa via JOIN
                    cur.execute("""
                        UPDATE empresas e
                           SET nome_socio = t.nome_socio,
                               cpf_socio  = t.cpf_socio
                          FROM _tmp_socios t
                         WHERE LEFT(e.cnpj, 8) = t.cnpj_basico
                    """)
                    n = cur.rowcount
                    conn.commit()
                    total += n
                    log.info(f"  chunk {chunk_num}: {n:,} atualizados em {time.time()-t0:.1f}s (total: {total:,})")
                except Exception as e:
                    log.warning(f"  ⚠ Erro chunk {chunk_num}: {e}")
                    try: conn.rollback()
                    except: pass
                finally:
                    try: cur.close(); conn.close()
                    except: pass

    log.info(f"  ✅ Socios{parte}: {total:,} empresas atualizadas")
    return total


# ─── Simples / MEI ────────────────────────────────────────────

def processar_simples(engine, pasta: Path) -> int:
    """
    Lê Simples.zip e atualiza a coluna opcao_mei na tabela empresas.
    Usa COPY + temp table para máxima velocidade.
    """
    arq = pasta / "Simples.zip"
    if not arq.exists():
        log.error(f"  ❌ Simples.zip não encontrado em {pasta}")
        return 0

    log.info("  📖 Lendo Simples.zip em chunks…")
    colunas = [
        "cnpj_basico", "opcao_simples", "data_opcao_simples", "data_exclusao_simples",
        "opcao_mei", "data_opcao_mei", "data_exclusao_mei",
    ]

    raw_url = DATABASE_URL.replace("&channel_binding=require", "")
    total = 0
    chunk_num = 0

    with zipfile.ZipFile(arq) as z:
        with z.open(z.namelist()[0]) as raw:
            reader = pd.read_csv(
                TextIOWrapper(raw, encoding="latin1"),
                sep=";", header=None, dtype=str,
                quoting=1, quotechar='"', on_bad_lines="skip",
                chunksize=500_000,
            )
            for chunk in reader:
                chunk_num += 1
                chunk = chunk.iloc[:, :len(colunas)]
                chunk.columns = colunas[:len(chunk.columns)]
                for col in chunk.columns:
                    chunk[col] = chunk[col].fillna("").str.strip()

                chunk["cnpj_basico"] = chunk["cnpj_basico"].str.zfill(8)
                chunk = chunk[chunk["opcao_mei"].isin(["S", "N"])]
                if chunk.empty:
                    continue

                t0 = time.time()
                try:
                    conn = psycopg2.connect(raw_url)
                    cur  = conn.cursor()

                    # 1. Cria tabela temporária
                    cur.execute("""
                        CREATE TEMP TABLE _tmp_mei (
                            cnpj_basico CHAR(8),
                            opcao_mei   CHAR(1)
                        ) ON COMMIT DROP
                    """)

                    # 2. COPY para temp table
                    buf = io.StringIO()
                    for _, row in chunk[["cnpj_basico","opcao_mei"]].iterrows():
                        buf.write(f"{row.cnpj_basico}\t{row.opcao_mei}\n")
                    buf.seek(0)
                    cur.copy_from(buf, "_tmp_mei", sep="\t", columns=["cnpj_basico","opcao_mei"])

                    # 3. UPDATE em massa via JOIN
                    cur.execute("""
                        UPDATE empresas e
                           SET opcao_mei = t.opcao_mei
                          FROM _tmp_mei t
                         WHERE LEFT(e.cnpj, 8) = t.cnpj_basico
                    """)
                    n = cur.rowcount
                    conn.commit()
                    total += n
                    log.info(f"  chunk {chunk_num}: {n:,} atualizados em {time.time()-t0:.1f}s (total: {total:,})")
                except Exception as e:
                    log.warning(f"  ⚠ Erro chunk {chunk_num}: {e}")
                    try: conn.rollback()
                    except: pass
                finally:
                    try: cur.close(); conn.close()
                    except: pass

    log.info(f"  ✅ Simples: {total:,} empresas com opcao_mei atualizada")
    return total


# ─── Main ─────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uf",                 type=str, default=None)
    parser.add_argument("--cnae",               type=str, default=None)
    parser.add_argument("--partes",             nargs="+", default=list("0123456789"))
    parser.add_argument("--sem-download",       action="store_true")
    parser.add_argument("--forcar-reprocessar", action="store_true")
    parser.add_argument("--socios",             action="store_true",
                        help="Importar apenas sócios (rodar após importar empresas)")
    parser.add_argument("--simples",            action="store_true",
                        help="Importar opcao_mei do Simples.zip")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"  INÍCIO — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.socios:
        log.info("  MODO: importação de sócios")
    elif args.simples:
        log.info("  MODO: importação Simples/MEI")
    else:
        log.info(f"  Filtros: apenas ATIVAS | {len(PROHIBITED_CNAES)} CNAEs excluídos")
    log.info("=" * 60)

    if args.forcar_reprocessar and CHECKPOINT.exists():
        CHECKPOINT.unlink()
        log.info("🔄 Checkpoint apagado.")

    cp     = carregar_checkpoint()
    engine = get_engine()

    # ── Modo Simples/MEI ──────────────────────────────────────
    if args.simples:
        if not args.sem_download:
            log.info(f"\n{'─'*50}\n  DOWNLOAD SIMPLES\n{'─'*50}")
            baixar_arquivo("Simples.zip", PASTA_DOWNLOAD / "Simples.zip", cp)
        log.info(f"\n{'─'*50}\n  INGESTÃO SIMPLES/MEI\n{'─'*50}")
        total = processar_simples(engine, PASTA_DOWNLOAD)
        log.info(f"\n{'='*60}")
        log.info(f"  Total atualizado: {total:,}")
        log.info(f"  FIM — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info(f"{'='*60}")
        return

    # ── Modo sócios ───────────────────────────────────────────
    if args.socios:
        log.info(f"\n{'─'*50}\n  DOWNLOAD SÓCIOS\n{'─'*50}")
        if not args.sem_download:
            for p in args.partes:
                baixar_arquivo(f"Socios{p}.zip", PASTA_DOWNLOAD / f"Socios{p}.zip", cp)

        log.info(f"\n{'─'*50}\n  INGESTÃO SÓCIOS\n{'─'*50}")
        total = 0
        for p in args.partes:
            try:
                total += processar_socios(engine, PASTA_DOWNLOAD, p)
            except Exception as e:
                log.error(f"  ❌ Erro nos sócios parte {p}: {e}")

        log.info(f"\n{'='*60}")
        log.info(f"  Total de empresas com sócio atualizado: {total:,}")
        log.info(f"  FIM — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info(f"{'='*60}")
        return

    # ── Modo normal (empresas) ────────────────────────────────
    if not args.sem_download:
        log.info(f"\n{'─'*50}\n  DOWNLOAD — {COMPETENCIA}\n{'─'*50}")
        for nome in ["Cnaes", "Motivos", "Municipios", "Naturezas", "Paises"]:
            baixar_arquivo(f"{nome}.zip", PASTA_DOWNLOAD / f"{nome}.zip", cp)
        for p in args.partes:
            baixar_arquivo(f"Estabelecimentos{p}.zip", PASTA_DOWNLOAD / f"Estabelecimentos{p}.zip", cp)
            baixar_arquivo(f"Empresas{p}.zip",         PASTA_DOWNLOAD / f"Empresas{p}.zip", cp)

    lookups = carregar_lookups(PASTA_DOWNLOAD)

    # Carrega TODOS os arquivos Empresas de uma vez
    emp_dict = carregar_todas_empresas(PASTA_DOWNLOAD, args.partes)

    log.info(f"\n{'─'*50}\n  INGESTÃO\n{'─'*50}")
    total = 0
    partes_erro = []

    for p in args.partes:
        if parte_ja_processada(cp, p, None) and not args.forcar_reprocessar:
            log.info(f"  ⏭ Parte {p} já processada, pulando.")
            continue
        try:
            n = processar_parte(engine, PASTA_DOWNLOAD, p, lookups,
                                cp=cp, emp_dict=emp_dict,
                                filtro_uf=args.uf, filtro_cnae=args.cnae)
            total += n
            marcar_parte_ok(cp, p, None)
        except Exception as e:
            log.error(f"  ❌ Erro na parte {p}: {e}")
            partes_erro.append(p)

    log.info(f"\n{'='*60}")
    log.info(f"  Total inserido/atualizado: {total:,} registros")
    if partes_erro:
        log.info(f"  Partes com erro: {partes_erro} → rode novamente para retentar")
    log.info(f"  FIM — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'='*60}")

if __name__ == "__main__":
    main()
