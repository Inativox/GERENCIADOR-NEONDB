"""
Importa tabulacoes do relatorio de chamadas Fastway (CSV) para o banco de dados.
Interface grafica com log em tempo real.

Requisitos:
    python -m pip install psycopg2-binary requests

Uso:
    python importar_tabulacao.py
"""

import csv
import io
import time
import threading
from datetime import datetime

import requests
import psycopg2
import psycopg2.extras
import tkinter as tk
from tkinter import ttk

DATABASE_URL = "postgresql://neondb_owner:npg_FjPvo2f6dxRy@ep-mute-bird-ac4p4q9g-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"
API_URL = "https://mbfinance2.fastssl.com.br/api/relatorio/chamadas.php"
SERVICO = "62"
RESULTADO = "ELETRONICO"
BATCH_SIZE = 50_000


def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode='require', connect_timeout=15,
                            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)


class App:
    def __init__(self, root):
        self.root = root
        root.title("MB Finance - Importar Tabulacoes Fastway")
        root.geometry("750x580")
        root.configure(bg="#1c1917")

        style = ttk.Style(); style.theme_use("clam")
        style.configure("TFrame", background="#1c1917")
        style.configure("TLabel", background="#1c1917", foreground="#e7e5e4", font=("Segoe UI", 10))
        style.configure("Title.TLabel", background="#1c1917", foreground="#d97706", font=("Segoe UI", 14, "bold"))

        ttk.Label(root, text="  Importar Tabulacoes - Fastway", style="Title.TLabel").pack(anchor="w", padx=20, pady=(16,8))

        cfg = ttk.Frame(root); cfg.pack(fill="x", padx=20, pady=8)
        ttk.Label(cfg, text="Serviço:").grid(row=0, column=0, padx=(0,4))
        self.servico = ttk.Entry(cfg, width=6, font=("Segoe UI", 10))
        self.servico.insert(0, "62")
        self.servico.grid(row=0, column=1, padx=(0,10))
        ttk.Label(cfg, text="Resultado:").grid(row=0, column=2, padx=(0,4))
        self.resultado = ttk.Entry(cfg, width=14, font=("Segoe UI", 10))
        self.resultado.insert(0, "ELETRONICO")
        self.resultado.grid(row=0, column=3, padx=(0,10))
        ttk.Label(cfg, text="De:").grid(row=0, column=4, padx=(0,4))
        self.data_ini = ttk.Entry(cfg, width=12, font=("Segoe UI", 10))
        self.data_ini.insert(0, datetime.now().strftime("%d/%m/%Y"))
        self.data_ini.grid(row=0, column=5, padx=(0,10))
        ttk.Label(cfg, text="Até:").grid(row=0, column=6, padx=(0,4))
        self.data_fim = ttk.Entry(cfg, width=12, font=("Segoe UI", 10))
        self.data_fim.insert(0, datetime.now().strftime("%d/%m/%Y"))
        self.data_fim.grid(row=0, column=7, padx=(0,10))
        self.btn = tk.Button(cfg, text="  Importar", bg="#b45309", fg="white", font=("Segoe UI", 10, "bold"),
                             relief="flat", padx=16, pady=4, cursor="hand2", command=self.iniciar)
        self.btn.grid(row=0, column=8, padx=(8,0))

        self.progress = ttk.Progressbar(root, mode="indeterminate", length=710)
        self.progress.pack(padx=20, pady=(8,4))

        sf = ttk.Frame(root); sf.pack(fill="x", padx=20, pady=4)
        self.stats = {}
        for i, (k, l) in enumerate([("registros","Registros"),("inseridos","Inseridos"),("atualizados","Atualizados"),("custo","Custo total")]):
            f = tk.Frame(sf, bg="#292524", highlightbackground="#44403c", highlightthickness=1, padx=12, pady=6)
            f.grid(row=0, column=i, padx=4, sticky="ew"); sf.columnconfigure(i, weight=1)
            tk.Label(f, text=l, bg="#292524", fg="#78716c", font=("Segoe UI", 9)).pack()
            v = tk.Label(f, text="-", bg="#292524", fg="#fbbf24", font=("Segoe UI", 16, "bold")); v.pack()
            self.stats[k] = v

        lf = ttk.Frame(root); lf.pack(fill="both", expand=True, padx=20, pady=(8,16))
        self.log = tk.Text(lf, bg="#0c0a09", fg="#a8a29e", font=("Consolas", 10), relief="flat", wrap="word", state="disabled", padx=10, pady=8)
        sc = ttk.Scrollbar(lf, command=self.log.yview); self.log.configure(yscrollcommand=sc.set)
        sc.pack(side="right", fill="y"); self.log.pack(fill="both", expand=True)
        self.log.tag_configure("ok", foreground="#4ade80")
        self.log.tag_configure("warn", foreground="#fbbf24")
        self.log.tag_configure("err", foreground="#f87171")
        self.log.tag_configure("info", foreground="#60a5fa")
        self.log.tag_configure("head", foreground="#d97706", font=("Consolas", 10, "bold"))
        self.running = False

    def _log(self, msg, tag=""):
        self.root.after(0, lambda: [self.log.configure(state="normal"), self.log.insert("end", msg+"\n", tag), self.log.see("end"), self.log.configure(state="disabled")])

    def _stat(self, key, val):
        self.root.after(0, lambda: self.stats[key].configure(text=val))

    def iniciar(self):
        if self.running: return
        self.running = True
        self.btn.configure(state="disabled", text="Processando...")
        self.progress.start(12)
        self.log.configure(state="normal"); self.log.delete("1.0","end"); self.log.configure(state="disabled")
        for k in self.stats: self._stat(k, "-")
        threading.Thread(target=self._executar, daemon=True).start()

    def _executar(self):
        try:
            di, df = self.data_ini.get().strip(), self.data_fim.get().strip()
            srv = self.servico.get().strip() or "62"
            res = self.resultado.get().strip()
            self._log("=" * 55, "head")
            self._log(f"  IMPORTAR TABULACOES - Servico {srv} - {di} a {df}", "head")
            if res: self._log(f"  Resultado: {res}", "head")
            self._log("=" * 55, "head")
            self._log("")
            self._log(f"Baixando CSV da API Fastway (servico {srv})...", "info")
            t0 = time.time()
            params_req = {"servico":srv,"data_inicial":di,"data_final":df,"formato":"csv","status":""}
            if res: params_req["resultado"] = res
            resp = requests.get(API_URL, params=params_req, timeout=300)
            resp.raise_for_status()
            conteudo = resp.text
            self._log(f"   {len(conteudo)/1024/1024:.1f} MB | {conteudo.count(chr(10)):,} linhas | {time.time()-t0:.1f}s", "ok")
            self._log("")
            self._log("Parseando CSV...", "info")
            reader = csv.DictReader(io.StringIO(conteudo), delimiter=';')
            self._log(f"   Colunas: {', '.join(reader.fieldnames or [])}")
            todas = list(reader)
            self._log(f"   Linhas de dados: {len(todas)}", "info")
            # Debug: mostra todas se < 20, senão primeiras 3
            debug_linhas = todas if len(todas) < 20 else todas[:3]
            for idx, row in enumerate(debug_linhas):
                campos = [f"{k}={v.strip()[:50]}" for k,v in row.items() if v and v.strip()]
                self._log(f"   [{idx+1}]", "warn")
                for c in campos:
                    self._log(f"      {c}", "warn")
            registros, erros = [], 0
            for row in todas:
                try:
                    # CNPJ: tenta 'cpf' primeiro, depois 'destino'
                    cnpj_raw = (row.get('cpf','') or '').strip()
                    if not cnpj_raw: cnpj_raw = (row.get('destino','') or '').strip()
                    cnpj = ''.join(c for c in cnpj_raw if c.isdigit())
                    if not cnpj: continue
                    # Tabulação: tenta múltiplas colunas
                    tab = (row.get('cdr_disposition','') or '').strip()
                    if not tab: tab = (row.get('tabulacao_nome','') or '').strip()
                    if not tab: tab = (row.get('nome_grupo_tabulacao','') or '').strip()
                    if not tab: tab = (row.get('resultado','') or '').strip()
                    if not tab: continue
                    # Telefone: tenta 'destino', 'fone_cliente', 'origem' — remove 0 da frente
                    tel_raw = (row.get('destino','') or '').strip()
                    if not tel_raw: tel_raw = (row.get('fone_cliente','') or '').strip()
                    if not tel_raw: tel_raw = (row.get('origem','') or '').strip()
                    tel = ''.join(c for c in tel_raw if c.isdigit()).lstrip('0')
                    cr = (row.get('custo_ligacao','0') or '0').strip()
                    try: custo = float(cr)
                    except: custo = 0.0
                    # Data/hora real da ligação (do CSV)
                    data_raw = (row.get('data','') or '').strip()
                    hora_raw = (row.get('hora','') or '').strip()
                    data_hora = None
                    if data_raw:
                        try:
                            if hora_raw:
                                data_hora = datetime.strptime(f"{data_raw} {hora_raw}", "%d/%m/%Y %H:%M:%S")
                            else:
                                data_hora = datetime.strptime(data_raw, "%d/%m/%Y")
                        except: pass
                    registros.append({'cnpj':cnpj,'telefone':tel,'tabulacao':tab,'custo':custo,'data_hora':data_hora})
                except: erros += 1
            self._log(f"   {len(registros):,} registros | {erros} erros", "ok")
            self._stat("registros", f"{len(registros):,}")
            if not registros: self._log("\nNenhum registro.", "warn"); return

            contagem, custo_total = {}, 0.0
            for r in registros:
                contagem[r['tabulacao']] = contagem.get(r['tabulacao'],0)+1
                custo_total += r['custo']
            self._log(f"\n{'-'*55}")
            self._log("RESUMO", "head")
            for tab, qtd in sorted(contagem.items(), key=lambda x:-x[1]): self._log(f"   {tab}: {qtd:,}")
            self._log(f"   CNPJs unicos: {len(set(r['cnpj'] for r in registros)):,}")
            self._log(f"   Custo total: R$ {custo_total:,.4f}")
            self._log(f"{'-'*55}\n")
            self._stat("custo", f"R$ {custo_total:,.2f}")

            total = len(registros)
            tl = (total+BATCH_SIZE-1)//BATCH_SIZE
            ins = 0

            # Detecta datas no lote para dedup
            datas_no_lote = set()
            for r in registros:
                if r['data_hora']:
                    datas_no_lote.add(r['data_hora'].strftime('%Y-%m-%d'))
            if datas_no_lote:
                data_min = min(datas_no_lote)
                data_max = max(datas_no_lote)
                self._log(f"🔍 Verificando duplicatas ({data_min} a {data_max})...", "info")
                try:
                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM historico_chamadas WHERE data_hora::date BETWEEN %s AND %s", (data_min, data_max))
                    existentes = cur.fetchone()[0]
                    if existentes > 0:
                        self._log(f"   ⚠ {existentes:,} registros já existem nesse período — removendo para reimportar...", "warn")
                        cur.execute("DELETE FROM historico_chamadas WHERE data_hora::date BETWEEN %s AND %s", (data_min, data_max))
                        conn.commit()
                        self._log(f"   🗑 {cur.rowcount:,} registros removidos", "ok")
                    else:
                        self._log(f"   ✅ Nenhuma duplicata — período limpo", "ok")
                    cur.close(); conn.close()
                except Exception as e:
                    self._log(f"   ⚠ Erro ao verificar duplicatas: {e}", "warn")

            self._log(f"Inserindo no historico_chamadas ({tl} lotes)...", "info")
            for i in range(0, total, BATCH_SIZE):
                lote = registros[i:i+BATCH_SIZE]; n = i//BATCH_SIZE+1
                for att in range(1,4):
                    try:
                        conn = get_conn(); cur = conn.cursor()
                        psycopg2.extras.execute_values(cur, "INSERT INTO historico_chamadas (cnpj,telefone,tabulacao,custo_ligacao,data_hora) VALUES %s",
                            [(r['cnpj'],r['telefone'],r['tabulacao'],r['custo'],r['data_hora'] or datetime.now()) for r in lote], template="(%s,%s,%s,%s,%s)")
                        conn.commit(); ins += len(lote); cur.close(); conn.close()
                        self._log(f"   Lote {n}/{tl}: {len(lote):,} inseridos (total: {ins:,})", "ok")
                        self._stat("inseridos", f"{ins:,}"); break
                    except Exception as e:
                        try: conn.close()
                        except: pass
                        if att==3: self._log(f"   Lote {n} falhou: {e}", "err")
                        else: self._log(f"   Tentativa {att}/3: {e}", "warn"); time.sleep(3)

            self._log("")
            self._log("Atualizando contadores via SQL direto...", "info")
            atl = 0
            for att in range(1, 4):
                try:
                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("""
                        WITH contagem AS (
                            SELECT
                                cnpj,
                                SUM(CASE WHEN tabulacao = 'NAO RESPONDEU MENU ABORDAGEM' THEN 1 ELSE 0 END) AS nao,
                                SUM(CASE WHEN tabulacao = 'DESLIGOU NA APRESENTACAO' THEN 1 ELSE 0 END) AS apr,
                                SUM(CASE WHEN tabulacao = 'DESLIGOU NO MEU ABORDAGEM' THEN 1 ELSE 0 END) AS menu,
                                SUM(CASE WHEN tabulacao = 'ELETRONICO' THEN 1 ELSE 0 END) AS elet,
                                SUM(CASE WHEN tabulacao = 'NAO ATENDE' THEN 1 ELSE 0 END) AS natende
                            FROM historico_chamadas
                            GROUP BY cnpj
                        )
                        UPDATE empresas e SET
                            nao_respondeu_menu = c.nao,
                            desligou_apresentacao = c.apr,
                            desligou_menu = c.menu,
                            sem_contato_eletronico = c.elet,
                            sem_contato_nao_atende = c.natende
                        FROM contagem c
                        WHERE e.cnpj = c.cnpj
                    """)
                    atl = cur.rowcount
                    conn.commit(); cur.close(); conn.close()
                    self._log(f"   {atl:,} CNPJs atualizados em uma query", "ok")
                    self._stat("atualizados", f"{atl:,}")
                    break
                except Exception as e:
                    try: conn.close()
                    except: pass
                    if att == 3: self._log(f"   Falhou após 3 tentativas: {e}", "err")
                    else: self._log(f"   Tentativa {att}/3: {e}", "warn"); time.sleep(5)

            self._log(f"\n{'='*55}", "head")
            self._log("CONCLUIDO!", "head")
            self._log(f"   Historico: {ins:,} inseridos", "ok")
            self._log(f"   Contadores: {atl:,} CNPJs", "ok")
            self._log(f"   Custo: R$ {custo_total:,.4f}", "ok")
            self._log(f"{'='*55}", "head")
        except Exception as e:
            self._log(f"\nERRO: {e}", "err")
        finally:
            self.running = False
            self.root.after(0, lambda: [self.btn.configure(state="normal", text="  Importar"), self.progress.stop()])


if __name__ == '__main__':
    root = tk.Tk()
    App(root)
    root.mainloop()