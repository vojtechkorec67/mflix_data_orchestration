# mflix_data_orchestration

[![CI](https://github.com/vojtechkorec67/mflix_data_orchestration/actions/workflows/ci.yml/badge.svg)](https://github.com/vojtechkorec67/mflix_data_orchestration/actions/workflows/ci.yml)

Krátký návod jak pracovat s projektem (uv + src layout).

## Požadavky
- Python 3.10+
- Git
- uv (Astral) (doporučeno) — správa projektů, venv a lockfile
- Přístupové údaje pro služby (Snowflake, MongoDB) jako env vars

## Struktura projektu
- `src/mflix_data_orchestration/` — zdrojový balíček (src layout)
- `pyproject.toml` — primární zdroj závislostí (pyproject, používané `uv`)
- `.github/workflows/ci.yml` — CI (instalace přes `uv`, testy, lint)
- `data/` — lokální výstupy (CSV/PNG) — Git ignoruje generované soubory

## Rychlý start (lokálně)
1. Klon repozitáře:
```bash
git clone <repo-url>
cd mflix_data_orchestration
```
2. Nainstalujte `uv` (pokud nemáte):
```bash
python -m pip install --upgrade pip
pipx install uv
# nebo (jednodušší): python -m pip install uv
```
3. Nainstalujte závislosti (doporučeno použít lockfile):
```bash
uv venv --python 3.10
uv sync
```
Pokud `uv.lock` chybí, spusťte `uv lock` (lokálně) a commitněte `uv.lock`.

## Spouštění a vývoj
- Spustit testy:
```bash
uv run pytest -q
```
- Vstoupit do virtuálního prostředí:
```bash
uv venv --python 3.10
source .venv/bin/activate
```
- Spustit `dagit`/jiné příkazy přes:
```bash
uv run dagit
```

## Přidání nebo aktualizace závislostí
- Přidat runtime dependency:
```bash
uv add <package>
```
- Přidat dev dependency:
```bash
uv add --dev <package>
```
Po změně dependency vždy commitujte `pyproject.toml` a `uv.lock`.

## Export pro Docker / starší tooling
Pokud potřebujete `requirements.txt` pro Docker nebo jiný systém:
```bash
# S uv (Astral) - doporučeno: vytvoří platformově nezávislý requirements
uv pip compile requirements.in --universal --output-file requirements.txt

# Alternativně (pokud stále používáte Poetry):
poetry export -f requirements.txt --output requirements.txt --without-hashes
```

## Použití `uv` pro správu projektu
Krátký návod, jak začít s `uv` (Astral) v tomto projektu:

1. Nainstalujte `uv` (doporučeno přes `pipx` nebo instalátor):
```bash
pipx install uv
# nebo
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Inicializujte `uv` projekt (volitelné):
```bash
uv init .
# nebo pojmenujte projekt: uv init myproject
```

3. Vytvořte/aktivujte virtuální prostředí s konkrétní verzí Pythonu (např. 3.10):
```bash
uv venv --python 3.10
# nebo pin verzi projektu: uv python pin 3.10
```

4. Přidejte závislosti a vytvořte lockfile:
```bash
uv add <package>
uv lock
```

5. Instalace podle locku:
```bash
uv sync
```

6. Export `requirements.txt` pro Docker/CI:
```bash
uv pip compile requirements.in --universal --output-file requirements.txt
# poté v Dockeru: uv pip sync requirements.txt
```

## Environment variables
Projekt používá externí služby — nastavte tyto proměnné před spuštěním:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- (další dle potřeby: Mongo connection string, atd.)

Doporučeno: vytvořit `.env` pro lokální vývoj a načíst jej (neukládejte do VCS).

## CI
 - Při `push`/PR GitHub Actions spustí instalaci přes `uv` a běh testů/lint.

## Poznámky
- Pokud narazíte na chyby při instalaci nativních závislostí (pyarrow, snowflake connector), ujistěte se, že máte systémové závislosti (např. `libarrow`, build tools) nebo použijte oficiální binary wheel v prostředí podobném CI.
 - Pokud chcete, mohu vygenerovat `uv.lock` lokálně a commitnout ho — dejte vědět a případně spusťte `uv lock` na svém stroji a pošlete `uv.lock`, nebo povolte, abych ho vytvořil bez lokálního běhu (méně doporučeno).

---
Pokud chcete, doplním README o konkrétní příklady `dagit` spuštění, nebo o tabulku potřebných env vars s popisy.
