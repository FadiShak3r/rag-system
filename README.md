# MySQL RAG System

Ein RAG-System (Retrieval-Augmented Generation) zum Abfragen von MySQL-Datenbanktabellen mit OpenAI-Embeddings und GPT-Modellen.

## Funktionen

- Automatische Datenextraktion aus MySQL-Tabellen
- OpenAI-Embeddings für Vektordarstellungen
- ChromaDB für schnelle Ähnlichkeitssuche
- Web-Chatbot-Interface zum Stellen von Fragen
- Tägliche Synchronisation

## Installation

### 1. Abhängigkeiten installieren

```bash
pip install -r requirements.txt
```

### 2. Konfiguration

Erstelle eine `.env` Datei im Projektverzeichnis:

```env
# MySQL Konfiguration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=dein_benutzername
MYSQL_PASSWORD=dein_passwort
MYSQL_DATABASE=deine_datenbank

# OpenAI Konfiguration
OPENAI_API_KEY=dein_openai_api_key

# ChromaDB Konfiguration (optional)
CHROMA_DB_PATH=./chroma_db
```

## Verwendung

### Datenbank indexieren

```bash
python indexer.py
```

Bestehende Daten löschen und neu indexieren:

```bash
python indexer.py --clear
```

### Chatbot starten

```bash
python app.py
```

Dann im Browser öffnen: `http://localhost:5000`

### Tägliche Synchronisation

Einmalig ausführen:

```bash
python daily_sync.py --run-once
```

## Projektstruktur

```
├── config.py          # Konfiguration
├── database.py        # MySQL-Verbindung
├── data_processor.py  # Datenverarbeitung
├── embedding.py       # Embedding-Generierung
├── vector_store.py    # Vektorspeicher
├── rag_system.py      # RAG-System
├── indexer.py         # Indexierung
├── app.py             # Flask-Chatbot
├── templates/         # HTML-Templates
└── requirements.txt   # Abhängigkeiten
```

## Fehlerbehebung

**MySQL-Verbindungsfehler**: Überprüfe die Zugangsdaten in `.env`

**OpenAI API-Fehler**: 
- API-Schlüssel in `.env` überprüfen
- Billing-Status prüfen: https://platform.openai.com/account/billing

**Keine Ergebnisse**: Stelle sicher, dass `python indexer.py` ausgeführt wurde
