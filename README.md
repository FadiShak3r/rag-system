# SQL Server RAG System

Ein RAG-System (Retrieval-Augmented Generation) zum Abfragen von SQL Server-Datenbanktabellen mit OpenAI-Embeddings und GPT-Modellen.

## Funktionen

- Automatische Datenextraktion aus SQL Server-Tabellen
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
# SQL Server Konfiguration
SQL_SERVER=134.103.216.87
SQL_DATABASE=AdventureWorksDW2022
SQL_USER=SA
SQL_PASSWORD=MeinSicheresPW123!

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
├── database.py        # SQL Server-Verbindung
├── data_processor.py  # Datenverarbeitung
├── embedding.py       # Embedding-Generierung
├── vector_store.py    # Vektorspeicher
├── rag_system.py      # RAG-System
├── indexer.py         # Indexierung
├── app.py             # Flask-Chatbot
├── templates/         # HTML-Templates
└── requirements.txt   # Abhängigkeiten
```

## Unterstützte Tabellen

Das System indexiert aktuell die folgende Tabelle:
- `dbo.dimProduct` - Produktdimensionstabelle mit vollständigen Produktinformationen

## Fehlerbehebung

**SQL Server-Verbindungsfehler**: 
- Überprüfe die Zugangsdaten in `.env`
- Stelle sicher, dass der ODBC Driver 18 for SQL Server installiert ist
- Überprüfe, ob der SQL Server erreichbar ist und die Firewall-Regeln korrekt sind

**OpenAI API-Fehler**: 
- API-Schlüssel in `.env` überprüfen
- Billing-Status prüfen: https://platform.openai.com/account/billing

**Keine Ergebnisse**: Stelle sicher, dass `python indexer.py` ausgeführt wurde
