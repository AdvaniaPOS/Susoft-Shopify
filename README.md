# Susoft-Shopify Sync

Multi-tenant integrasjonsløsning for synkronisering mellom Susoft ERP/POS og Shopify.

## 🎯 Funksjonalitet

- **Ordresynkronisering**: Shopify-ordre → Susoft
- **Lagersynkronisering**: Susoft lager → Shopify (Susoft er lagermaster)
- **Multi-tenant**: Støtter flere kunder på samme instans
- **Pålitelig**: Købasert prosessering med retry og dead letter queue
- **Varsling**: Slack/Telegram-varsler ved feil (etter 5 min terskel)
- **Admin Dashboard**: Overvåking av alle integrasjoner

## 🏗️ Arkitektur

```
┌─────────────┐     ┌─────────────────────┐     ┌─────────────┐
│   Shopify   │────▶│  Susoft-Shopify     │────▶│   Susoft    │
│  (Webhooks) │     │      Sync API       │     │  (ERP/POS)  │
└─────────────┘     └─────────────────────┘     └─────────────┘
                              │
                    ┌─────────┴──────────┐
                    │                    │
              ┌─────▼─────┐       ┌──────▼─────┐
              │  Celery   │       │ PostgreSQL │
              │  Workers  │       │  Database  │
              └─────┬─────┘       └────────────┘
                    │
              ┌─────▼─────┐
              │   Redis   │
              │   Queue   │
              └───────────┘
```

### Dataflyt

1. **Ordre (Shopify → Susoft)**
   - Shopify sender `orders/create` webhook
   - Verifiseres med HMAC-SHA256
   - Legges i Celery-kø for prosessering
   - Ordre mappes via SKU og opprettes i Susoft
   - `alternativeId = "SHOPIFY-{order_id}"` for idempotens

2. **Lager (Susoft → Shopify)**
   - Susoft sender `ON_PRODUCT_STOCK_CHANGED` webhook
   - Verifiseres med Bearer token
   - Produktet mappes via SKU eller Susoft product UUID
   - Tilgjengelig mengde = lager - safety stock
   - Shopify inventory oppdateres via Admin API

## 📦 Installasjon

### Forutsetninger

- Python 3.12+
- PostgreSQL 15+
- Redis 7+
- Docker & Docker Compose (anbefalt)

### Med Docker (anbefalt)

```bash
# Klon repo
git clone <repo-url>
cd susoft-shopify-sync

# Kopier og konfigurer miljøvariabler
cp .env.example .env
# Rediger .env med dine verdier

# Start alle tjenester
docker-compose up -d

# Kjør database-migrasjoner
docker-compose exec api alembic upgrade head
```

### Uten Docker

```bash
# Opprett virtuelt miljø
python -m venv venv
source venv/bin/activate  # Linux/Mac
# eller: venv\Scripts\activate  # Windows

# Installer avhengigheter
pip install -r requirements.txt

# Konfigurer miljøvariabler
cp .env.example .env
# Rediger .env

# Start PostgreSQL og Redis (lokal installasjon kreves)

# Kjør migrasjoner
alembic upgrade head

# Start API
uvicorn app.main:app --reload

# Start worker (ny terminal)
celery -A app.workers.celery_app worker --loglevel=info

# Start beat scheduler (ny terminal)
celery -A app.workers.celery_app beat --loglevel=info
```

## ⚙️ Konfigurasjon

### Miljøvariabler

Se `.env.example` for alle tilgjengelige variabler. De viktigste:

| Variabel | Beskrivelse |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `ENCRYPTION_KEY` | Fernet-nøkkel for kryptering av credentials |
| `ADMIN_API_KEY` | API-nøkkel for admin-endepunkter |
| `SLACK_WEBHOOK_URL` | Webhook URL for Slack-varsler |
| `TELEGRAM_BOT_TOKEN` | Bot token for Telegram-varsler |

### Generere krypteringsnøkkel

```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

## 🔌 API Endepunkter

### Webhooks

| Metode | Endepunkt | Beskrivelse |
|--------|-----------|-------------|
| POST | `/webhooks/shopify/orders/create` | Mottar nye Shopify-ordre |
| POST | `/webhooks/shopify/orders/updated` | Mottar ordreoppdateringer |
| POST | `/webhooks/shopify/refunds/create` | Mottar refusjoner (logges) |
| POST | `/webhooks/susoft/{tenant_id}/stock-changed` | Mottar lagerendringer |
| POST | `/webhooks/susoft/{tenant_id}/order-created` | Mottar Susoft-ordre (logges) |

### Admin API

Krever `X-Admin-Api-Key` header.

| Metode | Endepunkt | Beskrivelse |
|--------|-----------|-------------|
| POST | `/admin/tenants` | Opprett ny tenant |
| GET | `/admin/tenants` | List alle tenants |
| GET | `/admin/tenants/{id}` | Hent tenant |
| GET | `/admin/tenants/{id}/status` | Hent tenant-status |
| POST | `/admin/tenants/{id}/mappings` | Opprett produktmapping |
| GET | `/admin/tenants/{id}/mappings` | List produktmappinger |
| POST | `/admin/tenants/{id}/sync-stock` | Trigger manuell lagersynk |
| GET | `/admin/tenants/{id}/sync-logs` | Hent synkroniseringslogg |
| GET | `/admin/dlq` | List dead letter queue |
| POST | `/admin/dlq/{id}/retry` | Prøv på nytt fra DLQ |
| GET | `/admin/dashboard` | Dashboard-statistikk |

## 🚀 Oppsett av ny kunde (tenant)

### 1. Opprett tenant via Admin API

```bash
curl -X POST http://localhost:8000/admin/tenants \
  -H "Content-Type: application/json" \
  -H "X-Admin-Api-Key: your-admin-key" \
  -d '{
    "name": "Min Butikk",
    "susoft_api_url": "https://kunde.susoft.no/api/v3",
    "susoft_api_key": "susoft-api-key",
    "susoft_integration_id": "int-123",
    "susoft_webhook_secret": "webhook-secret",
    "shopify_shop_url": "minbutikk.myshopify.com",
    "shopify_access_token": "shpat_xxxxx",
    "shopify_default_location_id": "12345678",
    "safety_stock_default": 2
  }'
```

### 2. Opprett produktmappinger

```bash
curl -X POST http://localhost:8000/admin/tenants/{tenant_id}/mappings \
  -H "Content-Type: application/json" \
  -H "X-Admin-Api-Key: your-admin-key" \
  -d '{
    "sku": "PROD-001",
    "susoft_product_id": "uuid-fra-susoft",
    "shopify_product_id": "123456",
    "shopify_variant_id": "789012",
    "shopify_inventory_item_id": "345678",
    "safety_stock": 5
  }'
```

### 3. Konfigurer webhooks i Shopify

Gå til Shopify Admin → Settings → Notifications → Webhooks:

- **orders/create**: `https://your-domain.com/webhooks/shopify/orders/create`
- **orders/updated**: `https://your-domain.com/webhooks/shopify/orders/updated`
- **refunds/create**: `https://your-domain.com/webhooks/shopify/refunds/create`

### 4. Konfigurer webhooks i Susoft

Bruk Susoft Integration API for å registrere webhooks:

- **ON_PRODUCT_STOCK_CHANGED**: `https://your-domain.com/webhooks/susoft/{tenant_id}/stock-changed`
- **ON_ORDER_CREATED**: `https://your-domain.com/webhooks/susoft/{tenant_id}/order-created`

## 📊 Overvåking

### Flower (Celery dashboard)

Tilgjengelig på `http://localhost:5555` - viser køstatus, aktive tasks og workers.

### Admin Dashboard

```bash
curl http://localhost:8000/admin/dashboard \
  -H "X-Admin-Api-Key: your-admin-key"
```

Returnerer:
- Antall tenants (totalt, aktive, friske)
- DLQ-status
- Dagens synkroniseringer og feilrate

### Varsler

Konfigurer `SLACK_WEBHOOK_URL` og/eller `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` for å motta varsler ved:

- Dead letter queue-elementer
- Tasks som står i kø >5 minutter
- Tenants som ikke har synket på >30 minutter

## 🔒 Sikkerhet

- Alle API-credentials lagres kryptert med Fernet
- Shopify webhooks verifiseres med HMAC-SHA256
- Susoft webhooks verifiseres med Bearer token
- Admin API krever API-nøkkel
- Alle passord/secrets eksponeres aldri i logger

## 🧪 Testing

```bash
# Kjør tester
pytest

# Med coverage
pytest --cov=app tests/

# Kun enhetstester
pytest tests/unit/

# Kun integrasjonstester (krever Docker)
pytest tests/integration/
```

## 📁 Prosjektstruktur

```
susoft-shopify-sync/
├── app/
│   ├── api/              # FastAPI routers
│   │   ├── admin.py      # Admin API
│   │   └── webhooks.py   # Webhook endpoints
│   ├── core/             # Core modules
│   │   ├── config.py     # Settings
│   │   ├── database.py   # Database setup
│   │   └── security.py   # Encryption & auth
│   ├── db/               # Database layer
│   │   ├── models.py     # SQLAlchemy models
│   │   └── repositories.py
│   ├── services/         # External API clients
│   │   ├── susoft_client.py
│   │   └── shopify_client.py
│   ├── workers/          # Celery tasks
│   │   ├── celery_app.py
│   │   ├── tasks.py
│   │   └── scheduled_tasks.py
│   ├── utils/            # Utilities
│   │   └── notifier.py
│   └── main.py           # FastAPI app
├── alembic/              # Database migrations
├── tests/                # Test suite
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## 📝 Lisens

Proprietær - Alle rettigheter forbeholdt.

## 🤝 Support

Kontakt: [din-epost@example.com]
