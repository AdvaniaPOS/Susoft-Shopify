# 🚀 Deployment Guide - Linux Server

## Forutsetninger

- Linux server med Docker installert
- Domene/IP som er tilgjengelig fra internett (for Shopify webhooks)
- Port 8000 åpen (eller bruk reverse proxy)

## Steg 1: Installer Docker

```bash
# Installer Docker
curl -fsSL https://get.docker.com | sh

# Start Docker ved boot
sudo systemctl enable docker

# Legg til bruker i docker-gruppen
sudo usermod -aG docker $USER
# Logg ut og inn igjen
```

## Steg 2: Klon prosjektet

```bash
# Fra GitHub
git clone https://github.com/AdvaniaPOS/Susoft-Shopify.git
cd Susoft-Shopify

# Eller kopier filene manuelt
scp -r susoft-shopify-sync/ user@server:/home/user/
```

## Steg 3: Kopier tenant-konfigurasjon

```bash
# Kopier tenants.json fra utviklingsmaskinen
scp tenants.json user@server:/home/user/Susoft-Shopify/
```

## Steg 4: Deploy

```bash
# Gjør deploy-scriptet kjørbart
chmod +x deploy.sh

# Kjør deployment
./deploy.sh
```

Scriptet vil:
- Generere sikre nøkler automatisk
- Bygge Docker images
- Starte alle tjenester
- Kjøre database-migrasjoner

## Steg 5: Verifiser

```bash
# Sjekk at tjenestene kjører
docker compose -f docker-compose.prod.yml ps

# Sjekk logger
docker compose -f docker-compose.prod.yml logs -f api

# Test API
curl http://localhost:8000/health
```

## Steg 6: Sett opp HTTPS (anbefalt)

### Med Caddy (enklest)

```bash
# Installer Caddy
sudo apt install -y caddy

# Konfigurer reverse proxy
sudo tee /etc/caddy/Caddyfile << EOF
susoft.dittdomene.no {
    reverse_proxy localhost:8000
}
EOF

# Restart Caddy
sudo systemctl restart caddy
```

Caddy håndterer HTTPS automatisk med Let's Encrypt.

### Med Nginx + Certbot

```bash
# Installer
sudo apt install nginx certbot python3-certbot-nginx

# Nginx config
sudo tee /etc/nginx/sites-available/susoft << EOF
server {
    server_name susoft.dittdomene.no;
    
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/susoft /etc/nginx/sites-enabled/
sudo certbot --nginx -d susoft.dittdomene.no
```

## Steg 7: Konfigurer Shopify Webhooks

Gå til Shopify Admin → Settings → Notifications → Webhooks:

1. **Order creation** → `https://susoft.dittdomene.no/webhooks/shopify/orders/create`
2. **Order payment** → `https://susoft.dittdomene.no/webhooks/shopify/orders/paid`

Webhook format: JSON

## Nyttige kommandoer

```bash
# Se logger
docker compose -f docker-compose.prod.yml logs -f

# Restart alt
docker compose -f docker-compose.prod.yml restart

# Stopp alt
docker compose -f docker-compose.prod.yml down

# Oppdater (etter git pull)
docker compose -f docker-compose.prod.yml build
docker compose -f docker-compose.prod.yml up -d

# Sjekk synkroniseringsstatus
curl http://localhost:8000/api/v1/tenants/advania-jonb/status

# Manuell synkronisering
curl -X POST http://localhost:8000/api/v1/sync/products/advania-jonb

# Database backup
docker compose -f docker-compose.prod.yml exec db pg_dump -U postgres susoft_shopify > backup.sql
```

## Feilsøking

### API svarer ikke
```bash
docker compose -f docker-compose.prod.yml logs api
docker compose -f docker-compose.prod.yml restart api
```

### Database-problemer
```bash
docker compose -f docker-compose.prod.yml logs db
docker compose -f docker-compose.prod.yml restart db
```

### Worker henger
```bash
docker compose -f docker-compose.prod.yml logs worker
docker compose -f docker-compose.prod.yml restart worker
```

## Overvåking

API'et har innebygde endepunkter:
- `/health` - Helsesjekk
- `/metrics` - Prometheus-metrikker (hvis aktivert)
- `/docs` - API-dokumentasjon

## Sikkerhet

1. **Aldri del `.env`-filen** - den inneholder sensitive nøkler
2. **Bruk HTTPS** - påkrevd for Shopify webhooks
3. **Begrens tilgang** - bruk firewall for å begrense direkte tilgang til port 8000
4. **Logg-rotasjon** - sett opp log-rotasjon for Docker-logger
