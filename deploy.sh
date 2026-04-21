#!/bin/bash
# ==============================================
# Susoft-Shopify Sync - Production Deployment
# ==============================================
# 
# Kjør dette scriptet på Linux-serveren:
#   chmod +x deploy.sh
#   ./deploy.sh

set -e

echo "🚀 Susoft-Shopify Sync - Deployment"
echo "===================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker er ikke installert. Installer Docker først:"
    echo "   curl -fsSL https://get.docker.com | sh"
    exit 1
fi

# Check if Docker Compose is available
if ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose er ikke tilgjengelig."
    echo "   Oppdater Docker til nyeste versjon."
    exit 1
fi

echo "✅ Docker funnet: $(docker --version)"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo ""
    echo "📝 Oppretter .env fil..."
    
    # Generate secure keys
    SECRET_KEY=$(openssl rand -hex 32)
    # Fernet key = urlsafe-base64 of 32 random bytes
    ENCRYPTION_KEY=$(openssl rand 32 | base64 | tr '+/' '-_' | tr -d '\n')
    JWT_SECRET=$(openssl rand -hex 32)
    ADMIN_KEY=$(openssl rand -hex 16)
    ADMIN_PASS=$(openssl rand -hex 12)
    WEBHOOK_SECRET_VAL=$(openssl rand -hex 32)
    DB_PASSWORD=$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)
    
    cat > .env << EOF
# Production Environment - Generated $(date)
DB_PASSWORD=${DB_PASSWORD}
SECRET_KEY=${SECRET_KEY}
ENCRYPTION_KEY=${ENCRYPTION_KEY}
JWT_SECRET_KEY=${JWT_SECRET}
ADMIN_API_KEY=${ADMIN_KEY}
ADMIN_PASSWORD=${ADMIN_PASS}
WEBHOOK_SECRET=${WEBHOOK_SECRET_VAL}
WEBHOOK_BASE_URL=https://shopify.poshub.no
EOF
    
    echo "✅ .env opprettet med sikre nøkler"
    echo ""
    echo "⚠️  VIKTIG: Ta vare på disse verdiene!"
    echo "   ADMIN_API_KEY: ${ADMIN_KEY}"
    echo ""
else
    echo "✅ .env fil funnet"
fi

# Check if tenants.json exists
if [ ! -f tenants.json ]; then
    echo ""
    echo "⚠️  tenants.json mangler!"
    echo "   Kopier tenant-konfigurasjonen fra utviklingsmaskinen."
    exit 1
fi

echo ""
echo "🔧 Bygger Docker images..."
docker compose -f docker-compose.prod.yml build

echo ""
echo "🚀 Starter tjenester..."
docker compose -f docker-compose.prod.yml up -d

echo ""
echo "⏳ Venter på database..."
sleep 5

echo ""
echo "📊 Kjører database-migrasjoner..."
docker compose -f docker-compose.prod.yml exec -T api alembic upgrade head || true

echo ""
echo "✅ Deployment fullført!"
echo ""
echo "===================================="
echo "Tjenester kjører på:"
echo "  API:     http://localhost:8000"
echo "  Docs:    http://localhost:8000/docs"
echo "  Health:  http://localhost:8000/health"
echo ""
echo "Kommandoer:"
echo "  Se logger:     docker compose -f docker-compose.prod.yml logs -f"
echo "  Stop:          docker compose -f docker-compose.prod.yml down"
echo "  Restart:       docker compose -f docker-compose.prod.yml restart"
echo ""
echo "Webhook URL for Shopify:"
echo "  https://YOUR-DOMAIN:8000/webhooks/shopify/orders/create"
echo "===================================="
