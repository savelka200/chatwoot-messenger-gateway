#!/bin/bash
set -e

echo "🔐 Installing Russian Trusted CA certificates for MAX..."

# Скачиваем сертификаты Минцифры
cd /tmp

# Корневой сертификат
curl -fsSL -o russian_trusted_root_ca.crt \
    "https://gu-st.ru/content/lending/russian_trusted_root_ca_pem.crt" || {
    echo "❌ Failed to download Russian Trusted Root CA"
    exit 1
}

# Промежуточный сертификат
curl -fsSL -o russian_trusted_sub_ca.crt \
    "https://gu-st.ru/content/lending/russian_trusted_sub_ca_pem.crt" || {
    echo "❌ Failed to download Russian Trusted Sub CA"
    exit 1
}

# Проверяем валидность сертификатов
openssl x509 -in russian_trusted_root_ca.crt -noout -subject > /dev/null 2>&1 || {
    echo "❌ Invalid root certificate"
    exit 1
}

openssl x509 -in russian_trusted_sub_ca.crt -noout -subject > /dev/null 2>&1 || {
    echo "❌ Invalid sub certificate"
    exit 1
}

echo "✅ Certificates downloaded and validated"

# Добавляем сертификаты в certifi bundle
CERTIFI_PATH=$(python -c "import certifi; print(certifi.where())")
echo "📍 Certifi bundle path: $CERTIFI_PATH"

# Добавляем с правильными разделителями
echo "" >> "$CERTIFI_PATH"
echo "# Russian Trusted Root CA (added by install-certs.sh)" >> "$CERTIFI_PATH"
cat russian_trusted_root_ca.crt >> "$CERTIFI_PATH"
echo "" >> "$CERTIFI_PATH"
echo "# Russian Trusted Sub CA (added by install-certs.sh)" >> "$CERTIFI_PATH"
cat russian_trusted_sub_ca.crt >> "$CERTIFI_PATH"
echo "" >> "$CERTIFI_PATH"

# Проверяем, что bundle всё ещё читается
python -c "import ssl; ssl.create_default_context(cafile='$CERTIFI_PATH')" || {
    echo "❌ Certifi bundle is broken after adding certificates"
    exit 1
}

echo "✅ Certificates successfully added to certifi bundle"

# Также устанавливаем в систему (для openssl и других утилит)
cp russian_trusted_root_ca.crt /usr/local/share/ca-certificates/russian_trusted_root_ca.crt
cp russian_trusted_sub_ca.crt /usr/local/share/ca-certificates/russian_trusted_sub_ca.crt
update-ca-certificates

echo "✅ System CA certificates updated"

# Проверяем подключение к MAX API
python -c "
import httpx
try:
    resp = httpx.get('https://platform-api2.max.ru/me', timeout=5)
    print(f'✅ MAX API reachable: status={resp.status_code}')
except Exception as e:
    print(f'⚠️ MAX API check failed: {e}')
    print('This is OK if running in isolated network')
"

# Очищаем временные файлы
rm -f russian_trusted_*.crt

echo "🎉 Certificate installation complete!"