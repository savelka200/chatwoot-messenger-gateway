.PHONY: help build run stop logs shell test clean

# По умолчанию показываем справку
help:
	@echo "Доступные команды:"
	@echo "  make build       - Собрать Docker образ"
	@echo "  make run         - Запустить контейнер"
	@echo "  make stop        - Остановить контейнер"
	@echo "  make logs        - Показать логи"
	@echo "  make shell       - Войти в контейнер"
	@echo "  make test        - Запустить тесты"
	@echo "  make clean       - Удалить контейнеры и volumes"
	@echo "  make multi       - Запустить несколько экземпляров"

# Сборка образа
build:
	docker build -t chatwoot-gateway:latest .

# Запуск (один экземпляр)
run:
	docker-compose up -d

# Запуск нескольких экземпляров
multi:
	docker-compose --profile multi-instance up -d

# Остановка
stop:
	docker-compose down

# Логи
logs:
	docker-compose logs -f

# Shell в контейнере
shell:
	docker-compose exec chatwoot-gateway /bin/bash

# Тесты
test:
	docker-compose run --rm chatwoot-gateway pytest

# Очистка
clean:
	docker-compose down -v
	docker system prune -f

# Проверка health
health:
	curl -s http://localhost:8000/health | jq .

# Перезапуск
restart: stop run

# Обновление (пересборка + перезапуск)
update: build restart