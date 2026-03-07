.PHONY: run test lint format install deploy backup

install:
	pip install -r requirements.txt

run:
	python bot.py

test:
	pytest tests/ -v

lint:
	flake8 . --max-line-length=120 --exclude=.venv
	mypy . --ignore-missing-imports

format:
	black . --line-length=120
	isort .

deploy:
	rsync -avz --exclude='.venv' --exclude='data/*.db' --exclude='.env' --exclude='__pycache__' \
		. ubuntu@$(SERVER):/home/ubuntu/f1-fantasy-bot/
	ssh ubuntu@$(SERVER) 'cd /home/ubuntu/f1-fantasy-bot && source .venv/bin/activate && pip install -r requirements.txt && sudo systemctl restart f1bot'

backup:
	bash deployment/backup.sh
