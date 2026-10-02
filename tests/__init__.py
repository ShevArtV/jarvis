"""Запуск: ``python -m unittest discover -s tests -t .`` из корня репозитория.

Тесты не читают .env: иначе они проверяют не код, а окружение автора.
"""

import os

os.environ["JARVIS_DOTENV"] = "0"
