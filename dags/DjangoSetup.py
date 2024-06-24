import os
import sys
import django


django_project_directory = "."
sys.path.append(django_project_directory)

os.environ.setdefault("DJANGO_SETTINGS_MODULE","aqdms.settings")
django.setup()
