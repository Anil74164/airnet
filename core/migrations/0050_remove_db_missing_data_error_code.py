# Generated by Django 5.0.6 on 2024-06-10 07:03

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0049_alter_db_manufacturer_access_id_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='db_missing_data',
            name='error_code',
        ),
    ]
