# Generated by Django 5.0.6 on 2024-06-21 09:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0062_db_audit_trail'),
    ]

    operations = [
        migrations.AddField(
            model_name='db_device',
            name='is_reference_grade',
            field=models.BooleanField(default=False),
        ),
    ]