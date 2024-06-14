# Generated by Django 5.0.6 on 2024-06-13 09:03

import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('RawData', '0008_rename_httpcode_db_airnet_raw_response_http_code_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='db_airnet_raw_response',
            name='device_id',
            field=models.CharField(default=django.utils.timezone.now, max_length=100),
            preserve_default=False,
        ),
    ]