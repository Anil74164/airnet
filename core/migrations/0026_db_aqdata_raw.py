# Generated by Django 5.0.4 on 2024-05-21 11:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0025_delete_db_aqdata_raw'),
    ]

    operations = [
        migrations.CreateModel(
            name='db_AQData_Raw',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('data', models.JSONField()),
            ],
        ),
    ]
