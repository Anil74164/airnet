# Generated by Django 5.0.4 on 2024-05-24 11:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('RawData', '0004_db_airnet_raw_reponse_delete_db_aqdata_raw'),
    ]

    operations = [
        migrations.CreateModel(
            name='db_AirNet_Raw',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('timestamp', models.DateTimeField(auto_now_add=True)),
                ('manufacturer', models.CharField(max_length=100)),
                ('data_raw', models.JSONField()),
                ('data_cal', models.JSONField()),
                ('processed_dt', models.DateTimeField(null=True)),
            ],
        ),
    ]
