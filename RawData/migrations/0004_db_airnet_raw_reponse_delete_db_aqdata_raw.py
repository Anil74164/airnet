# Generated by Django 5.0.4 on 2024-05-24 11:15

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('RawData', '0003_db_aqdata_raw_processed_dt'),
    ]

    operations = [
        migrations.CreateModel(
            name='db_AirNet_Raw_Reponse',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('timestamp', models.DateTimeField(auto_now_add=True)),
                ('manufacturer', models.CharField(max_length=100)),
                ('request_url', models.TextField()),
                ('data', models.JSONField()),
                ('httpCode', models.CharField(max_length=255, null=True)),
                ('isCalibrated', models.BooleanField(default=False)),
                ('processed_dt', models.DateTimeField(null=True)),
                ('pollutant', models.CharField(max_length=100, null=True)),
            ],
        ),
        migrations.DeleteModel(
            name='db_AQData_Raw',
        ),
    ]
