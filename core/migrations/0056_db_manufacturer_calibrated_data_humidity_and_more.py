# Generated by Django 5.0.6 on 2024-06-10 19:45

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0055_rename_date_time_db_manufacturer_calibrated_data_time_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='db_manufacturer_calibrated_data',
            name='humidity',
            field=models.FloatField(null=True),
        ),
        migrations.AddField(
            model_name='db_manufacturer_calibrated_data',
            name='temperature',
            field=models.FloatField(null=True),
        ),
    ]