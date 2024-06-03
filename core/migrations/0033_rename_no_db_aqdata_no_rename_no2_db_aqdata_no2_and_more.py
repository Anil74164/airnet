# Generated by Django 5.0.4 on 2024-05-28 09:53

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0032_rename_co_db_aqdata_co_rename_co2_db_aqdata_co2_and_more'),
    ]

    operations = [
        migrations.RenameField(
            model_name='db_aqdata',
            old_name='no',
            new_name='NO',
        ),
        migrations.RenameField(
            model_name='db_aqdata',
            old_name='no2',
            new_name='NO2',
        ),
        migrations.RenameField(
            model_name='db_aqdata',
            old_name='o3',
            new_name='O3',
        ),
        migrations.RenameField(
            model_name='db_aqdata',
            old_name='pm1',
            new_name='PM1',
        ),
        migrations.RenameField(
            model_name='db_aqdata',
            old_name='pm10',
            new_name='PM10',
        ),
        migrations.RenameField(
            model_name='db_aqdata',
            old_name='pm2_5',
            new_name='PM2_5',
        ),
        migrations.RenameField(
            model_name='db_aqdata',
            old_name='so2',
            new_name='SO2',
        ),
    ]
