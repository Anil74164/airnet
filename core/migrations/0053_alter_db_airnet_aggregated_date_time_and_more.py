# Generated by Django 5.0.6 on 2024-06-10 13:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0052_alter_db_std_data_status'),
    ]

    operations = [
        migrations.AlterField(
            model_name='db_airnet_aggregated',
            name='date_time',
            field=models.DateTimeField(null=True),
        ),
        migrations.AlterField(
            model_name='db_airnet_aggregated',
            name='status',
            field=models.IntegerField(default=0),
        ),
    ]