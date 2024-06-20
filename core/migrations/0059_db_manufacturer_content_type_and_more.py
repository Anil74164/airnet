# Generated by Django 5.0.6 on 2024-06-18 07:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0058_db_missing_data_error_code_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='db_manufacturer',
            name='content_type',
            field=models.CharField(max_length=1000, null=True),
        ),
        migrations.AlterField(
            model_name='db_manufacturer',
            name='access_id',
            field=models.CharField(max_length=500, null=True),
        ),
        migrations.AlterField(
            model_name='db_manufacturer',
            name='access_key',
            field=models.CharField(max_length=1000, null=True),
        ),
    ]