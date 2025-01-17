# Generated by Django 5.0.4 on 2024-05-31 06:32

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0034_db_airnet_aggregated'),
    ]

    operations = [
        migrations.RenameField(
            model_name='db_alertconfig',
            old_name='isCurrent',
            new_name='is_current',
        ),
        migrations.RenameField(
            model_name='db_datasource',
            old_name='isCurrent',
            new_name='is_current',
        ),
        migrations.RenameField(
            model_name='db_parameter',
            old_name='isCurrent',
            new_name='is_current',
        ),
        migrations.RenameField(
            model_name='db_permission',
            old_name='isCurrent',
            new_name='is_current',
        ),
        migrations.RenameField(
            model_name='db_role',
            old_name='isCurrent',
            new_name='is_current',
        ),
        migrations.RenameField(
            model_name='db_units',
            old_name='isCurrent',
            new_name='is_current',
        ),
        migrations.RenameField(
            model_name='db_user',
            old_name='isCurrent',
            new_name='is_current',
        ),
    ]
