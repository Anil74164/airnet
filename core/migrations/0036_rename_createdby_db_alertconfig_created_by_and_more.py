# Generated by Django 5.0.4 on 2024-05-31 06:36

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0035_rename_iscurrent_db_alertconfig_is_current_and_more'),
    ]

    operations = [
        migrations.RenameField(
            model_name='db_alertconfig',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_alertconfig',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_aqdatasparse',
            old_name='createddt',
            new_name='created_dt',
        ),
        migrations.RenameField(
            model_name='db_device',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_device',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_devicemodel',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_devicemodel',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_host',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_host',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_location',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_location',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_manufacturer',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_manufacturer',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_network',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_network',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_parameter',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_parameter',
            old_name='createddt',
            new_name='created_dt',
        ),
        migrations.RenameField(
            model_name='db_parameter',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_parameter',
            old_name='modifieddt',
            new_name='modified_dt',
        ),
        migrations.RenameField(
            model_name='db_permission',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_permission',
            old_name='createddt',
            new_name='created_dt',
        ),
        migrations.RenameField(
            model_name='db_permission',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_permission',
            old_name='modifieddt',
            new_name='modified_dt',
        ),
        migrations.RenameField(
            model_name='db_site',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_site',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_units',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_units',
            old_name='createddt',
            new_name='created_dt',
        ),
        migrations.RenameField(
            model_name='db_units',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_units',
            old_name='modifieddt',
            new_name='modified_dt',
        ),
        migrations.RenameField(
            model_name='db_user',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_user',
            old_name='createddt',
            new_name='created_dt',
        ),
        migrations.RenameField(
            model_name='db_user',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
        migrations.RenameField(
            model_name='db_user',
            old_name='modifieddt',
            new_name='modified_dt',
        ),
        migrations.RenameField(
            model_name='db_user',
            old_name='ROLE',
            new_name='role',
        ),
        migrations.RenameField(
            model_name='db_vendor',
            old_name='createdBy',
            new_name='created_by',
        ),
        migrations.RenameField(
            model_name='db_vendor',
            old_name='modifiedBy',
            new_name='modified_by',
        ),
    ]
